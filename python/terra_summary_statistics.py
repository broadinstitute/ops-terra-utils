import logging
import os.path
from typing import Any, Tuple, Optional
from pandas import DataFrame
import numpy as np
import re
from argparse import ArgumentParser, Namespace
from datetime import datetime

from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP
from utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from utils.terra_utils.terra_util import TerraWorkspace
from utils.csv_util import Csv

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

INPUT_HEADERS = ["table_name", "column_name"]
REQUIRED_OUTPUT_HEADERS = [
    "table_name", "column_name", "label", "description", "multiple_values_allowed", "data_type", "primary_key",
    "refers_to_column", "required", "allowed_values_list", "allowed_values_pattern", "inferred_data_type"
    "inferred_multiple_values_allowed", "record_count", "null_value_count",
    "unique_value_count", "value_not_in_ref_col_count", "non_allowed_value_count", "flagged", "flag_notes"
]

ALL_FIELDS_NON_REQUIRED = True
FORCE_COLUMNS_TO_STRING = True
NA = "N/A"

INPUT_DT_TO_INFERRED_DTS = {
    "boolean": "boolean",
    "date": "date",
    "datetime": "datetime",
    "float": "float64",
    "int": "int64",
    "string": "string",
    "fileref": "fileref"
}


def get_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--workspace_name", "-w", type=str, required=True, help="Terra workspace name")
    parser.add_argument("--billing_project", "-b", type=str, required=True, help="Billing project name")
    parser.add_argument(
        "--data_dictionary_file",
        "-d", type=str,
        help=f"Input tsv to validate against what exists in the workspace. Headers should:  {INPUT_HEADERS}. "
             "Not required and if provided does not need every column filled out"
    )
    return parser.parse_args()


class ParseInputDataDict:
    def __init__(self, data_dictionary_file: str):
        self.data_dictionary_file = data_dictionary_file

    @staticmethod
    def _convert_to_bool(value: str) -> Any:
        if not value:
            return None
        if value.lower() == 'y':
            return True
        elif value.lower() == 'n':
            return False
        else:
            return value

    def run(self) -> dict:
        if not self.data_dictionary_file:
            return {}
        input_data = Csv(self.data_dictionary_file).create_list_of_dicts_from_tsv(
            expected_headers=INPUT_HEADERS,
            allow_extra_headers=True
        )
        # Create a dictionary with the key being a tuple of table_name and column_name
        return {
            (row['table_name'], row['column_name']): {k: self._convert_to_bool(v) for k, v in row.items()}
            for row in input_data
        }


class GetTablesInfo:
    def __init__(self, workspace: TerraWorkspace):
        self.workspace = workspace

    def _convert_cell(self, cell_value: Any) -> Tuple[Any, Optional[str]]:
        linked_column = None
        if isinstance(cell_value, dict):
            # If the cell value is a dictionary, check if it has an entityName key
            # Which means it is a linked entity
            entity_name = cell_value.get("entityName")
            if entity_name:
                linked_table = cell_value.get("entityType")
                linked_column = f"{linked_table}.{linked_table}_id"
                # Turn the cell value into the entity name
                cell_value = entity_name
            # If the cell value is a list of dictionaries, recursively call this function on each dictionary
            else:
                entity_list = cell_value.get("items")
                if entity_list or entity_list == []:
                    new_list = []
                    # For each item in the list, convert the cell value
                    for item in entity_list:
                        cell_value, linked_column = self._convert_cell(item)
                        new_list.append(cell_value)
                    cell_value = new_list
        return cell_value, linked_column

    @staticmethod
    def _update_dict_with_content_stats(table_contents: list[dict], column_info: dict) -> None:
        df = DataFrame(table_contents)
        for column_name, column_dict in column_info.items():
            processed_column = df[column_name].apply(
                lambda x: ", ".join(sorted(map(str, x))) if isinstance(x, (list, set, np.ndarray)) else x
            )
            # Count empty cells (including null and empty arrays)
            column_dict["empty_cells"] = int(processed_column.isnull().sum() + (processed_column == "").sum())
            # Count distinct values (using sorted strings for consistency)
            column_dict["distinct_values"] = int(processed_column.nunique())

    def _create_table_info_dict(self, table_name: str, table_info: dict) -> dict:
        table_dict = {
            'primary_key': table_info['idName'],
            'total_rows': table_info['count'],
            'table_contents': [],
            'column_info': {}
        }
        table_metrics = terra.get_gcp_workspace_metrics(entity_type=table_name)
        for row in table_metrics:
            id_column = f"{row['entityType']}_id"
            reformatted_row = {id_column: row['name']}
            table_dict['column_info'][id_column] = {
                'primary_key': True,
                'linked_column': None,
                'record_count': table_info['count']
            }
            for column, cell in row['attributes'].items():
                cell_value, linked_column = self._convert_cell(cell)
                reformatted_row[column] = cell_value
                table_dict['column_info'][column] = {
                    'primary_key': False,
                    'linked_column': linked_column,
                    'record_count': table_info['count']
                }
            table_dict['table_contents'].append(reformatted_row)
        self._update_dict_with_content_stats(table_dict['table_contents'], table_dict['column_info'])
        return table_dict

    def run(self) -> dict:
        tables_dict = {}
        tables_info = terra.get_workspace_entity_info()
        for table_name, table_info in tables_info.items():
            tables_dict[table_name] = self._create_table_info_dict(table_name, table_info)
        return tables_dict


class AddInferredInfo:
    def __init__(self, tables_info: dict):
        self.tables_info = tables_info

    def run(self) -> dict:
        for table_name, table_info in self.tables_info.items():
            inferred_schema = InferTDRSchema(
                table_name=table_name,
                input_metadata=tables_info[table_name]['table_contents'],
                all_fields_non_required=ALL_FIELDS_NON_REQUIRED,
                allow_disparate_data_types_in_column=FORCE_COLUMNS_TO_STRING
            ).infer_schema()
            # Add the inferred schema to the table_info dictionary
            for inferred_column in inferred_schema['columns']:
                column_dict = table_info['column_info'][inferred_column['name']]
                column_dict['inferred_data_type'] = inferred_column['datatype']
                column_dict['inferred_multiple_values_allowed'] = inferred_column['array_of']
        return self.tables_info


class CompareExpectedToActual:
    def __init__(self, expected_data: dict, actual_workspace_info: dict):
        self.expected_data = expected_data
        self.actual_workspace_info = actual_workspace_info

    def _create_row_dict(
            self,
            table: str,
            column: str,
            label: str,
            description: str,
            column_dict: dict,
            expected_info: dict,
            flagged: bool,
            flag_notes: str,
            value_not_in_ref_col_count: int,
            non_allowed_value_count: int
    ) -> dict:
        base_dict = {
            'table_name': table,
            'column_name': column,
            'label': label,
            'description': description,
            'data_type': expected_info.get('data_type'),
            'multiple_values_allowed': expected_info.get('multiple_values_allowed'),
            'primary_key': expected_info.get('primary_key'),
            'refers_to_column': expected_info.get('refers_to_column'),
            'required': expected_info.get('required'),
            'inferred_multiple_values_allowed': column_dict['inferred_multiple_values_allowed'],
            'inferred_data_type': column_dict['inferred_data_type'],
            'record_count': column_dict['record_count'],
            'null_value_count': column_dict['empty_cells'],
            'unique_value_count': column_dict['distinct_values'],
            'allowed_values_list': expected_info.get('allowed_values_list'),
            'allowed_values_pattern': expected_info.get('allowed_values_pattern'),
            'value_not_in_ref_col_count': value_not_in_ref_col_count,
            'non_allowed_value_count': non_allowed_value_count,
            'flagged': flagged,
            'flag_notes': flag_notes
        }
        # Add any extra key-value pairs from expected_info
        additional_info = {k: v for k, v in expected_info.items() if k not in base_dict}
        base_dict.update(additional_info)
        return base_dict

    @staticmethod
    def _compare_values(expected: Any, actual: Any, column: str) -> Tuple[bool, str]:
        flagged = False
        # If the expected value is not provided, do not flag it and return an empty note
        if not expected:
            return flagged, ""
        # If the column is data_type, check if the expected data type is in the expected types
        if column == 'data_type':
            if expected not in INPUT_DT_TO_INFERRED_DTS:
                note = f"Data type {expected} not in expected types: {list(INPUT_DT_TO_INFERRED_DTS.keys())}"
                flagged = True
                return flagged, note
            else:
                # Convert the expected data type to the inferred data type
                expected = INPUT_DT_TO_INFERRED_DTS[expected]
        # If the expected value is not the same as the actual value, flag it
        if expected != actual:
            flagged = True
            note = f'Column "{column}" not matching'
        else:
            note = ""
        return flagged, note

    @staticmethod
    def _validate_column_contents(expected_info: dict, actual_column_info: list[Any]) -> Tuple[bool, str, int]:
        flagged = False
        flag_notes = ""
        non_allowed_value_count = 0
        allowed_values = expected_info.get('allowed_values_list')
        if allowed_values:
            allowed_values_list = [
                allowed_value.strip()
                for allowed_value in allowed_values.split(',')
            ]
            # Check if any of the actual values are not in the allowed values list
            not_allowed_values = [value for value in actual_column_info if value not in allowed_values_list]
            if not_allowed_values:
                flagged = True
                flag_notes = "Column contains values not in allowed value list"
                # Count the number of values that are not in the allowed values list
                non_allowed_value_count = len(not_allowed_values)
        allowed_pattern = expected_info.get('allowed_values_pattern')
        if allowed_pattern:
            # Check if any of the actual values do not match the allowed pattern
            not_matching_values = [value for value in actual_column_info if not re.search(allowed_pattern, str(value))]
            if not_matching_values:
                flagged = True
                if flag_notes:
                    flag_notes += ", "
                flag_notes += "Column contains values not matching allowed value pattern"
                # Count the number of values that do not match the allowed pattern
                non_allowed_value_count += len(not_matching_values)
        return flagged, flag_notes, non_allowed_value_count

    @staticmethod
    def _check_required(required: Any, null_fields: int) -> Tuple[bool, str]:
        flagged = False
        flag_notes = ""
        if required and null_fields > 0:
            flagged = True
            flag_notes = "Column is required but has empty cells"
        return flagged, flag_notes

    def _check_referenced_column(
            self, refers_to_column: Optional[str], column_contents: list[Any]
    ) -> Tuple[bool, str, int]:
        flagged = False
        flag_notes = ""
        value_not_in_ref_col_count = 0
        if refers_to_column:
            table, column = refers_to_column.split('.')
            if table not in self.actual_workspace_info:
                flagged = True
                flag_notes = "Referenced table not found in workspace"
                value_not_in_ref_col_count = len(column_contents)
            elif column not in self.actual_workspace_info[table]['column_info']:
                flagged = True
                flag_notes = "Referenced column not found in workspace"
                value_not_in_ref_col_count = len(column_contents)
            else:
                referenced_column_contents = [
                    row[column] for row in
                    self.actual_workspace_info[table]['table_contents']
                ]
                # Check if any of the actual values are not in the referenced column
                bad_references = [value for value in column_contents if value not in referenced_column_contents]
                if bad_references:
                    flagged = True
                    flag_notes = "Column contains values not in referenced column"
                    value_not_in_ref_col_count = len(bad_references)

        return flagged, flag_notes, value_not_in_ref_col_count

    def _validate_column(
            self,
            column_name: str,
            expected_info: dict,
            actual_column_info: dict,
            table_contents: list[Any]
    ) -> Tuple[bool, str, int, int]:
        required_flagged, required_notes = self._check_required(
            required=expected_info.get('required'),
            null_fields=actual_column_info['empty_cells'],
        )

        multiple_values_allowed_flagged, multiple_values_allowed_notes = self._compare_values(
            expected=expected_info.get('multiple_values_allowed'),
            actual=actual_column_info['inferred_multiple_values_allowed'],
            column='multiple_values_allowed'
        )

        primary_key_flagged, primary_key_notes = self._compare_values(
            expected=expected_info.get('primary_key'),
            actual=actual_column_info['primary_key'],
            column='primary_key'
        )

        data_type_flagged, data_type_notes = self._compare_values(
            # Convert the expected data type to the inferred data type
            expected=expected_info.get('data_type'),
            actual=actual_column_info['inferred_data_type'],
            column='data_type'
        )

        if expected_info.get('allowed_values_list') or expected_info.get('allowed_values_pattern') or \
                expected_info.get('refers_to_column'):
            # Only get column contents if need to check allowed values or referenced column
            column_contents = [row[column_name] for row in table_contents if column_name in row]

            contents_flagged, content_notes, non_allowed_value_count = self._validate_column_contents(
                expected_info=expected_info,
                actual_column_info=column_contents
            )

            refers_to_flagged, refers_to_notes, value_not_in_ref_col_count = self._check_referenced_column(
                refers_to_column=expected_info.get('refers_to_column'),
                column_contents=column_contents
            )
        else:
            contents_flagged = False
            content_notes = ""
            refers_to_flagged = False
            refers_to_notes = ""
            value_not_in_ref_col_count = 0
            non_allowed_value_count = 0

        flagged = any(
            [
                data_type_flagged, required_flagged, multiple_values_allowed_flagged,
                primary_key_flagged, contents_flagged, refers_to_flagged
            ]
        )
        flag_notes = [
            note for note in [
                required_notes, multiple_values_allowed_notes, primary_key_notes,
                data_type_notes, content_notes, refers_to_notes
            ] if note
        ]
        return flagged, ", ".join(flag_notes), value_not_in_ref_col_count, non_allowed_value_count

    def run(self) -> list[dict]:
        output_content = []
        for table, table_info in self.actual_workspace_info.items():
            for column, column_info in table_info['column_info'].items():
                expected_info = self.expected_data.get((table, column), {})
                label = expected_info.get('label', NA)
                description = expected_info.get('description', NA)
                # If the column is not in the expected data, flag it and do not look into the column
                if not expected_info:
                    flagged = True
                    value_not_in_ref_col_count = 0
                    non_allowed_value_count = 0
                    flag_notes = "Column not found in input file"
                else:
                    flagged, flag_notes, value_not_in_ref_col_count, non_allowed_value_count = self._validate_column(
                        column_name=column,
                        expected_info=expected_info,
                        actual_column_info=column_info,
                        table_contents=table_info['table_contents']
                    )
                output_content.append(
                    self._create_row_dict(
                        table=table,
                        column=column,
                        label=label,
                        description=description,
                        column_dict=column_info,
                        flagged=flagged,
                        expected_info=expected_info,
                        flag_notes=flag_notes,
                        value_not_in_ref_col_count=value_not_in_ref_col_count,
                        non_allowed_value_count=non_allowed_value_count
                    )
                )
        return output_content


class CreateOutputTsv:
    def __init__(self, output_file: str, output_content: list[dict], input_headers: list[str] = []):
        self.output_file = output_file
        self.output_content = output_content
        # If input file is given then get the headers that originally existed to go in front and in that order
        self.input_headers = input_headers

    def _create_ordered_header_list(self) -> list[str]:
        all_keys: set[str] = set()
        for record in self.output_content:
            all_keys.update(record.keys())
        # Add any keys that are not already in the output_headers to the end of the list
        updated_headers = self.input_headers + [key for key in REQUIRED_OUTPUT_HEADERS if key not in self.input_headers]
        return updated_headers

    def run(self) -> None:
        Csv(
            file_path=self.output_file
        ).create_tsv_from_list_of_dicts(
            list_of_dicts=self.output_content,
            header_list=self._create_ordered_header_list()
        )


if __name__ == '__main__':
    args = get_args()
    workspace_name = args.workspace_name
    billing_project = args.billing_project
    data_dictionary_file = args.data_dictionary_file

    date_string = datetime.now().strftime("%Y%m%d")
    if data_dictionary_file:
        data_dict_file_name = os.path.basename(data_dictionary_file).replace(".tsv", "")
        output_file = f"{data_dict_file_name}.summary_stats.{date_string}.tsv"
        # Get the headers from the input file to keep the order consistent
        output_headers = Csv(file_path=data_dictionary_file).get_header_order_from_tsv()
    else:
        output_file = f"{billing_project}.{workspace_name}.summary_stats.{date_string}.tsv"
        # If no input file then just use the required headers
        output_headers = []

    # Parse the input data dictionary file
    input_data = ParseInputDataDict(data_dictionary_file).run()

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    terra = TerraWorkspace(request_util=request_util, workspace_name=workspace_name, billing_project=billing_project)

    # Get the tables information and create a dictionary with the table name as the key
    tables_info = GetTablesInfo(workspace=terra).run()

    # Add inferred schema information to the table_info dictionary
    full_tables_info = AddInferredInfo(tables_info=tables_info).run()

    output_content = CompareExpectedToActual(
        expected_data=input_data,
        actual_workspace_info=full_tables_info
    ).run()

    CreateOutputTsv(output_file=output_file, output_content=output_content).run()
