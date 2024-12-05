import logging
from typing import Any, Tuple, Optional
from pandas import DataFrame
import numpy as np
from argparse import ArgumentParser, Namespace

from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP
from utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from utils.terra_utils.terra_util import TerraWorkspace
from utils.csv_util import Csv

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

INPUT_HEADERS = [
    "table_name", "column_name", "label", "array", "description", "primary_key", "refers_to_column", "required"
]
OUTPUT_HEADERS = [
    "table_name", "column_name", "label", "description", "array", "data_type", "primary_key", "refers_to_column",
    "required", "record_count", "null_value_count", "unique_value_count", "flagged", "notes"
]

ALL_FIELDS_NON_REQUIRED = False
FORCE_COLUMNS_TO_STRING = True
NA = "N/A"
OUTPUT = "/Users/samn/Desktop/test_output.tsv"


def get_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--workspace_name", "-w", type=str, required=True, help="Terra workspace name")
    parser.add_argument("--billing_project", "-b", type=str, required=True, help="Billing project name")
    parser.add_argument("--output_tsv", "-o", type=str, help="Output tsv file", default=OUTPUT)
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

    def run(self) -> dict:
        if not self.data_dictionary_file:
            return {}
        input_data = Csv(self.data_dictionary_file).create_list_of_dicts_from_tsv(expected_headers=INPUT_HEADERS)
        # Create a dictionary with the key being a tuple of table_name and column_name
        return {
            (row['table_name'], row['column_name']): row
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
            # Remove the table_contents key from the table_info dictionary
            del table_info['table_contents']
            # Add the inferred schema to the table_info dictionary
            for inferred_column in inferred_schema['columns']:
                column_dict = table_info['column_info'][inferred_column['name']]
                column_dict['inferred_data_type'] = inferred_column['datatype']
                column_dict['inferred_array'] = inferred_column['array_of']
                column_dict['inferred_required'] = inferred_column['required']
        return self.tables_info


class CompareExpectedToActual:
    def __init__(self, expected_data: dict, actual_workspace_info: dict):
        self.expected_data = expected_data
        self.actual_workspace_info = actual_workspace_info

    @staticmethod
    def _create_row_dict(table: str, column: str, label: str, description: str, column_dict: dict, flagged: bool, notes: str) -> dict:
        return {
            'table_name': table,
            'column_name': column,
            'label': label,
            'description': description,
            'array': column_dict['inferred_array'],
            'data_type': column_dict['inferred_data_type'],
            'primary_key': column_dict['primary_key'],
            'refers_to_column': column_dict.get('linked_column', NA),
            'required': column_dict['inferred_required'],
            'record_count': column_dict['record_count'],
            'null_value_count': column_dict['empty_cells'],
            'unique_value_count': column_dict['distinct_values'],
            'flagged': flagged,
            'notes': notes
        }

    @staticmethod
    def _convert_for_comparison(value: str) -> Any:
        if not value:
            return None
        if value.lower() == 'y':
            return True
        elif value.lower() == 'n':
            return False
        else:
            return value

    def _compare_values(self, expected: Any, actual: Any, column: str) -> Tuple[bool, str, Any]:
        converted_expected = self._convert_for_comparison(expected)
        flagged = False
        if converted_expected != actual:
            flagged = True
            note = f'Column "{column}": Expected: {converted_expected}, Actual: {actual}'
        else:
            note = ""
        return flagged, note, converted_expected

    def _validate_column(self, expected_info: dict, actual_column_info: dict) -> Tuple[bool, str]:
        required_flagged, required_notes, required_converted = self._compare_values(
            expected=expected_info['required'],
            actual=actual_column_info['inferred_required'],
            column='required'
        )

        array_flagged, array_notes, array_converted = self._compare_values(
            expected=expected_info['array'],
            actual=actual_column_info['inferred_array'],
            column='array'
        )

        primary_key_flagged, primary_key_notes, primary_key_converted = self._compare_values(
            expected=expected_info['primary_key'],
            actual=actual_column_info['primary_key'],
            column='primary_key'
        )

        refers_to_column_flagged, refers_to_column_notes, refers_to_column_converted = self._compare_values(
            expected=expected_info['refers_to_column'],
            actual=actual_column_info['linked_column'],
            column='refers_to_column'
        )
        flagged = any([required_flagged, array_flagged, primary_key_flagged, refers_to_column_flagged])
        notes = [note for note in [required_notes, array_notes, primary_key_notes, refers_to_column_notes] if note]
        return flagged, ", ".join(notes)

    def run(self) -> list[dict]:
        output_content = []
        for table, table_info in self.actual_workspace_info.items():
            for column, column_info in table_info['column_info'].items():
                expected_info = self.expected_data.get((table, column))
                if not expected_info:
                    flagged = True
                    notes = "Column not found in expected data"
                    label = NA
                    description = NA
                else:
                    label = expected_info['label']
                    description = expected_info['description']
                    flagged, notes = self._validate_column(
                        expected_info=expected_info,
                        actual_column_info=column_info
                    )
                output_content.append(
                    self._create_row_dict(
                        table=table,
                        column=column,
                        label=label,
                        description=description,
                        column_dict=column_info,
                        flagged=flagged,
                        notes=notes
                    )
                )
        return output_content


if __name__ == '__main__':
    args = get_args()
    workspace_name = args.workspace_name
    billing_project = args.billing_project
    data_dictionary_file = args.data_dictionary_file
    output_file = args.output_tsv

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

    Csv(file_path=output_file).create_tsv_from_list_of_dicts(output_content, header_list=OUTPUT_HEADERS)
