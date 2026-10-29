import logging
import os.path
from collections import Counter, defaultdict
from typing import Any, Tuple, Optional
from pandas import DataFrame
import numpy as np
import re
from argparse import ArgumentParser, Namespace
from datetime import datetime

from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from ops_utils.terra_util import TerraWorkspace
from ops_utils.csv_util import Csv
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.thread_pool_executor_util import MultiThreadedJobs
from ops_utils.vars import ARG_DEFAULTS

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

INPUT_HEADERS = ["table_name", "column_name"]
REQUIRED_OUTPUT_HEADERS = [
    "table_name", "column_name", "label", "description", "multiple_values_allowed", "data_type", "primary_key",
    "refers_to_column", "required", "allowed_values_list", "allowed_values_pattern", "inferred_data_type",
    "inferred_multiple_values_allowed", "record_count", "null_value_count",
    "unique_value_count", "value_not_in_ref_col_count", "non_allowed_value_count", "flagged", "flag_notes"
]

ALL_FIELDS_NON_REQUIRED = True
FORCE_COLUMNS_TO_STRING = True
NA = "N/A"

GCS_PREFIX = "gs://"
UNKNOWN_EXTENSION = "(none)"
# Extensions that only mean something when combined with the one before them (e.g. .vcf.gz)
COMPRESSION_EXTENSIONS = [".gz", ".bgz"]
# (label, file extensions that should have an index, index suffixes that count as an index file).
# An index can either be the full file name plus the suffix (sample.bam.bai) or the file name with its extension
# swapped (sample.bai). Uncompressed vcfs are left out because they cannot be indexed with tabix
INDEX_RULES: list[Tuple[str, list[str], list[str]]] = [
    ("BAM", [".bam"], [".bai", ".csi"]),
    ("CRAM", [".cram"], [".crai"]),
    ("VCF/GVCF", [".vcf.gz", ".g.vcf.gz", ".gvcf.gz", ".vcf.bgz", ".g.vcf.bgz"], [".tbi", ".csi"]),
]

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
    parser.add_argument(
        "--output_dir",
        "-o",
        type=str,
        help="GCP directory (gs://) the output files will be copied to. Used to point to the output files from the "
             "validation report. If not provided, only the file names are used."
    )
    args = parser.parse_args()
    if args.output_dir and not args.output_dir.startswith(GCS_PREFIX):
        parser.error(f"--output_dir must be a GCP path starting with {GCS_PREFIX}, got: {args.output_dir}")
    return args


class ParseInputDataDict:
    def __init__(self, data_dict_contents: list[dict]):
        self.data_dict_contents = data_dict_contents

    @staticmethod
    def _convert_to_bool(value: str) -> Any:
        if not value:
            return None
        if value.lower() in ['true', 'yes', 'y']:
            return True
        elif value.lower() in ['false', 'no', 'n']:
            return False
        else:
            return value

    def _convert_values(self, key: str, value: Any) -> Any:
        # Always convert data type to lowercase
        if key == 'data_type':
            return value.lower() if isinstance(value, str) else value
        # If not string then check if boolean and convert
        else:
            return self._convert_to_bool(value)

    def run(self) -> dict:
        if not self.data_dict_contents:
            return {}
        # Create a dictionary with the key being a tuple of table_name and column_name
        return {
            (row['table_name'], row['column_name']): {k: self._convert_values(k, v) for k, v in row.items()}
            for row in self.data_dict_contents
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
        table_metrics = self.workspace.get_gcp_workspace_metrics(entity_type=table_name)
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
        tables_info = self.workspace.get_workspace_entity_info().json()
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
                input_metadata=table_info['table_contents'],
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
                # Get the contents of the referenced column and convert them to strings
                # for comparison with the actual column contents
                referenced_column_contents = [
                    str(row[column]) for row in
                    self.actual_workspace_info[table]['table_contents']
                ]
                # Check if any of the actual values are not in the referenced column
                # Convert the values to strings for comparison
                bad_references = [value for value in column_contents if str(value) not in referenced_column_contents]
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
    def __init__(
            self,
            output_file: str,
            output_content: list[dict],
            input_headers: list[str] = [],
            row_order: list[tuple[str, str]] = []
    ):
        self.output_file = output_file
        self.output_content = output_content
        # If input file is given then get the headers that originally existed to go in front and in that order
        self.input_headers = input_headers
        self.row_order = row_order

    def _create_ordered_header_list(self) -> list[str]:
        all_keys: set[str] = set()
        for record in self.output_content:
            all_keys.update(record.keys())
        # Add any keys that are not already in the output_headers to the end of the list
        updated_headers = self.input_headers + [key for key in REQUIRED_OUTPUT_HEADERS if key not in self.input_headers]
        return updated_headers

    def _sort_rows(self, item: dict) -> int:
        """Sort the output content based on the order of the input file"""
        item_tuple = (item['table_name'], item['column_name'])
        # If the tuple is in the order list, return the index of the tuple; otherwise, place it at the end
        return self.row_order.index(item_tuple) if item_tuple in self.row_order else len(self.row_order)

    def run(self) -> None:
        Csv(
            file_path=self.output_file
        ).create_tsv_from_list_of_dicts(
            list_of_dicts=sorted(self.output_content, key=self._sort_rows),
            header_list=self._create_ordered_header_list()
        )


class ExtractFileReferences:
    """Find every gs:// path referenced in the cells of the workspace tables."""

    def __init__(self, tables_info: dict):
        self.tables_info = tables_info

    def _paths_in_cell(self, cell: Any) -> list[str]:
        if isinstance(cell, str):
            return [cell.strip()] if cell.strip().startswith(GCS_PREFIX) else []
        if isinstance(cell, (list, set, tuple, np.ndarray)):
            return [path for item in cell for path in self._paths_in_cell(item)]
        return []

    def run(self) -> list[dict]:
        """Returns a list of dicts with table_name, column_name and path (one per reference)"""
        references = []
        for table_name, table_info in self.tables_info.items():
            for row in table_info['table_contents']:
                for column_name, cell in row.items():
                    for path in self._paths_in_cell(cell):
                        references.append({'table_name': table_name, 'column_name': column_name, 'path': path})
        return references


class ValidateBucketFiles:
    """Compare what is in the workspace bucket to what the tables reference."""

    def __init__(
            self,
            workspace_bucket: str,
            gcp: GCPCloudFunctions,
            file_references: list[dict]
    ):
        self.workspace_bucket = workspace_bucket
        self.gcp = gcp
        self.file_references = file_references

    @staticmethod
    def get_extension(path: str) -> str:
        base = os.path.basename(path).lower()
        stem, ext = os.path.splitext(base)
        if ext in COMPRESSION_EXTENSIONS and os.path.splitext(stem)[1]:
            return os.path.splitext(stem)[1] + ext
        return ext or UNKNOWN_EXTENSION

    @staticmethod
    def get_bucket(path: str) -> str:
        return path[len(GCS_PREFIX):].split("/", 1)[0]

    @staticmethod
    def _locations(references: list[dict]) -> dict[str, set[str]]:
        """Map each referenced path to the table.column(s) it is referenced from"""
        locations: dict[str, set[str]] = defaultdict(set)
        for ref in references:
            locations[ref['path']].add(f"{ref['table_name']}.{ref['column_name']}")
        return locations

    @staticmethod
    def _check_indexes(inventory_paths: set[str]) -> dict:
        results = {}
        for label, extensions, index_suffixes in INDEX_RULES:
            data_files = {}
            for path in inventory_paths:
                # Use the longest matching extension so sample.g.vcf.gz is stripped back to sample
                matching_extensions = [ext for ext in extensions if path.lower().endswith(ext)]
                if matching_extensions:
                    data_files[path] = max(matching_extensions, key=len)
            missing = []
            for path, extension in sorted(data_files.items()):
                candidates = [path + suffix for suffix in index_suffixes]
                candidates += [path[:-len(extension)] + suffix for suffix in index_suffixes]
                if not any(candidate in inventory_paths for candidate in candidates):
                    missing.append(path)
            results[label] = {
                'extensions': extensions,
                'index_extensions': index_suffixes,
                'total': len(data_files),
                'missing': missing
            }
        return results

    def _get_external_file_info(self, path: str) -> dict:
        """Check whether a file exists and has an MD5 in its metadata, without downloading it"""
        blob = self.gcp.load_blob_from_full_path(path)
        # The blob is only reloaded with its metadata if it exists, so a missing size means the file was not found
        exists = blob.size is not None
        return {'path': path, 'exists': exists, 'has_md5': exists and bool(blob.md5_hash)}

    def _check_external_bucket(self, bucket: str, paths: list[str]) -> dict:
        try:
            # Probe one file first so a bucket we cannot access is skipped quickly instead of failing on every file.
            # A file that does not exist returns False here, while no access raises an error
            self.gcp.check_file_exists(paths[0])
            file_infos = MultiThreadedJobs().run_multi_threaded_job(
                workers=ARG_DEFAULTS["multithread_workers"],  # type: ignore[arg-type]
                function=self._get_external_file_info,
                list_of_jobs_args_list=[[path] for path in paths],
                collect_output=True,
                max_retries=ARG_DEFAULTS["max_retries"],  # type: ignore[arg-type]
                fail_on_error=True,
                jobs_complete_for_logging=500
            ) or []
        except Exception as e:
            logging.warning(f"Unable to check files in gs://{bucket}: {e}")
            return {
                'status': "unable to access (check permissions, or the files may not exist): "
                          f"{str(e)[:200]}",
                'missing': [],
                'missing_md5': []
            }
        return {
            'status': 'checked',
            'missing': sorted(info['path'] for info in file_infos if not info['exists']),
            'missing_md5': sorted(info['path'] for info in file_infos if info['exists'] and not info['has_md5'])
        }

    def run(self) -> dict:
        inventory = self.gcp.list_bucket_contents(bucket_name=self.workspace_bucket)
        inventory_paths = {item['path'] for item in inventory}
        locations = self._locations(self.file_references)
        referenced_paths = set(locations)

        # Files with no md5 in the object metadata
        missing_md5 = sorted(item['path'] for item in inventory if not item.get('md5_hash'))
        zero_byte = sorted(item['path'] for item in inventory if not item.get('size_in_bytes'))

        # Distribution of files by extension
        distribution: dict[str, dict] = defaultdict(lambda: {'count': 0, 'size_in_bytes': 0})
        for item in inventory:
            entry = distribution[self.get_extension(item['path'])]
            entry['count'] += 1
            entry['size_in_bytes'] += item.get('size_in_bytes') or 0

        # Split references into the ones pointing at the workspace bucket and the ones pointing elsewhere
        workspace_refs = {path for path in referenced_paths if self.get_bucket(path) == self.workspace_bucket}
        external_refs: dict[str, list[str]] = defaultdict(list)
        for path in sorted(referenced_paths - workspace_refs):
            external_refs[self.get_bucket(path)].append(path)

        external_results = {}
        for bucket, paths in sorted(external_refs.items()):
            external_results[bucket] = {'referenced_count': len(paths), **self._check_external_bucket(bucket, paths)}
            for path in external_results[bucket]['missing']:
                logging.debug(f"Missing: {path}")

        return {
            'inventory': inventory,
            'inventory_paths': inventory_paths,
            'locations': locations,
            'missing_md5': missing_md5,
            'zero_byte': zero_byte,
            'indexes': self._check_indexes(inventory_paths),
            'distribution': dict(distribution),
            'not_referenced': sorted(inventory_paths - referenced_paths),
            'workspace_ref_count': len(workspace_refs),
            'workspace_refs_missing': sorted(workspace_refs - inventory_paths),
            'external': external_results,
            'total_references': len(self.file_references),
            'duplicate_references': sum(
                1 for count in Counter(r['path'] for r in self.file_references).values() if count > 1
            )
        }


class CreateValidationReport:
    def __init__(
            self,
            workspace_name: str,
            billing_project: str,
            workspace_bucket: str,
            output_prefix: str,
            timestamp_string: str,
            run_time_display: str,
            output_dir: str,
            summary_stats_file: str,
            summary_stats_content: list[dict],
            results: dict,
            data_dictionary_file: Optional[str] = None
    ):
        self.workspace_name = workspace_name
        self.billing_project = billing_project
        self.workspace_bucket = workspace_bucket
        self.output_prefix = output_prefix
        self.timestamp_string = timestamp_string
        self.run_time_display = run_time_display
        self.output_dir = output_dir
        self.summary_stats_file = summary_stats_file
        self.summary_stats_content = summary_stats_content
        self.results = results
        self.data_dictionary_file = data_dictionary_file

    @staticmethod
    def _pct(count: int, total: int) -> str:
        return f"{count / total * 100:.2f}%" if total else NA

    @staticmethod
    def _table(headers: list[str], rows: list[list[Any]]) -> str:
        lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
        lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
        return "\n".join(lines)

    def _output_path(self, file_name: str) -> str:
        return f"{self.output_dir}{file_name}"

    def _write_detail(self, name: str, rows: list[dict], headers: list[str]) -> Optional[str]:
        """Write a detail tsv (skipped if there is nothing to list) and return its path for the report"""
        if not rows:
            return None
        file_name = f"{self.output_prefix}.validation_detail.{name}.{self.timestamp_string}.tsv"
        Csv(file_path=file_name).create_tsv_from_list_of_dicts(list_of_dicts=rows, header_list=headers)
        return self._output_path(file_name)

    def _detail_line(self, path: Optional[str]) -> str:
        return f"\n\nFull list: `{path}`" if path else ""

    def _summary_stats_section(self) -> list[str]:
        content = self.summary_stats_content
        has_dictionary = bool(self.data_dictionary_file)
        # Rows, columns and file reference columns per table (in the order the tables were found)
        tables: dict[str, dict] = {}
        for row in content:
            table = tables.setdefault(
                row['table_name'], {'rows': row['record_count'], 'columns': 0, 'fileref_columns': 0, 'flagged': 0}
            )
            table['columns'] += 1
            table['fileref_columns'] += row['inferred_data_type'] == 'fileref'
            table['flagged'] += bool(row['flagged'])
        total_columns = len(content)
        flagged = [row for row in content if row['flagged']]
        if has_dictionary:
            dictionary_line = (
                "Data dictionary provided: **Yes** "
                f"(`{self._output_path(os.path.basename(str(self.data_dictionary_file)))}`)"
            )
            flagged_text = f"**{len(flagged)} of {total_columns}** ({self._pct(len(flagged), total_columns)})"
        else:
            dictionary_line = (
                "Data dictionary provided: **No**. Columns were not validated against expected values, "
                "so no columns are flagged."
            )
            flagged_text = NA
        section = [
            "## Table summary statistics",
            f"\nSummary statistics file: `{self._output_path(self.summary_stats_file)}`\n",
            dictionary_line + "\n",
            f"- Tables: **{len(tables)}**",
            f"- Total rows: **{sum(t['rows'] for t in tables.values())}**",
            f"- Total columns: **{total_columns}**",
            f"- File reference columns: **{sum(t['fileref_columns'] for t in tables.values())}**",
            f"- Columns flagged as needing attention: {flagged_text}\n",
            self._table(
                ["Table", "Rows", "Columns", "File reference columns", "Flagged columns"],
                [
                    [name, t['rows'], t['columns'], t['fileref_columns'], t['flagged'] if has_dictionary else NA]
                    for name, t in tables.items()
                ]
            )
        ]
        if has_dictionary and flagged:
            by_reason: Counter = Counter()
            for row in flagged:
                # Drop the allowed list of data types from the note so it is not split apart
                notes = re.sub(r"\[.*?\]", "[...]", row['flag_notes'] or "")
                by_reason.update(note.strip() for note in notes.split(", ") if note.strip())
            section.append("")
            section.append(self._table(["Flag reason", "Columns"], [[k, v] for k, v in by_reason.most_common()]))
        return section

    def _md5_section(self) -> list[str]:
        total = len(self.results['inventory'])
        missing = self.results['missing_md5']
        path = self._write_detail("files_missing_md5", [{'path': p} for p in missing], ['path'])
        return [
            "## Files missing MD5 in GCS metadata",
            f"\n**{len(missing)} of {total}** files ({self._pct(len(missing), total)}) "
            "have no MD5 in the object metadata."
            + self._detail_line(path)
        ]

    def _index_section(self) -> list[str]:
        rows = []
        missing_rows: list[dict] = []
        for label, info in self.results['indexes'].items():
            missing = info['missing']
            rows.append([
                label,
                ", ".join(f"`{e}`" for e in info['extensions']),
                ", ".join(f"`{e}`" for e in info['index_extensions']),
                info['total'], len(missing), self._pct(len(missing), info['total'])
            ])
            missing_rows += [{'file_type': label, 'path': p} for p in missing]
        path = self._write_detail("files_missing_index", missing_rows, ['file_type', 'path'])
        return [
            "## Files missing an index file",
            "\nAn index is accepted as `<file><index ext>` or `<file without ext><index ext>` in the workspace bucket.\n",
            self._table(["File type", "File extensions", "Index extensions", "Files", "Missing index", "% missing"], rows)
            + self._detail_line(path)
        ]

    def _unreferenced_section(self) -> list[str]:
        total = len(self.results['inventory_paths'])
        orphans = self.results['not_referenced']
        sizes = {item['path']: item.get('size_in_bytes') or 0 for item in self.results['inventory']}
        submissions_prefix = f"{GCS_PREFIX}{self.workspace_bucket}/submissions/"
        in_submissions = {p for p in orphans if p.startswith(submissions_prefix)}
        path = self._write_detail(
            "files_not_referenced",
            [{'path': p, 'size_in_bytes': sizes[p], 'in_submissions': p in in_submissions} for p in orphans],
            ['path', 'size_in_bytes', 'in_submissions']
        )
        outside_submissions = len(orphans) - len(in_submissions)
        return [
            "## Bucket files not referenced in the tables",
            f"\n**{len(orphans)} of {total}** files in the workspace bucket ({self._pct(len(orphans), total)}) "
            "are not referenced anywhere in the tabular data.\n",
            f"Of the unreferenced files, **{len(in_submissions)} of {len(orphans)}** "
            f"({self._pct(len(in_submissions), len(orphans))}) are in the `submissions/` directory, where Terra "
            f"writes workflow outputs. The other **{outside_submissions}** "
            f"({self._pct(outside_submissions, len(orphans))}) are elsewhere in the bucket."
            + self._detail_line(path)
        ]

    def _distribution_section(self) -> list[str]:
        distribution = self.results['distribution']
        total_size = sum(v['size_in_bytes'] for v in distribution.values())
        rows = [
            [ext, v['count'], v['size_in_bytes'], f"{v['size_in_bytes'] / 1024 ** 3:.3f}"]
            for ext, v in sorted(distribution.items(), key=lambda kv: kv[1]['size_in_bytes'], reverse=True)
        ]
        rows.append(["**Total**", len(self.results['inventory']), total_size, f"{total_size / 1024 ** 3:.3f}"])
        return [
            "## Bucket file distribution",
            "\n" + self._table(["Extension", "Files", "Size (bytes)", "Size (GiB)"], rows)
        ]

    def _referenced_missing_section(self) -> list[str]:
        results = self.results
        locations = results['locations']
        missing_rows = []
        for path in results['workspace_refs_missing']:
            missing_rows.append(
                {'path': path, 'bucket_type': 'workspace', 'referenced_in': ", ".join(sorted(locations[path]))}
            )
        external_rows = []
        missing_md5_rows = []
        for bucket, info in results['external'].items():
            checked = info['status'] == 'checked'
            external_rows.append([
                f"gs://{bucket}", info['referenced_count'], info['status'],
                len(info['missing']) if checked else NA,
                len(info['missing_md5']) if checked else NA
            ])
            missing_rows += [
                {'path': p, 'bucket_type': 'external', 'referenced_in': ", ".join(sorted(locations[p]))}
                for p in info['missing']
            ]
            missing_md5_rows += [
                {'path': p, 'bucket': bucket, 'referenced_in': ", ".join(sorted(locations[p]))}
                for p in info['missing_md5']
            ]
        md5_path = self._write_detail(
            "external_files_missing_md5", missing_md5_rows, ['path', 'bucket', 'referenced_in']
        )
        path = self._write_detail("referenced_files_missing", missing_rows, ['path', 'bucket_type', 'referenced_in'])
        ws_total = results['workspace_ref_count']
        ws_missing = len(results['workspace_refs_missing'])
        section = [
            "## Files referenced in the tables but not found",
            f"\nUnique file paths referenced: **{len(locations)}** ({results['total_references']} references, "
            f"{results['duplicate_references']} paths referenced more than once).\n",
            f"### Pointing at the workspace bucket (`gs://{self.workspace_bucket}`)\n",
            f"**{ws_missing} of {ws_total}** referenced files ({self._pct(ws_missing, ws_total)}) were not found."
            + (" These files are referenced in the tables but do not exist in the bucket." if ws_missing else "")
            + "\n",
            "### Pointing at other buckets\n",
        ]
        if external_rows:
            section.append(
                self._table(
                    ["Bucket", "Unique files referenced", "Existence check", "Missing", "Missing MD5"], external_rows
                )
            )
            section.append(
                "\nThe TDR service account needs read access to each of these buckets for ingest. Missing MD5 is "
                "the number of files that exist but have no MD5 in the GCS metadata, which also affects ingest."
            )
        else:
            section.append("No references to other buckets.")
        if path:
            section.append(f"\nMissing files: `{path}`")
        if md5_path:
            section.append(f"\nExternal files missing an MD5: `{md5_path}`")
        return section

    def _other_section(self) -> list[str]:
        zero_byte = self.results['zero_byte']
        path = self._write_detail("zero_byte_files", [{'path': p} for p in zero_byte], ['path'])
        return [
            "## Other checks",
            f"\nZero-byte files in the workspace bucket: **{len(zero_byte)}**" + self._detail_line(path)
        ]

    def run(self) -> str:
        results = self.results
        sections = [
            [
                f"# Staging workspace validation: {self.billing_project}/{self.workspace_name}",
                f"\nWorkspace bucket: `gs://{self.workspace_bucket}`  \nReport generated: {self.run_time_display}  \n"
                f"Files in bucket: {len(results['inventory'])}"
            ],
            self._summary_stats_section(),
            self._md5_section(),
            self._index_section(),
            self._unreferenced_section(),
            self._distribution_section(),
            self._referenced_missing_section(),
            self._other_section(),
        ]
        report_file = f"{self.output_prefix}.validation_report.{self.timestamp_string}.md"
        with open(report_file, "w") as f:
            f.write("\n\n".join("\n".join(section) for section in sections) + "\n")
        logging.info(f"Created {report_file}")
        return report_file


if __name__ == '__main__':
    args = get_args()
    workspace_name = args.workspace_name
    billing_project = args.billing_project
    data_dictionary_file = args.data_dictionary_file
    output_dir = args.output_dir
    if output_dir and not output_dir.endswith("/"):
        output_dir += "/"

    # One timestamp shared by all output files so reruns do not overwrite each other
    run_time = datetime.now()
    timestamp_string = run_time.strftime("%Y%m%d_%H%M%S")
    if data_dictionary_file:
        data_dict_file_name = os.path.basename(data_dictionary_file).replace(".tsv", "")
        output_prefix = data_dict_file_name
        # Get the headers from the input file to keep the order consistent
        input_headers = Csv(file_path=data_dictionary_file).get_header_order_from_tsv()
        # Get the contents of the input file to use as the expected data
        data_dict_contents = Csv(file_path=data_dictionary_file).create_list_of_dicts_from_tsv(
            expected_headers=INPUT_HEADERS,
            allow_extra_headers=True
        )
        # Create a list of tuples from input to keep the order consistent later
        row_order = [(row['table_name'], row['column_name']) for row in data_dict_contents]
    else:
        output_prefix = f"{billing_project}.{workspace_name}"
        # If no input file is given, set the headers, row order, and contents to empty lists
        input_headers = []
        row_order = []
        data_dict_contents = []

    output_file = f"{output_prefix}.summary_stats.{timestamp_string}.tsv"

    # Parse the input data dictionary contents
    input_data = ParseInputDataDict(data_dict_contents=data_dict_contents).run()

    token = Token()
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

    CreateOutputTsv(
        input_headers=input_headers,
        output_file=output_file,
        output_content=output_content,
        row_order=row_order
    ).run()

    # Validate the files in the workspace bucket against the files referenced in the tables
    workspace_bucket = terra.get_workspace_bucket()
    file_references = ExtractFileReferences(tables_info=full_tables_info).run()
    results = ValidateBucketFiles(
        workspace_bucket=workspace_bucket,
        gcp=GCPCloudFunctions(),
        file_references=file_references
    ).run()

    CreateValidationReport(
        workspace_name=workspace_name,
        billing_project=billing_project,
        workspace_bucket=workspace_bucket,
        output_prefix=output_prefix,
        timestamp_string=timestamp_string,
        run_time_display=run_time.strftime("%Y-%m-%d %H:%M:%S"),
        output_dir=output_dir,
        summary_stats_file=output_file,
        summary_stats_content=output_content,
        results=results,
        data_dictionary_file=data_dictionary_file
    ).run()
