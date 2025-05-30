import logging
from argparse import ArgumentParser, Namespace
import numpy as np

from pandas import DataFrame
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv
from ops_utils.tdr_utils.tdr_bq_utils import TdrBq, GetTdrAssetInfo

FILE_REF = "fileref"
OUTPUT_FILE = "summary_statistics.tsv"

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="description of script")
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
    mutually_exclusive_group.add_argument("--dataset_id", "-d")
    mutually_exclusive_group.add_argument("--snapshot_id", "-s")
    return parser.parse_args()


class CreateSchemaDict:
    def __init__(self, tdr_table_info: dict, relationships: list[dict]):
        self.tdr_table_info = tdr_table_info
        self.relationships = relationships

    def _create_relationship_dict(self) -> dict:
        relationship_dict = {}
        for relationship in self.relationships:
            source_info = relationship["to"]
            linked_info = relationship["from"]
            relationship_dict[(linked_info["table"], linked_info["column"])] = {
                'source_table': source_info["table"],
                'source_column': source_info["column"],
            }
        return relationship_dict

    def run(self) -> dict:
        schema_dict = {}
        relationship_dict = self._create_relationship_dict()
        for table in self.tdr_table_info:
            table_dict = {
                'primaryKey': table["primaryKey"],
                'columns': {}
            }
            table_name = table["name"]
            for column in table["columns"]:
                column_dict = {
                    "datatype": column["datatype"],
                    "array_of": column["array_of"],
                    "required": column["required"],
                    "primary_key": True if column["name"] in table["primaryKey"] else False
                }
                # Add relationship info to column_dict if it exists
                if (table_name, column["name"]) in relationship_dict:
                    column_dict['source_relationship'] = relationship_dict[(table_name, column["name"])]
                table_dict['columns'][column["name"]] = column_dict
            schema_dict[table_name] = table_dict
        return schema_dict


class GetTableContents:
    def __init__(self, tdr_bq_util: TdrBq, table_schema_dict: dict, bq_project: str, bq_schema: str):
        self.tdr_bq_util = tdr_bq_util
        self.bq_project = bq_project
        self.bq_schema = bq_schema
        self.table_schema_dict = table_schema_dict

    def run(self) -> tuple[dict, DataFrame]:
        table_contents_dict = {}
        for table_name in self.table_schema_dict.keys():
            table_contents_dict[table_name] = self.tdr_bq_util.get_tdr_table_contents(
                table_name=table_name,
                to_dataframe=True,
                exclude_datarepo_id=True
            )
        load_history_df = self.tdr_bq_util.get_tdr_table_contents(
            table_name='datarepo_load_history',
            to_dataframe=True,
            exclude_datarepo_id=False
        )
        return table_contents_dict, load_history_df


class CreateSummaryStatistics:
    def __init__(self, table_contents_dict: dict, table_schema_dict: dict, file_load_df: DataFrame):
        self.table_contents_dict = table_contents_dict
        self.table_schema_dict = table_schema_dict
        self.file_load_df = file_load_df

    def _create_source_relationship_dict(self) -> dict:
        # Preprocess all source relationships into sets for fast lookup
        foreign_key_sets = {}
        for table_name, metadata in self.table_schema_dict.items():
            for column_name, column_info in metadata["columns"].items():
                if "source_relationship" in column_info:
                    source_table = column_info["source_relationship"]["source_table"]
                    source_column = column_info["source_relationship"]["source_column"]
                    if source_table in self.table_contents_dict:
                        if (source_table, source_column) not in foreign_key_sets:
                            foreign_key_sets[(source_table, source_column)] = set(
                                self.table_contents_dict[source_table][source_column].dropna())
        return foreign_key_sets

    def _get_uploaded_files(self) -> set:
        return set(self.file_load_df[file_load_df["state"] == "succeeded"]["file_id"])

    def analyze_tables(self) -> dict:
        results: dict = {'table_info': {}}
        foreign_key_sets = self._create_source_relationship_dict()
        uploaded_files = self._get_uploaded_files()
        referenced_files = set()

        for table_name, df in self.table_contents_dict.items():
            total_records = len(df)
            table_result = {"total_records": total_records, "columns": {}}

            for column_name in df.columns:
                column_info = self.table_schema_dict[table_name]["columns"][column_name]
                column_result = {
                    "column_type": column_info["datatype"],
                    "is_array": column_info["array_of"]
                }

                # Handle arrays by converting to sorted strings
                processed_column = df[column_name].apply(
                    lambda x: ", ".join(sorted(map(str, x))) if isinstance(x, (list, set, np.ndarray)) else x
                )

                # Count empty cells (including null and empty arrays)
                column_result["empty_cells"] = int(processed_column.isnull().sum() + (processed_column == "").sum())

                # Count distinct values (using sorted strings for consistency)
                column_result["distinct_values"] = int(processed_column.nunique())

                # Check for foreign key linkage
                if "source_relationship" in column_info:
                    source_table = column_info["source_relationship"]["source_table"]
                    source_column = column_info["source_relationship"]["source_column"]
                    fk_set = foreign_key_sets.get((source_table, source_column), set())
                    unmatched_count = int(
                        processed_column[~processed_column.isin(fk_set) & processed_column.notnull()].shape[0])
                    column_result["unmatched_foreign_keys"] = unmatched_count

                # Collect fileref values if datatype is FILE_REF
                if column_info["datatype"] == FILE_REF:
                    fileref_values = df[column_name].dropna()
                    for value in fileref_values:
                        if isinstance(value, (list, set, np.ndarray)):
                            referenced_files.update(value)  # Flatten and add individual items
                        else:
                            referenced_files.add(value)
                table_result["columns"][column_name] = column_result  # type: ignore[index]

            results['table_info'][table_name] = table_result

        # Calculate orphaned files
        orphaned_files = uploaded_files - referenced_files
        results["orphaned_files"] = len(orphaned_files)
        return results


class WriteTsv:
    HEADERS = [
        "Table", "Column", "Column Type", "Column Array", "Total Table Rows", "Empty Cells",
        "Distinct Values", "Unmatched Foreign Keys", "Flagged", "Flag Reason"
    ]

    def __init__(self, results: dict):
        self.results = results

    def run(self) -> None:
        na = "N/A"
        orphaned_files = self.results["orphaned_files"]
        tsv_data = [
            {
                "Table": "Orphaned Files",
                "Column": na,
                "Column Type": na,
                "Column Array": na,
                "Total Table Rows": self.results["orphaned_files"],
                "Empty Cells": na,
                "Distinct Values": na,
                "Unmatched Foreign Keys": na,
                "Flagged": True if orphaned_files > 0 else False,
                "Flag Reason": "Orphaned files" if orphaned_files > 0 else ""
            }
        ]
        for table_name, table_info in self.results["table_info"].items():
            total_record = table_info["total_records"]
            tsv_data.append(
                {
                    "Table": table_name,
                    "Column": na,
                    "Column Type": na,
                    "Column Array": na,
                    "Total Table Rows": table_info["total_records"],
                    "Empty Cells": na,
                    "Distinct Values": na,
                    "Unmatched Foreign Keys": na,
                    # Flag table if it has no records
                    "Flagged": True if total_record == 0 else False,
                    "Flag Reason": "No records" if total_record == 0 else ""
                }
            )
            for column_name, column_info in table_info["columns"].items():
                tsv_data.append(
                    {
                        "Table": table_name,
                        "Column": column_name,
                        "Column Type": column_info["column_type"],
                        "Column Array": column_info["is_array"],
                        "Total Table Rows": table_info["total_records"],
                        "Empty Cells": column_info["empty_cells"],
                        "Distinct Values": column_info["distinct_values"],
                        "Unmatched Foreign Keys": column_info.get("unmatched_foreign_keys", na),
                        "Flagged": True if column_info.get("unmatched_foreign_keys", 0) > 0 else False,
                        "Flag Reason": "Unmatched foreign keys" if
                        column_info.get("unmatched_foreign_keys", 0) > 0 else ""
                    }
                )
        Csv(file_path=OUTPUT_FILE).create_tsv_from_list_of_dicts(
            header_list=self.HEADERS,
            list_of_dicts=tsv_data
        )


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    snapshot_id = args.snapshot_id

    token = Token()
    request_util = RunRequest(token=token, max_retries=1, max_backoff_time=1)
    tdr = TDR(request_util=request_util)

    asset_info_dict = GetTdrAssetInfo(tdr=tdr, dataset_id=dataset_id, snapshot_id=snapshot_id).run()

    table_schema_dict = CreateSchemaDict(
        tdr_table_info=asset_info_dict["tables"],
        relationships=asset_info_dict["relationships"]
    ).run()

    tdr_bq_util = TdrBq(
        project_id=asset_info_dict["bq_project"],
        bq_schema=asset_info_dict["bq_schema"]
    )
    if not tdr_bq_util.check_permissions(raise_on_other_failure=True):
        raise ValueError(
            "User does not have sufficient permissions to access BigQuery project. If running in WDL "
            "it can take time for permissions to propagate. You can try to run the task again later. "
            f"To check project manually you can try going to the BigQuery console going "
            f"to project {asset_info_dict['bq_project']}."
        )
    table_contents_dict, file_load_df = GetTableContents(
        tdr_bq_util=tdr_bq_util,
        table_schema_dict=table_schema_dict,
        bq_project=asset_info_dict["bq_project"],
        bq_schema=asset_info_dict["bq_schema"]
    ).run()

    results = CreateSummaryStatistics(
        table_contents_dict=table_contents_dict,
        table_schema_dict=table_schema_dict,
        file_load_df=file_load_df
    ).analyze_tables()

    WriteTsv(results).run()
