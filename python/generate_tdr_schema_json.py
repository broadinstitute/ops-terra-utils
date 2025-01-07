import argparse
import json
from pathlib import Path

from python.utils import GCP, ARG_DEFAULTS, comma_separated_list
from python.utils.gcp_utils import GCPCloudFunctions
from python.utils.requests_utils.request_util import RunRequest
from python.utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from python.utils.terra_utils.terra_util import TerraWorkspace
from python.utils.token_util import Token

CLOUD_TYPE = GCP


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get TDR schema JSON file")

    workspace_args = parser.add_argument_group("All arguments required if using workspace metadata as input")
    workspace_args.add_argument(
        "--billing_project",
        required=False,
        type=str,
        help="The billing project for the Terra workspace"
    )
    workspace_args.add_argument(
        "--workspace_name",
        required=False,
        type=str,
        help="The Terra workspace name"
    )
    workspace_args.add_argument(
        "--terra_table_names",
        required=False,
        type=comma_separated_list,
        help="The name(s) of the table(s) within the Terra workspace to generate the schema JSON for"
    )

    tsv_group = parser.add_mutually_exclusive_group(required=False)
    tsv_group.add_argument(
        "--input_tsv",
        required=False,
        type=str,
        help="The GCP path to the tsv containing the metadata to generate a schema JSON for",
    )

    parser.add_argument(
        "--force_disparate_rows_to_string",
        action="store_true",
        help="If used, all rows in a column containing disparate data types will be forced to a string"
    )

    args = parser.parse_args()

    # Custom validation logic
    workspace_args = [args.billing_project, args.workspace_name, args.terra_table_names]
    if args.input_tsv:
        if any(workspace_args):
            parser.error(
                "Cannot provide BOTH input_tsv AND the combination of billing_project, workspace_name, "
                "and terra_table_names."
            )
    else:
        if not all(workspace_args):
            parser.error(
                "If input_tsv is not provided, you must provide ALL of workspace_name, billing_project, "
                "and terra_table_names."
            )

    return args


if __name__ == '__main__':
    args = get_args()

    schema_metadata = []

    if args.input_tsv:
        metadata = GCPCloudFunctions().read_tsv_as_list_of_dictionaries(cloud_path=args.input_tsv)
        schema = InferTDRSchema(
            input_metadata=metadata,
            table_name=Path(args.input_tsv).stem,
            all_fields_non_required=False,
            allow_disparate_data_types_in_column=args.force_disparate_rows_to_string,
        ).infer_schema()
        schema_metadata.append(schema)
    else:
        token = Token(cloud=CLOUD_TYPE)
        request_util = RunRequest(
            token=token,
            max_retries=ARG_DEFAULTS["max_retries"],
            max_backoff_time=ARG_DEFAULTS["max_backoff_time"],
        )
        terra_workspace = TerraWorkspace(
            billing_project=args.billing_project,
            workspace_name=args.workspace_name,
            request_util=request_util
        )
        for table_name in args.terra_table_names:
            metadata = [
                a["attributes"] for a in terra_workspace.get_gcp_workspace_metrics(
                    entity_type=table_name, remove_dicts=True
                )
            ]
            schema = InferTDRSchema(
                input_metadata=metadata,
                table_name=table_name,
                all_fields_non_required=False,
                allow_disparate_data_types_in_column=args.force_disparate_rows_to_string,
            ).infer_schema()

            schema_metadata.append(schema)

    with open(f"schema.json", "w") as schema_json:
        schema_json.write(json.dumps(schema_metadata, indent=2))
