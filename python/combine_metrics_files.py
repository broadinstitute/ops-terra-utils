import logging
import os.path
from typing import Optional
from argparse import Namespace, ArgumentParser

from ops_utils.vars import GCP
from ops_utils.terra_utils.terra_util import TerraWorkspace
from ops_utils.requests_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("-b", "--billing_project", type=str, required=True)
    parser.add_argument("-w", "--workspace_name", type=str, required=True)
    parser.add_argument("-t", "--table_name", type=str, required=True)
    parser.add_argument("-m", "--metrics_file_column", type=str, required=True)
    parser.add_argument(
        "-identifier", "--identifier_column", type=str,
        help="If not supplied will use file name as identifier."
    )
    parser.add_argument("-o", "--output_gcp_path", type=str, required=True)
    return parser.parse_args()


class CombineMetricFilesContents:
    def __init__(
            self,
            terra_metrics: list[dict],
            metric_column: str,
            id_column: Optional[str],
            gcp_functions: GCPCloudFunctions
    ):
        self.terra_metrics = terra_metrics
        self.id_column = id_column
        self.metric_column = metric_column
        self.gcp_functions = gcp_functions

    def _get_identifier_to_metric_map(self) -> dict:
        """Return a dictionary mapping the identifier to the metric file."""
        metrics_mapping_dict = {}
        valid = True
        for row in self.terra_metrics:
            # If id column is supplied, use that as the identifier
            if self.id_column:
                identifier = row[self.id_column]
            # If not supplied, use the file name as the identifier
            else:
                identifier = os.path.basename(row[self.metric_column])
            if identifier not in metrics_mapping_dict:
                metrics_mapping_dict[identifier] = row[self.metric_column]
            else:
                valid = False
                logging.warning(f"Duplicate identifier found: {identifier}. Identifier must be unique.")
        if not valid:
            raise ValueError("Duplicate identifiers found. Please ensure that the identifiers are unique.")
        return metrics_mapping_dict

    def _read_file_and_add_identifier_column(self, file_path: str, identifier: str) -> list[str]:
        """Read the file and add the identifier column to the data."""
        logging.info(f"Reading file: {file_path}")
        file_contents = self.gcp_functions.read_file(file_path)
        if file_path.endswith(".csv"):
            delimiter = ","
        elif file_path.endswith(".tsv"):
            delimiter = "\t"
        else:
            raise ValueError(f"Unsupported file type: {file_path}. Must be .csv or .tsv.")
        updated_file_lines = [
            f'{identifier}{delimiter}{line}'
            for line in file_contents.strip().splitlines()
        ]
        return updated_file_lines

    def run(self) -> list[str]:
        mapping_dict = self._get_identifier_to_metric_map()
        full_metrics_contents = []
        for identifier, file_path in mapping_dict.items():
            file_contents = self._read_file_and_add_identifier_column(file_path, identifier)
            if file_contents:
                full_metrics_contents.extend(file_contents)
        return full_metrics_contents


if __name__ == '__main__':
    args = parse_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    table_name = args.table_name
    metrics_file_column = args.metrics_file_column
    identifier_column = args.identifier_column
    output_gcp_path = args.output_gcp_path

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    # Initialize the source Terra workspace classes
    workspace_util = TerraWorkspace(
        billing_project=billing_project, workspace_name=workspace_name, request_util=request_util
    )
    table_metrics = workspace_util.get_gcp_workspace_metrics(entity_type=table_name)

    gcp_functions = GCPCloudFunctions()
    # Combine the metrics files and add the identifier column
    full_metrics_list = CombineMetricFilesContents(
        terra_metrics=table_metrics,
        metric_column=metrics_file_column,
        id_column=identifier_column,
        gcp_functions=gcp_functions
    ).run()
    # Write the combined metrics to the output file
    logging.info(f"Writing to {output_gcp_path}")
    gcp_functions.write_to_gcs(cloud_path=output_gcp_path, content='\n'.join(full_metrics_list))
    logging.info(f"Finished writing to {output_gcp_path}")
