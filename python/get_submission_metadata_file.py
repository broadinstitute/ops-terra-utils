import json
import os
import logging
from argparse import ArgumentParser, Namespace
from typing import Optional
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.google_sheets_util import GoogleSheets
from ops_utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="description of script")
    parser.add_argument("--billing_project", "-b")
    parser.add_argument("--workspace_name", "-w")
    parser.add_argument("--submission_id", "-s")
    parser.add_argument("--cloud_directory", "-c")
    parser.add_argument("--use_spreadsheet", "-u", action="store_true")
    return parser.parse_args()


class SubmissionWorkflowHandler:
    """Handles getting successful workflow info from Terra submissions."""

    def __init__(self, terra_workspace: TerraWorkspace):
        """
        Initialize the handler with a TerraWorkspace instance.

        Args:
            terra_workspace: An initialized TerraWorkspace object
        """
        self.terra = terra_workspace

    def get_successful_workflow_info(self, submission_id: str) -> Optional[dict]:
        """
        Get workflow info for a successful workflow within a submission.

        Args:
            submission_id: The submission ID to check

        Returns:
            dict: The workflow info as JSON if a successful workflow is found, None otherwise
        """
        submission_info = self.terra.get_submission_status(submission_id=submission_id).json()
        succeeded_workflow_id = None

        for workflow in submission_info["workflows"]:
            if workflow["status"] == "Succeeded":
                succeeded_workflow_id = workflow["workflowId"]
                break

        if not succeeded_workflow_id:
            logging.error("No succeeded workflow found in the submission.")
            return None

        workflow_info = self.terra.get_workflow_status(
            submission_id=submission_id,
            workflow_id=succeeded_workflow_id,
            expand_sub_workflow_metadata=True
        ).json()

        return workflow_info


class SpreadsheetHandler:
    SPREADSHEET_ID = "1P6PGKdmhtW1c0gj3csDYZaCSG9GOLncrdMmke_gCByk"
    WORKSHEET_NAME = "Workflows"
    SUBMISSION_COLUMN = "Submission"
    METADATA_JSON_COLUMN = "Metadata_json"
    PREFIX_COLUMN = "Prefix"
    SUBMISSION_METADATA_NOTES = "Submission metadata notes"

    def __init__(self, request_util: RunRequest):
        self.gs = GoogleSheets()
        self.request_util = request_util
        self.gcp = GCPCloudFunctions()

    @staticmethod
    def _get_info_from_submission_link( submission_link: str) -> tuple:
        """
        Parse the submission link to extract billing project, workspace name, and submission ID.
        Args:
            submission_link:

        Returns:
            tuple: (billing_project, workspace_name, submission_id)

        """
        parts = submission_link.split('/')
        billing_project = parts[4]
        workspace_name = parts[5]
        submission_id = parts[7].rstrip()
        return billing_project, workspace_name, submission_id

    def _get_output_path(self, terra: TerraWorkspace, submission_row: dict) -> str:
        prefix = submission_row.get(self.PREFIX_COLUMN)
        workspace_bucket = terra.get_workspace_bucket()
        output_path = f"gs://{workspace_bucket}/aou_submission_v{prefix}/"
        return output_path


    def run(self):
        # Get all records from the spreadsheet
        all_records = self.gs.get_worksheet_as_dict(
            spreadsheet_id=self.SPREADSHEET_ID,
            worksheet_name=self.WORKSHEET_NAME
        )
        # Open spreadsheet and go through each row to get submission info
        for idx, record in enumerate(all_records, start=2):
            logging.info(f"Processing record {idx}.")
            workflow_info = SubmissionWorkflowHandler(
                terra_workspace=TerraWorkspace(
                    request_util=self.request_util,
                    billing_project="gates-malaria",
                    workspace_name="Mad4Hatter_development"
                )
            ).get_successful_workflow_info(submission_id="5d9ff2e9-7e27-4a6b-b655-5c776ca55444")
            print(workflow_info)
            exit()


            if record.get(self.METADATA_JSON_COLUMN) or record.get(self.SUBMISSION_METADATA_NOTES):
                logging.info(f"Row {idx} already has metadata or has been checked; skipping.")
                continue

            #print(json.dumps(record, indent=4))
            submission_link = record.get(self.SUBMISSION_COLUMN)

            if not submission_link:
                logging.warning("Missing submission link; skipping row.")
                continue

            billing_project, workspace_name, submission_id = self._get_info_from_submission_link(submission_link)

            terra = TerraWorkspace(
                request_util=self.request_util,
                billing_project=billing_project,
                workspace_name=workspace_name
            )

            workflow_info = SubmissionWorkflowHandler(
                terra_workspace=terra
            ).get_successful_workflow_info(submission_id=submission_id)

            if workflow_info:
                output_cloud_file = os.path.join(
                    self._get_output_path(terra, record),
                    f'{submission_id}.{workflow_info["id"]}.json'
                )
                logging.info(f"Wrote submission {submission_id} to {output_cloud_file}.")
                #self.gcp.write_to_gcp_file(
                #    cloud_path=output_cloud_file,
                #    file_contents=json.dumps(workflow_info, indent=4)
                #)
                logging.info(f"Updating spreadsheet for submission {submission_id}.")
                #self.gs.update_cell(
                #    spreadsheet_id=self.SPREADSHEET_ID,
                #    worksheet_name=self.WORKSHEET_NAME,
                #    cell=f"{self.METADATA_JSON_COLUMN}{idx}",
                #    value=output_cloud_file
                #)
            else:
                logging.info(f"No successful workflow metadata found for submission {submission_id}.")
                #self.gs.update_cell(
                #    spreadsheet_id=self.SPREADSHEET_ID,
                #    worksheet_name=self.WORKSHEET_NAME,
                #    cell=f"{self.SUBMISSION_METADATA_NOTES}{idx}",
                #    value="No successful workflow metadata found"
                #)
                continue


if __name__ == '__main__':
    args = get_args()

    # Get arguments
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    submission_id = args.submission_id
    cloud_directory = args.cloud_directory
    use_spreadsheet = args.use_spreadsheet

    if not use_spreadsheet and not all([billing_project, workspace_name, submission_id, cloud_directory]):
        logging.error("All of billing_project, workspace_name, submission_id, and cloud_directory must be provided if not using spreadsheet.")
        exit(1)
    if any([billing_project, workspace_name, submission_id, cloud_directory]) and use_spreadsheet:
        logging.error("Cannot provide individual parameters when using spreadsheet option.")
        exit(1)

    token = Token()
    request_util = RunRequest(token=token)
    if billing_project:
        terra = TerraWorkspace(
            request_util=request_util,
            billing_project=billing_project,
            workspace_name=workspace_name
        )

        handler = SubmissionWorkflowHandler(terra_workspace=terra)
        workflow_info = handler.get_successful_workflow_info(submission_id=submission_id)

        if workflow_info:
            print(json.dumps(workflow_info, indent=4))
    else:
        SpreadsheetHandler(request_util=request_util).run()
