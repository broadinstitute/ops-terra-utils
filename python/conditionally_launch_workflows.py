import argparse
import logging
from argparse import ArgumentParser
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional

from ops_utils import comma_separated_list
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.token_util import Token

ACTIVE_WORKFLOWS_MAXIMUM = 40000
DEFAULT_DAYS_BACK_TO_CHECK = 30
logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    parser = ArgumentParser("Conditionally launch new workflows in a Terra workspace.")
    parser.add_argument("--workspace_name", required=True, type=str)
    parser.add_argument("--billing_project", required=True, type=str)
    parser.add_argument("--workflow_name", required=True, type=str)
    parser.add_argument(
        "--entity_type",
        required=True,
        type=str,
        help="The entity type (table name) in Terra to check for submissions. By default, all entities in the table will be included."
    )
    parser.add_argument(
        "--days_back",
        required=True,
        type=int,
        help="How many days back to look for submissions", default=DEFAULT_DAYS_BACK_TO_CHECK
    )
    parser.add_argument(
        "--entities_to_exclude",
        required=False,
        type=comma_separated_list,
        help="List of entity names to exclude.",
        default=[]
    )
    parser.add_argument(
        "--launch_new_submissions",
        action="store_true",
        help="If indicated, will launch new workflows as needed. Otherwise, will not launch new submissions, only retry failed ones."
    )
    return parser.parse_args()


class FilterSubmissionsLaunchFailures:
    def __init__(
            self,
            submissions: list[dict],
            workflow_name: str,
            days_back: int,
            workspace_obj: TerraWorkspace,
            entity_type: str,
            entities_to_exclude: Optional[list[str]],
            billing_project: str,
            launch_new_submissions: bool = False
    ):
        self.submissions = submissions
        self.workflow_name = workflow_name
        self.days_back = days_back
        self.workspace = workspace_obj
        self.entity_type = entity_type
        self.entities_to_exclude = entities_to_exclude
        self.billing_project = billing_project
        self.launch_new_submissions = launch_new_submissions

    def _filter_submissions_by_workflow_name(self) -> list[dict]:
        """Filter submissions in the workspace by workflow name and date (number of days to look back)"""
        filtered_submissions = [
            submission for submission in self.submissions if submission["methodConfigurationName"] == self.workflow_name
        ]

        date_filtered_submissions = []
        now = datetime.now(timezone.utc)
        days_back_to_check = now - timedelta(days=self.days_back)
        for sub in filtered_submissions:
            submission_date = sub["submissionDate"]
            # Parse ISO timestamp ending with Z (UTC)
            dt = datetime.fromisoformat(submission_date.replace("Z", "+00:00"))
            if dt >= days_back_to_check:
                date_filtered_submissions.append(sub)
        logging.info(f"Found {len(date_filtered_submissions)} submissions for workflow '{self.workflow_name}'")
        return date_filtered_submissions

    @staticmethod
    def _find_submission_with_all_terminal_workflows(filtered_submissions: list[dict]) -> list[dict]:
        """Find all submissions where every workflow is in a terminal state (Succeeded or Failed),
        AND does not have any other submissions with the same user comment."""
        submissions_to_check = []
        terminal_statuses = {"Succeeded", "Failed"}
        comment_counts = Counter(sub['userComment'] for sub in filtered_submissions)

        for sub in filtered_submissions:
            workflow_statuses = sub["workflowStatuses"]
            user_comment = sub["userComment"]
            num_submissions_with_same_comment = comment_counts[user_comment]
            # The only submissions eligible to be relaunched are ones that have ONLY succeeded and failed workflows, AND
            # if there is only one submission with that user comment (to avoid relaunching multiple times)
            if set(workflow_statuses.keys()) == terminal_statuses and num_submissions_with_same_comment == 1:
                submissions_to_check.append(sub)
        return submissions_to_check

    def _count_running_or_pending_workflows(self) -> int:
        """Count the number of running or pending workflows in the workspace."""
        workflow_submission_stats = self.workspace.get_workspace_submission_stats(
            method_name=self.workflow_name, retrieve_running_ids=False)
        return workflow_submission_stats.get("submitted") + workflow_submission_stats.get("queued") + workflow_submission_stats.get("running")

    def _find_non_submitted_entities(
            self, workspace_entity_metadata: list[dict], filtered_submissions: list[dict]
    ) -> list[str]:
        """Find entities in the workspace that do not have any submissions yet, excluding specified entities."""
        all_workspace_entities = [e["name"] for e in workspace_entity_metadata if e["entityType"] == self.entity_type]
        logging.info(f"Found {len(all_workspace_entities)} entities for entity type: {self.entity_type}")
        entities_with_to_exclude_removed = [
            e for e in all_workspace_entities if e not in self.entities_to_exclude]  # type:ignore[operator]
        logging.info(
            f"Removed {len(self.entities_to_exclude)} by user request. {len(entities_with_to_exclude_removed)} entities remain after exclusion.")  # type:ignore[arg-type]

        # Get entities that have submissions
        logging.info("Checking for remaining entities without submissions")
        entities_already_submitted = [e["submissionEntity"]["entityName"] for e in filtered_submissions]
        final_entities_to_submit = [
            e for e in entities_with_to_exclude_removed if e not in entities_already_submitted
        ]
        logging.info(f"Found {len(final_entities_to_submit)} entities without submissions after filtering.")
        return final_entities_to_submit

    def filter_and_launch_failed_submissions(self) -> None:
        """Filter submissions in the workspace by workflow name and date. Then look for submissions where ALL workflows
        have failed and re-launched failures."""
        # Filter submissions by workflow name and date
        filtered_submissions = self._filter_submissions_by_workflow_name()
        # Find which submissions are eligible to be relaunched
        submissions_to_relaunch = self._find_submission_with_all_terminal_workflows(
            filtered_submissions=filtered_submissions)
        # Relaunch the failed submissions
        logging.info(f"Found {len(submissions_to_relaunch)} submissions to re-launch")
        if submissions_to_relaunch:
            for submission in submissions_to_relaunch:
                self.workspace.retry_failed_submission(submission["submissionId"])

        # Conditionally launch the next sample set if there aren't too many running/pending workflows
        active_workflows_count = self._count_running_or_pending_workflows()
        # Only launch new submissions if the number of active workflows is below the maximum threshold
        # AND the user has indicated to launch new submissions
        if active_workflows_count < ACTIVE_WORKFLOWS_MAXIMUM:
            logging.info("Active workflows below maximum threshold. Looking to launch next sample set.")
            if self.launch_new_submissions:
                # Get all sample sets from workspace (minus ones to exclude) and launch the next set
                workspace_entity_metadata = self.workspace.get_gcp_workspace_metrics(
                    entity_type=self.entity_type, remove_dicts=True)
                entities_eligible_for_submission = self._find_non_submitted_entities(
                    workspace_entity_metadata=workspace_entity_metadata, filtered_submissions=filtered_submissions
                )
                if entities_eligible_for_submission:
                    expression = self.entity_type.replace("_set", "")
                    entity_name = entities_eligible_for_submission[0]
                    res = self.workspace.initiate_submission(
                        method_config_namespace=self.billing_project,
                        method_config_name=self.workflow_name,
                        entity_type=self.entity_type,
                        entity_name=entity_name,
                        expression=f"this.{expression}s",
                        user_comment=entity_name
                    )
                    logging.info(f"Launched new submission for entity: {entity_name}. Response: {res.status_code}")
                else:
                    logging.info(
                        "No eligible entities found to launch new submissions. It's possible all entities have already been launched")
            else:
                logging.info(
                    "Active workflows below maximum threshold, but not launching new submissions as per user request. Run with the '--launch_new_submissions' flag to enable."
                )
        else:
            logging.info("Active workflows above maximum threshold. Not launching new submissions at this time.")


if __name__ == '__main__':
    args = get_args()
    auth_token = Token()
    request_util = RunRequest(token=auth_token)

    workspace = TerraWorkspace(
        billing_project=args.billing_project, workspace_name=args.workspace_name, request_util=request_util
    )

    all_workspace_submissions = workspace.get_workspace_submission_status().json()

    FilterSubmissionsLaunchFailures(
        submissions=all_workspace_submissions,
        workflow_name=args.workflow_name,
        days_back=args.days_back,
        workspace_obj=workspace,
        entity_type=args.entity_type,
        entities_to_exclude=args.entities_to_exclude,
        billing_project=args.billing_project,
        launch_new_submissions=args.launch_new_submissions
    ).filter_and_launch_failed_submissions()
