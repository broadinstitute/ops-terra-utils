import os
import yaml  # type: ignore[import]  # noqa: F401
import re
from .terra_util import TerraWorkspace
from .. import ARG_DEFAULTS
import logging

DOCKER_IMAGE = "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"
# Define the relative path to the file
DOCKSTORE_YAML = "../../../.dockstore.yml"
WDL_ROOT_DIR = "../../../"

# Get the absolute path to the file based on the script's location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YAML_FILE_FULL_PATH = os.path.join(SCRIPT_DIR, DOCKSTORE_YAML)
WDL_ROOT_DIR_FULL_PATH = os.path.join(SCRIPT_DIR, WDL_ROOT_DIR)


class GetWorkflowNames:
    def __init__(self) -> None:
        """
        Initialize the GetWorkflowNames class.

        Loads the YAML file and extracts workflow names.
        """
        # Load the YAML file
        with open(YAML_FILE_FULL_PATH, 'r') as file:
            yaml_data = yaml.safe_load(file)

        # Extract workflow names and store them
        self.workflow_names = [
            workflow_info['name']
            for workflow_info in yaml_data['workflows']
        ]

    def get_workflow_names(self) -> list[str]:
        """
        Get the list of workflow names.

        Returns:
            list: A list of workflow names.
        """
        return list(self.workflow_names)


class WorkflowConfigs:
    def __init__(
            self,
            workflow_name: str,
            billing_project: str,
            terra_workspace_util: TerraWorkspace,
            set_input_defaults: bool = False,
            extra_default_inputs: dict = {}
    ):
        """
        Initialize the WorkflowConfigs class.

        Args:
            workflow_name (str): The name of the workflow to configure.
            billing_project (str): The billing project to use for the workflow.
            terra_workspace_util (TerraWorkspace): The TerraWorkspace utility object.
            set_input_defaults (bool): Whether to set the default input values for the workflow configuration.
            tdr_billing_profile (str): The TDR billing profile for workflow.

        Raises:
            ValueError: If the workflow name is not found in the YAML file.
        """
        self.workflow_name = workflow_name
        self.terra_workspace_util = terra_workspace_util
        self.set_input_defaults = set_input_defaults
        self.billing_project = billing_project
        self.extra_default_inputs = extra_default_inputs

        # Check if the workflow name is in the YAML file
        available_workflows = GetWorkflowNames().get_workflow_names()
        if workflow_name not in available_workflows:
            raise ValueError(f"Workflow name {workflow_name} not found in {YAML_FILE_FULL_PATH}: {available_workflows}")

        # Load the YAML file
        with open(YAML_FILE_FULL_PATH, 'r') as file:
            yaml_data = yaml.safe_load(file)
        # Extract specific workflow information from yaml_data
        self.yaml_info = next(workflow for workflow in yaml_data['workflows'] if workflow['name'] == self.workflow_name)
        self.workflow_info = self._create_workflow_info_dict()
        # Set the initial workflow configuration
        self.workflow_config = self._create_up_workflow_config()

    def _create_wdl_absolute_path(self, relative_path: str) -> str:
        """
        Create an absolute path from a relative path. Will join the relative path with the script's directory.

        Args:
            relative_path (str): The relative path to convert.

        Returns:
            str: The absolute path.
        """
        return os.path.join(WDL_ROOT_DIR_FULL_PATH, relative_path.lstrip('/'))

    @staticmethod
    def _get_wdl_workflow_name(wdl_file_path: str) -> str:
        """
        Get the WDL workflow name from the WDL file.

        Args:
            wdl_file_path (str): The path to the WDL file.

        Returns:
            str: The name of the WDL workflow.

        Raises:
            ValueError: If the workflow name is not found in the WDL file.
        """
        with open(wdl_file_path, 'r') as file:
            for line in file:
                # Search for the workflow name in the WDL file
                match = re.search(r'^workflow\s+(\w+)\s', line)
                if match:
                    return match.group(1)
        raise ValueError(f"Workflow name not found in {wdl_file_path}")

    def _create_input_defaults(self) -> dict:
        """
        Set the default input values for the workflow configuration.
        """
        workflow = self.workflow_info['wdl_workflow_name']
        workflow_default_inputs = {
            f"{workflow}.docker": f"\"{ARG_DEFAULTS['docker_image']}\"",
            f"{workflow}.max_retries": f"{ARG_DEFAULTS['max_retries']}",
            f"{workflow}.max_backoff_time": f"{ARG_DEFAULTS['max_backoff_time']}",
            f"{workflow}.update_strategy": f"\"{ARG_DEFAULTS['update_strategy']}\"",
            f"{workflow}.bulk_mode": "false",
            f"{workflow}.workers": f"{ARG_DEFAULTS['multithread_workers']}",
            f"{workflow}.batch_size": f"{ARG_DEFAULTS['batch_size']}",
            f"{workflow}.batch_size_to_list_files": f"{ARG_DEFAULTS['batch_size_to_list_files']}",
            f"{workflow}.file_ingest_batch_size": f"{ARG_DEFAULTS['file_ingest_batch_size']}",
            f"{workflow}.waiting_time_to_poll": f"{ARG_DEFAULTS['waiting_time_to_poll']}",
            f"{workflow}.billing_project": f"\"{self.terra_workspace_util.billing_project}\"",
            f"{workflow}.workspace_name": f"\"{self.terra_workspace_util.workspace_name}\""
        }
        for key, value in self.extra_default_inputs.items():
            workflow_default_inputs[f"{workflow}.{key}"] = value
        return workflow_default_inputs

    def import_workflow(self, continue_if_exists: bool = False) -> int:
        """
        Import the workflow into the Terra workspace.

        Args:
            continue_if_exists (bool, optional): Whether to continue if the workflow already exists. Defaults to False.

        Returns:
            int: The status code of the import operation.
        """
        logging.info(f"Importing {self.workflow_name} into {self.terra_workspace_util}")
        return self.terra_workspace_util.import_workflow(
            workflow_dict=self.workflow_config,
            continue_if_exists=continue_if_exists
        )

    def _create_workflow_info_dict(self) -> dict:
        """
        Create a dictionary containing workflow information.

        This method constructs a dictionary with the absolute paths to the WDL file and the README file,
        the name of the WDL workflow, and the base name of the WDL file.

        Returns:
            dict: A dictionary containing workflow information.
        """
        wdl_path = self._create_wdl_absolute_path(self.yaml_info['primaryDescriptorPath'])
        return {
            'wdl_path': wdl_path,
            'read_me': self._create_wdl_absolute_path(self.yaml_info['readMePath']),
            'wdl_workflow_name': self._get_wdl_workflow_name(wdl_path),
            'wdl_name': os.path.basename(self.yaml_info['primaryDescriptorPath']).rstrip('.wdl'),
            'read_me_link': f'https://dockstore.org/workflows/github.com/broadinstitute/ops-terra-utils/{self.workflow_name}'  # noqa: E501
        }

    def _create_up_workflow_config(self) -> dict:
        """
        Set up the default input values for the workflow configuration.

        If `set_defaults` is True, it sets the default input values.
        If `is_anvil` is True, it sets the Anvil-specific defaults.
        """
        return {
            "deleted": False,
            "methodConfigVersion": 0,
            "methodRepoMethod": {
                "sourceRepo": "dockstore",
                "methodVersion": "main",
                "methodUri": f"dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%{self.workflow_name}/main",
                "methodPath": f"github.com/broadinstitute/ops-terra-utils/{self.workflow_name}"
            },
            "name": self.workflow_name,
            "namespace": self.billing_project,
            "outputs": {},
            "inputs": self._create_input_defaults() if self.set_input_defaults else {},
        }
