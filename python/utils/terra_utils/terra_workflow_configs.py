import os
import yaml  # type: ignore[import]  # noqa: F401
import re
from .terra_util import TerraWorkspace
import logging

DOCKER_IMAGE = "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"
# Define the relative path to the file
DOCKSTORE_YAML = "../../../.dockstore.yml"
WDL_ROOT_DIR = "../../../"

# Get the absolute path to the file based on the script's location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YAML_FILE_FULL_PATH = os.path.join(SCRIPT_DIR, DOCKSTORE_YAML)
WDL_ROOT_DIR_FULL_PATH = os.path.join(SCRIPT_DIR, WDL_ROOT_DIR)

ANVIL_TERRA_BILLING_PROJECT = "anvil-datastorage"
ANVIL_TDR_BILLING_PROFILE = "e0e03e48-5b96-45ec-baa4-8cc1ebf74c61"


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
            set_defaults: bool = False,
            is_anvil: bool = False
    ):
        """
        Initialize the WorkflowConfigs class.

        Args:
            workflow_name (str): The name of the workflow to configure.
            billing_project (str): The billing project to use for the workflow.
            terra_workspace_util (TerraWorkspace): The TerraWorkspace utility object.
            set_defaults (bool): Whether to set the default input values for the workflow configuration.
            is_anvil (bool): Whether the workflow is for an Anvil project.

        Raises:
            ValueError: If the workflow name is not found in the YAML file.
        """
        self.workflow_name = workflow_name
        self.terra_workspace_util = terra_workspace_util
        self.set_defaults = set_defaults
        self.is_anvil = is_anvil

        # Check if the workflow name is in the YAML file
        available_workflows = GetWorkflowNames().get_workflow_names()
        if workflow_name not in available_workflows:
            raise ValueError(f"Workflow name {workflow_name} not found in {YAML_FILE_FULL_PATH}: {available_workflows}")

        # Load the YAML file
        with open(YAML_FILE_FULL_PATH, 'r') as file:
            yaml_data = yaml.safe_load(file)
        # Extract specific workflow information from yaml_data
        yaml_info = next(workflow for workflow in yaml_data['workflows'] if workflow['name'] == self.workflow_name)
        self.workflow_info = {
            # Remove the leading slash from the path so join works correctly
            'wdl_path': os.path.join(WDL_ROOT_DIR_FULL_PATH, yaml_info['primaryDescriptorPath'].lstrip('/')),
            'read_me': os.path.join(WDL_ROOT_DIR_FULL_PATH, yaml_info['readMePath'].lstrip('/')),
            'wdl_name': os.path.basename(yaml_info['primaryDescriptorPath']).rstrip('.wdl'),
            'wdl_workflow_name': self._get_wdl_workflow_name(
                os.path.join(WDL_ROOT_DIR_FULL_PATH, yaml_info['primaryDescriptorPath'].lstrip('/'))
            )
        }
        # Set the initial workflow configuration
        self.workflow_config = {
            "deleted": False,
            "methodConfigVersion": 0,
            "methodRepoMethod": {
                "sourceRepo": "dockstore",
                "methodVersion": "main",
                "methodUri": f"dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%{self.workflow_name}/main",
                "methodPath": f"github.com/broadinstitute/ops-terra-utils/{self.workflow_name}"
            },
            "name": self.workflow_name,
            "namespace": billing_project,
            "outputs": {},
            "inputs": {}
        }
        # Set the default input values. Will not change anything if set_defaults is False
        self._set_up_input_defaults()

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

    def _set_input_defaults(self) -> None:
        """
        Set the default input values for the workflow configuration.
        """
        self.workflow_config["inputs"] = {
            f"{self.workflow_info['wdl_workflow_name']}.docker": f"\"{DOCKER_IMAGE}\"",
            f"{self.workflow_info['wdl_workflow_name']}.max_retries": "5",
            f"{self.workflow_info['wdl_workflow_name']}.max_backoff_time": "300",
            f"{self.workflow_info['wdl_workflow_name']}.update_strategy": "\"REPLACE\"",
            f"{self.workflow_info['wdl_workflow_name']}.bulk_mode": "false",
            f"{self.workflow_info['wdl_workflow_name']}.workers": "10",
            f"{self.workflow_info['wdl_workflow_name']}.batch_size": "500",
            f"{self.workflow_info['wdl_workflow_name']}.batch_size_to_list_files": "20000",
            f"{self.workflow_info['wdl_workflow_name']}.file_ingest_batch_size": "500",
        }

    def _set_anvil_defaults(self) -> None:
        """
        Set the default input values for the Anvil project in the workflow configuration.
        """
        self._set_input_defaults()
        self.workflow_config["inputs"][f"{self.workflow_info['wdl_workflow_name']}.billing_project"] = f"\"{ANVIL_TERRA_BILLING_PROJECT}\""  # type: ignore[index]  # noqa: E501
        self.workflow_config["inputs"][f"{self.workflow_info['wdl_workflow_name']}.tdr_billing_profile"] = f"\"{ANVIL_TDR_BILLING_PROFILE}\""  # type: ignore[index]  # noqa: E501

    def import_workflow(self, continue_if_exists: bool = False) -> int:
        """
        Import the workflow into the Terra workspace.

        Args:
            continue_if_exists (bool, optional): Whether to continue if the workflow already exists. Defaults to False.

        Returns:
            int: The status code of the import operation.
        """
        logging.info(f"Importing {self.workflow_name} into {self.terra_workspace_util}")
        return self.terra_workspace_util.import_workflow(workflow_dict=self.workflow_config, continue_if_exists=continue_if_exists)

    def _set_up_input_defaults(self) -> None:
        """
        Set up the default input values for the workflow configuration.

        If `set_defaults` is True, it sets the default input values. If `is_anvil` is True, it sets the Anvil-specific defaults.
        """
        if self.set_defaults:
            if self.is_anvil:
                self._set_anvil_defaults()
            else:
                self._set_input_defaults()
