import os
import yaml  # type: ignore[import]  # noqa: F401
import re

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
    def __init__(self, workflow_name: str, billing_project: str):
        """
        Initialize the WorkflowConfigs class.

        Args:
            workflow_name (str): The name of the workflow to configure.

        Raises:
            ValueError: If the workflow name is not found in the YAML file.
        """
        self.workflow_name = workflow_name

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
            'wdl_path': os.path.join(WDL_ROOT_DIR_FULL_PATH, yaml_info['primaryDescriptorPath'].lstrip('/')),
            'read_me': os.path.join(WDL_ROOT_DIR_FULL_PATH, yaml_info['readMePath'].lstrip('/')),
            'wdl_name': os.path.basename(yaml_info['primaryDescriptorPath']).rstrip('.wdl'),
            'wdl_workflow_name': self.get_wdl_workflow_name(
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

    def get_wdl_workflow_name(self, wdl_file_path: str) -> str:
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

    def set_input_defaults(self) -> None:
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

    def set_anvil_defaults(self) -> None:
        """
        Set the default input values for the Anvil project in the workflow configuration.
        """
        self.set_input_defaults()
        self.workflow_config["inputs"][f"{self.workflow_info['wdl_workflow_name']}.billing_project"] = "\"anvil-datastorage\""  # type: ignore[index]  # noqa: E501
        self.workflow_config["inputs"][f"{self.workflow_info['wdl_workflow_name']}.tdr_billing_profile"] = "\"e0e03e48-5b96-45ec-baa4-8cc1ebf74c61\""  # type: ignore[index]  # noqa: E501
