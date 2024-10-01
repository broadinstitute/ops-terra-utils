

class WorkflowConfigs:
    def __init__(self, billing_project: str):
        self.base_dict = {
            "deleted": False,
            "namespace": billing_project,
            "methodConfigVersion": 0,
            "methodRepoMethod": {
                "sourceRepo": "dockstore",
                "methodVersion": "main"
            }
        }

    def example_workflow(self) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "workflow_name"
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {}
        workflow_config["methodRepoMethod"]["methodUri"] = ""  # type: ignore[index]
        workflow_config["methodRepoMethod"]["methodPath"] = ""  # type: ignore[index]
        return workflow_config
