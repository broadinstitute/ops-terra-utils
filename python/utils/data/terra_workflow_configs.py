

class WorkflowConfigs:
    def __init__(self) -> None:
        self.base_dict = {
            "deleted": False,
            "methodConfigVersion": 0,
            "methodRepoMethod": {
                "sourceRepo": "dockstore",
                "methodVersion": "main"
            }
        }

    def list_workflows(self) -> list:
        workflow_list = ["AnvilGcpWorkspaceToDatasetCreationAndIngest",
                         "CopyDatasetToNewBillingProfile",
                         "DeleteBadStateFilesFromDataset",
                         "ExportDataFromSnapshotToBucket",
                         "FileExportAzureTdrToGcp",
                         "GetAndDeleteOrphanedFilesFromDataset",
                         "RenameAndReingestFiles",
                         "TerraWorkspaceTableIngest"]

        return workflow_list

    """
   def example_workflow(self) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "workflow_name"
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {}
        workflow_config["methodRepoMethod"]["methodUri"] = ""  # mypy: disable-error-code="index"
        workflow_config["methodRepoMethod"]["methodPath"] = "" # mypy: disable-error-code="index"
        return workflow_config
    """

    def AnvilGcpWorkspaceToDatasetCreationAndIngest(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "AnvilGcpWorkspaceToDatasetCreationAndIngest"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "billing_project": "String",
            "workspace_name": "String",
            "phs_id": "String",
            "already_added_to_auth_domain": "Boolean",
            "filter_existing_ids": "Boolean",
            "bulk_mode": "Boolean",
            "self_hosted": "Boolean",
            "file_path_flat": "Boolean",
            "dataset_name": "String",
            "update_strategy": "String",
            "docker": "String",
            "tdr_billing_profile": "String",
            "file_ingest_batch_size": "Int",
            "max_backoff_time": "Int",
            "max_retries": "Int",
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FAnvilGcpWorkspaceToDatasetCreationAndIngest/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/AnvilGcpWorkspaceToDatasetCreationAndIngest"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def CopyDatasetToNewBillingProfile(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "CopyDatasetToNewBillingProfile"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "new_billing_profile": "String",
            "orig_dataset_id": "String",
            "new_dataset_name": "String",
            "bulk_mode": "Boolean",
            "waiting_time_to_poll": "Int",
            "ingest_batch_size": "Int",
            "update_strategy": "String",
            "docker_name": "String"
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FCopyDatasetToNewBillingProfile/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/CopyDatasetToNewBillingProfile"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def DeleteBadStateFilesFromDataset(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "GetAndDeleteOrphanedFilesFromDataset"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "dataset_id": "String",
            "file_query_limit": "Int",
            "docker": "String"
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FGetAndDeleteOrphanedFilesFromDataset/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/GetAndDeleteOrphanedFilesFromDataset"  # type: ignore[index]  # noqa: E501
        return workflow_config

    """
    def ExportDataFromDatasetToBucket(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "ExportDataFromDatasetToBucket"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "dataset_id": "String",
            "output_bucket": "String",
            "download_type": "String",
            "max_backoff_time": "Int",
            "max_retries": "Int",
            "docker": "String"
        }
        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FExportDataFromDatasetToBucket/main"  # mypy: disable-error-code="index"  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/ExportDataFromDatasetToBucket"  # mypy: disable-error-code="index"  # noqa: E501
        return workflow_config
    """

    def ExportDataFromSnapshotToBucket(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "ExportDataFromSnapshotToBucket"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "snapshot_id": "String",
            "output_bucket": "String",
            "download_type": "String",
            "max_backoff_time": "Int",
            "max_retries": "Int",
            "docker": "String"
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FExportDataFromSnapshotToBucket/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/ExportDataFromSnapshotToBucket"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def FileExportAzureTdrToGcp(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "FileExportAzureTdrToGcp"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "export_type": "String",
            "target_id": "String",
            "bucket_id": "String",
            "bucket_output_path": "String",
            "retain_path_structure": "Boolean",
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FFileExportAzureTdrToGcp/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/FileExportAzureTdrToGcp"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def GetAndDeleteOrphanedFilesFromDataset(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "GetAndDeleteOrphanedFilesFromDataset"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "dataset_id": "String",
            "max_retries": "Int",
            "max_backoff_time": "Int",
            "batch_size_to_list_files": "Int",
            "docker": "String",
            "batch_size_to_delete_files": "Int",
            "delete_orphaned_files": "Boolean"
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FGetAndDeleteOrphanedFilesFromDataset/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/GetAndDeleteOrphanedFilesFromDataset"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def RenameAndReingestFiles(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "RenameAndReingestFiles"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "dataset_id": "String",
            "original_file_basename_column": "String",
            "new_file_basename_column": "String",
            "dataset_table_name": "String",
            "row_identifier": "String",
            "copy_and_ingest_batch_size": "Int",
            "workers": "Int",
            "max_retries": "Int",
            "max_backoff_time": "Int",
            "docker": "String",
            "billing_project": "String",
            "workspace_name": "String",
            "temp_bucket": "String",
            "report_updates_only": "Boolean"
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FRenameAndReingestFiles/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/RenameAndReingestFiles"  # type: ignore[index]  # noqa: E501
        return workflow_config

    def TerraWorkspaceTableIngest(self, billing_project: str) -> dict:
        workflow_config = self.base_dict.copy()
        workflow_config["name"] = "TerraWorkspaceTableIngest"
        workflow_config["namespace"] = billing_project
        workflow_config["outputs"] = {}
        workflow_config["inputs"] = {
            "billing_project": "String",
            "workspace_name": "String",
            "dataset_id": "String",
            "terra_table_name": "String",
            "target_table_name": "String",
            "primary_key_column_name": "String",
            "update_strategy": "String",
            "records_to_ingest": "String",
            "bulk_mode": "Boolean",
            "max_retries": "Int",
            "max_backoff_time": "Int",
            "docker": "String",
        }

        workflow_config["methodRepoMethod"]["methodUri"] = "dockstore://github.com%2Fbroadinstitute%2Fops-terra-utils%2FTerraWorkspaceTableIngest/main"  # type: ignore[index]  # noqa: E501
        workflow_config["methodRepoMethod"]["methodPath"] = "github.com/broadinstitute/ops-terra-utils/TerraWorkspaceTableIngest"  # type: ignore[index]  # noqa: E501
        return workflow_config
