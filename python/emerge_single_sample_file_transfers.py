from pathlib import Path
import csv
from io import StringIO
import logging

from ops_utils.gcp_utils import GCPCloudFunctions, MOVE
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

SOURCE_WORKSPACE_NAME = "AnVIL_eMERGE_eMERGEseq"
BILLING_PROJECT = "anvil-datastorage"


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


class ProcessFilesGetMapping():
    def __init__(self, gcp_cloud_functions, request_util):
        self.gcp_functions = gcp_cloud_functions
        self.request_util = request_util

    def list_all_bams(self):
        all_bams = self.gcp_functions.list_bucket_contents(
            bucket_name="fc-secure-50f2310f-659c-4c60-af42-e028b58bad52",
            prefix="eMERGEseq/bam_files/",
            file_extensions_to_include=[".bam"],
            file_name_only=True,
        )
        return [z["path"] for z in all_bams]

    def list_all_vcfs(self):
        all_vcfs = self.gcp_functions.list_bucket_contents(
            bucket_name="fc-secure-50f2310f-659c-4c60-af42-e028b58bad52",
            prefix="eMERGEseq/single_sample_clinical_vcfs/",
            file_extensions_to_include=[".vcf.gz"],
            file_name_only=True,
        )
        return [z["path"] for z in all_vcfs]

    def get_mapping_contents(self):
        mapping_path = "gs://fc-secure-50f2310f-659c-4c60-af42-e028b58bad52/Phenotypic_Data/eMERGEseq_SubjectConsent_DS_20201020.csv"
        file_contents = self.gcp_functions.read_file(cloud_path=mapping_path)
        csv_text = file_contents.lstrip("\ufeff")
        reader = csv.DictReader(StringIO(csv_text))
        return list(reader)

    def consent_to_sub_workspace_mapping(self):
        consent_code_mapping = []
        consent_to_workspace_mapping_path = "gs://fc-d9c13768-d5f6-4a14-8388-9819c58d7117/emerge_consent_to_sub_workspace_mapping.csv"
        file_contents = self.gcp_functions.read_file(cloud_path=consent_to_workspace_mapping_path)
        csv_text = file_contents.lstrip("\ufeff")
        reader = csv.DictReader(StringIO(csv_text))
        for row in reader:
            workspace_name = row["AnVIL Dataset Name"]
            if workspace_name.startswith(SOURCE_WORKSPACE_NAME):
                # Get each workspace's destination bucket here
                destination_bucket = TerraWorkspace(
                    billing_project=BILLING_PROJECT,
                    workspace_name=workspace_name,
                    request_util=self.request_util,
                ).get_workspace_bucket()
                row["destination_bucket"] = destination_bucket
                consent_code_mapping.append(row)
        return consent_code_mapping

    def verify_all_samples_ids_from_files_are_in_mapping(self):
        bam_file_paths = self.list_all_bams()
        vcf_file_paths = self.list_all_vcfs()

        # Get the mapping of sample -> consent code
        mapping_contents = self.get_mapping_contents()

        # Get all sample names that exist in the SampleSubjectMapping file
        sample_names_from_mapping = [x["subject_id"] for x in mapping_contents]

        files_with_ids_not_found_in_mapping = []
        sample_names_found_in_mapping = []

        # Map each sample from the existing bam paths to its consent group
        for bam in bam_file_paths:
            sample_name = Path(bam).stem

            if sample_name not in sample_names_from_mapping:
                files_with_ids_not_found_in_mapping.append(bam)
            else:
                sample_names_found_in_mapping.append(
                    {
                        "sample_id": sample_name,
                        "consent_group": f'c{[x["consent"] for x in mapping_contents if x["subject_id"] == sample_name][0]}',
                        "source_file": bam

                    }
                )

        # Map each sample from existing vcf paths to its consent group
        for vcf in vcf_file_paths:
            file_name = Path(vcf).stem.replace(".vcf", "")
            sample_name = file_name.split("_")[1]
            if sample_name not in sample_names_from_mapping:
                files_with_ids_not_found_in_mapping.append(vcf)
            else:
                sample_names_found_in_mapping.append(
                    {
                        "sample_id": sample_name,
                        "consent_group": f'c{[x["consent"] for x in mapping_contents if x["subject_id"] == sample_name][0]}',
                        "source_file": vcf

                    }
                )
        filename = "unmapped_files.csv"
        logging.info(
            f"Found {len(files_with_ids_not_found_in_mapping)} samples from bams/vcfs that are not found in the mapping file. Writing them to {filename}")
        with open(filename, "w") as outfile:
            for file_path in files_with_ids_not_found_in_mapping:
                outfile.write(f"{file_path}\n")

        return sample_names_found_in_mapping

    def add_sub_workspace_to_bam_path(self):
        # Get mapping of consent code to sub-workspace (custom code adds the destination bucket to this mapping as well)
        consent_to_sub_workspace_mapping = self.consent_to_sub_workspace_mapping()
        file_mapping = self.verify_all_samples_ids_from_files_are_in_mapping()

        for file in file_mapping:
            consent_group = file["consent_group"]
            # Find the destination workspace information based on the sample's consent group
            destination_workspace_info = [
                x for x in consent_to_sub_workspace_mapping if x["Consent Code"] == consent_group]
            if len(destination_workspace_info) != 1:
                raise ValueError(f"Consent group {consent_group} either not found, or mapped to more than one workspace")

            destination_workspace = destination_workspace_info[0]["AnVIL Dataset Name"]
            destination_bucket = destination_workspace_info[0]["destination_bucket"]

            # Add the destination workspace and bucket to the file mapping for each sample
            file["destination_workspace"] = destination_workspace
            file["destination_bucket"] = destination_bucket

            # Replace the original bucket from the source path to instead be the destination bucket to construct the destination path
            source_file = file["source_file"]
            full_destination_path = source_file.replace(
                "fc-secure-50f2310f-659c-4c60-af42-e028b58bad52", destination_bucket)
            file["full_destination_path"] = full_destination_path

        return file_mapping


if __name__ == '__main__':
    gcp_functions_obj = GCPCloudFunctions()
    token = Token()
    run_request = RunRequest(token=token)
    full_file_mapping = ProcessFilesGetMapping(
        gcp_cloud_functions=gcp_functions_obj,
        request_util=run_request,
    ).add_sub_workspace_to_bam_path()
    logging.info("Extracted all file mapping")

    # Save mapping information
    output_file_name = f"{SOURCE_WORKSPACE_NAME}_source_bam_vcf_to_destination_mapping.csv"
    logging.info(f"Writing mapping information to {output_file_name}")
    fieldnames = [x.keys() for x in full_file_mapping][0]
    with open(output_file_name, "w") as mapping_file:
        writer = csv.DictWriter(mapping_file, fieldnames=fieldnames, delimiter=",")
        writer.writeheader()
        writer.writerows(full_file_mapping)

    # Extract only what we need to map the source to destination paths
    keys_to_keep = ["source_file", "full_destination_path"]
    mapping_for_move_operation = [{k: d[k] for k in keys_to_keep} for d in full_file_mapping]

    # Uncomment to actually move files
    gcp_functions_obj.move_or_copy_multiple_files(files_to_move=mapping_for_move_operation, action=MOVE)
