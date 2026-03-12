import csv
import logging
from argparse import Namespace, ArgumentParser

from ops_utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--original_gvcf_mapping", type=str, required=True)
    parser.add_argument("--output_sample_map", type=str, required=True)

    return parser.parse_args()


class CopyGvcfsAndCreateSampleMap:
    def __init__(self, gcp_cloud_functions_obj: GCPCloudFunctions, original_gvcf_mapping: str, output_sample_map: str) -> None:
        self.gcp_cloud_functions_obj = gcp_cloud_functions_obj
        self.original_gvcf_mapping = original_gvcf_mapping
        self.output_sample_map = output_sample_map

    def get_mapping_file_contents(self) -> list[dict]:
        with open(self.original_gvcf_mapping, "r") as original_gvcf_mapping:
            reader = csv.DictReader(original_gvcf_mapping, delimiter="\t")
            return list(reader)

    def copy_gvcfs_and_index_to_new_extension(self, gvcf_to_sample_mapping: list[dict]) -> list[dict]:
        new_gvcf_metadata = []
        for gvcf_and_sample in gvcf_to_sample_mapping:
            sample_alias = gvcf_and_sample["sample_name"]
            original_gvcf_path = gvcf_and_sample["gvcf_file_path"]
            # Check that the original gvcf path ends with .gvcf.gz
            if not original_gvcf_path.endswith(".gvcf.gz"):
                logging.error(
                    f"Stopping processing - {original_gvcf_path} does not end with .gvcf.gz. All input gvcfs must end with this extension")
                exit()

            # Copy gvcf paths to new extensions
            new_gvcf_path = original_gvcf_path.replace(".gvcf.gz", ".g.vcf.gz")
            logging.info(f"Copying {original_gvcf_path} to {new_gvcf_path}")
            self.gcp_cloud_functions_obj.copy_cloud_file(
                src_cloud_path=original_gvcf_path, full_destination_path=new_gvcf_path)

            # Copy index paths to new extensions
            original_index_path = f"{original_gvcf_path}.tbi"
            new_index_path = original_index_path.replace(".gvcf.gz.tbi", ".g.vcf.gz.tbi")
            logging.info(f"Copying {original_index_path} to {new_index_path}")
            self.gcp_cloud_functions_obj.copy_cloud_file(
                src_cloud_path=original_index_path, full_destination_path=new_index_path)

            new_gvcf_metadata.append(
                {
                    "gvcf_path": new_gvcf_path,
                    "sample_alias": sample_alias,
                }
            )
        return new_gvcf_metadata

    def write_new_sample_map(self, new_gvcf_metadata: list[dict]) -> None:
        logging.info(f"Writing new sample map to {self.output_sample_map}")
        with open(self.output_sample_map, "w") as output_sample_map:
            writer = csv.DictWriter(output_sample_map, delimiter="\t", fieldnames=["sample_alias", "gvcf_path"])
            writer.writerows(new_gvcf_metadata)

    def run(self) -> None:
        gvcf_to_sample_mapping = self.get_mapping_file_contents()
        new_gvcf_metadata = self.copy_gvcfs_and_index_to_new_extension(gvcf_to_sample_mapping=gvcf_to_sample_mapping)
        self.write_new_sample_map(new_gvcf_metadata=new_gvcf_metadata)


if __name__ == '__main__':
    args = parse_args()
    gcp_cloud_functions = GCPCloudFunctions()

    CopyGvcfsAndCreateSampleMap(
        gcp_cloud_functions_obj=gcp_cloud_functions,
        original_gvcf_mapping=args.original_gvcf_mapping,
        output_sample_map=args.output_sample_map
    ).run()
