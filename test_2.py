import csv
import re


if __name__ == '__main__':
    source_workspace_metadata = "/Users/sahakian/Documents/source_workspace_metadata.tsv"
    destination_workspace_metadata = "/Users/sahakian/Documents/destination_workspace_metadata.tsv"
    source_bucket = "fc-f278dd4b-2d51-43f9-a991-15593eefac01"
    destination_bucket = "fc-b9eb30eb-7a88-4f06-9bbc-e182a034bdcb"

    source_metadata = []
    with open(source_workspace_metadata, "r") as source:
        reader = csv.DictReader(source, delimiter="\t")
        for row in reader:
            source_metadata.append(row)


    destination_metadata = []
    for sample in source_metadata:
        destination_metadata.append(
            {
                "entity:sample_id": sample["entity:sample_id"],
                "bam_file": re.sub(source_bucket, destination_bucket, sample["bam_file"]),
                "bam_index_file": re.sub(source_bucket, destination_bucket, sample["bam_index_file"]),
                "cram_file": re.sub(source_bucket, destination_bucket, sample["cram_file"]),
                "cram_index_file": re.sub(source_bucket, destination_bucket, sample["cram_index_file"]),
                "cram_md5_file": re.sub(source_bucket, destination_bucket, sample["cram_md5_file"]),
            }
        )
    headers = [f.keys() for f in destination_metadata][0]
    with open(destination_workspace_metadata, "w") as destination:
        writer = csv.DictWriter(destination, delimiter="\t", fieldnames=headers)
        writer.writeheader()
        writer.writerows(destination_metadata)
