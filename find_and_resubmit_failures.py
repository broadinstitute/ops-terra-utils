import csv
import json
import re
import warnings
from google.cloud import storage
from urllib.parse import urlparse


warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials from Google Cloud SDK without a quota project",
    category=UserWarning,
)

def extract_last_stargazer_target(stderr_path: str) -> str:
    pattern = r"-t\s+(\S+)"
    matches = []

    parsed = urlparse(stderr_path)
    bucket_name = parsed.netloc
    blob_path = parsed.path.lstrip("/")

    # Read file content from GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()

    # Search for all lines with `Command line: stargazer`
    for line in content.splitlines():
        if "Command line: stargazer" in line:
            match = re.search(pattern, line)
            if match:
                matches.append(match.group(1))

    # Return the last found -t argument (if any)
    if matches:
        return matches[-1]
    else:
        raise ValueError(f"No '-t' argument found in {stderr_path}")


if __name__ == '__main__':

    file_path = "/Users/sahakian/Documents/workflow_metadata.tsv"

    workflow_metadata = []
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for row in reader:
            workflow_metadata.append(row)

    samples_to_submit = []
    for workflow in workflow_metadata:
        workflow_id = workflow["workflowId"]
        submission_id = workflow["submissionId"]
        if submission_id == "f334caa8-60d8-4c94-a2a2-0243cc2d892a":
            stderr_path = f"gs://fc-secure-a2623d71-a8e7-44cf-a2ed-fb02088bb356/submissions/{submission_id}/StargazerFromJointVCF/{workflow_id}/call-RunStargazer/cacheCopy/stderr"
        else:
            stderr_path = f"gs://fc-secure-a2623d71-a8e7-44cf-a2ed-fb02088bb356/submissions/{submission_id}/StargazerFromJointVCF/{workflow_id}/call-RunStargazer/stderr"
        gene_to_remove = extract_last_stargazer_target(stderr_path)
        full_gene_list = ["2c_cluster","abcg2","cacna1s","cftr","cyp2b6","cyp2c19","cyp2c9","cyp3a4","cyp3a5","cyp4f2","g6pd","ifnl3","nat2","nudt15","ryr1","slco1b1","tpmt","ugt1a1","vkorc1"]
        edited_gene_list = [item for item in full_gene_list if item != gene_to_remove]
        samples_to_submit.append(
            {
                "workflow_entity": workflow["workflowEntity"],
                "sample_id": json.loads(workflow["workflowEntity"])["entityName"],
                "gene_to_remove": gene_to_remove,
                "edited_gene_list": edited_gene_list,
            }
        )

    with open("samples_to_resubmit.tsv", "w") as out_file:
        writer = csv.DictWriter(out_file, delimiter="\t", fieldnames=["workflow_entity", "sample_id", "gene_to_remove", "edited_gene_list"])
        writer.writeheader()
        writer.writerows(samples_to_submit)