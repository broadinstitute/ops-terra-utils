"""
Set up steps:

1. gcloud config set project broad-duos-dev
2. Get Tessera credentials: gcloud container clusters get-credentials tessera --region us-central1
3. Port forward: kubectl port-forward svc/tessera-api 8080:8080 -n tessera
4. When creating datasets, we have two options:
    a. Can either use the Tessera managed bucket: broad-duos-dev-tessera-datasets
    b. Can try to use a non Tessera managed GCP bucket, by giving read and write access to tessera-sa@broad-duos-dev.iam.gserviceaccount.com


Test/dev script for iterating on a DataIngest ingestion pipeline.

Pipeline steps (see `STEPS` in `__main__`):
  1. ListSourceFiles - find all files at the given source location(s)
  2. CreateDataset   - create a dataset backed by a storage bucket (no path/prefix)
  3. IngestFiles     - register the files found in step 1 into the dataset
  4. ReportResults   - list the files now in the dataset and report that alongside
                       the originally expected files and basic dataset info

Steps are plain classes with a `run(ctx)` method sharing an `IngestContext`, so
adding, removing, or reordering steps going forward is just editing the list
passed to `IngestPipeline` below. File discovery goes through
`GCPCloudFunctions`; everything else (datasets, ingestion, jobs) goes through
`DataIngest`.
"""
import argparse
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.ingest_util import DataIngest

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def _split_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Split a `gs://bucket/prefix` URI into `(bucket, prefix)`."""
    without_scheme = gs_uri.removeprefix("gs://")
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix


@dataclass
class IngestContext:
    """State shared between pipeline steps."""
    dataset_name: str
    storage_bucket: str
    source_locations: list[str]
    extensions_to_include: list[str] = field(default_factory=list)
    extensions_to_exclude: list[str] = field(default_factory=list)

    gcp: GCPCloudFunctions = field(default_factory=GCPCloudFunctions)
    ingest: DataIngest = field(default_factory=DataIngest)

    expected_files: list[str] = field(default_factory=list)
    dataset_id: Optional[str] = None


class IngestStep:
    """Base class for a single step in the ingest pipeline."""

    name = "step"

    def run(self, ctx: IngestContext) -> None:
        raise NotImplementedError


class ListSourceFiles(IngestStep):
    """Find all files to ingest.

    Entries in `ctx.source_locations` ending in `/` are treated as bucket/prefix
    directories and listed via `GCPCloudFunctions`. Any other entry is treated as
    a full file path and used as-is.
    """

    name = "list_source_files"

    def run(self, ctx: IngestContext) -> None:
        expected_files = []
        for location in ctx.source_locations:
            if location.endswith("/"):
                bucket, prefix = _split_gs_uri(location)
                found = ctx.gcp.list_bucket_contents(
                    bucket_name=bucket,
                    prefix=prefix,
                    file_extensions_to_include=ctx.extensions_to_include,
                    file_extensions_to_ignore=ctx.extensions_to_exclude,
                    file_name_only=True,
                )
                expected_files.extend(file_info["path"] for file_info in found)
            else:
                expected_files.append(location)
        ctx.expected_files = expected_files
        logging.info(f"Found {len(expected_files)} expected file(s) to ingest")


class CreateDataset(IngestStep):
    """Create the dataset, backed by the storage bucket with no path/prefix set."""

    name = "create_dataset"

    def run(self, ctx: IngestContext) -> None:
        bucket_name = ctx.storage_bucket.removeprefix("gs://").strip("/")
        job = ctx.ingest.create_dataset(
            name=ctx.dataset_name,
            storage={"cloud": "GCP", "bucket": bucket_name},
        ).json()
        result = ctx.ingest.wait_for_job(job["jobId"])
        ctx.dataset_id = result["id"]
        logging.info(f"Created dataset '{ctx.dataset_name}' ({ctx.dataset_id})")


class IngestFiles(IngestStep):
    """Register all files found by `ListSourceFiles` into the dataset."""

    name = "ingest_files"

    def run(self, ctx: IngestContext) -> None:
        if not ctx.expected_files:
            logging.warning("No files found to ingest, skipping")
            return
        job = ctx.ingest.register_files(
            dataset_id=ctx.dataset_id,
            files=[{"uri": path} for path in ctx.expected_files],
        ).json()
        ctx.ingest.wait_for_job(job["jobId"])
        logging.info(f"Registered {len(ctx.expected_files)} file(s) into dataset {ctx.dataset_id}")


class ReportResults(IngestStep):
    """List the files now in the dataset and report that alongside the
    originally expected files and basic dataset information.
    """

    name = "report_results"

    def run(self, ctx: IngestContext) -> None:
        dataset = ctx.ingest.get_dataset(ctx.dataset_id).json()
        registered_files = ctx.ingest.list_files(ctx.dataset_id).json()

        report = {
            "dataset": dataset,
            "expected_files": ctx.expected_files,
            "registered_files": registered_files,
        }
        print(json.dumps(report, sort_keys=True, indent=4, default=str))


class IngestPipeline:
    """Runs an ordered list of `IngestStep`s against a shared `IngestContext`.

    Add, remove, or reorder steps by editing the list passed in at the call site.
    """

    def __init__(self, steps: list[IngestStep]):
        self.steps = steps

    def run(self, ctx: IngestContext) -> IngestContext:
        for step in self.steps:
            logging.info(f"Running step: {step.name}")
            step.run(ctx)
        return ctx


def get_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Test ingestion pipeline")
    parser.add_argument(
        "--source-location", "-s", required=True, nargs="+",
        help="One or more gs:// locations containing the files to ingest. Locations ending in "
             "'/' are listed as bucket/prefix directories; anything else is treated as a "
             "single file path.",
    )
    parser.add_argument("--storage-bucket", "-b", required=True, help="gs:// bucket backing the new dataset")
    parser.add_argument("--dataset-name", "-n", required=True, help="Name for the new dataset")
    parser.add_argument(
        "--include-extension", "-i", nargs="+", default=[],
        help="Only include files with one of these extensions",
    )
    parser.add_argument(
        "--exclude-extension", "-e", nargs="+", default=[],
        help="Exclude files with one of these extensions",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    ctx = IngestContext(
        dataset_name=args.dataset_name,
        storage_bucket=args.storage_bucket,
        source_locations=args.source_location,
        extensions_to_include=args.include_extension,
        extensions_to_exclude=args.exclude_extension,
    )

    IngestPipeline([
        ListSourceFiles(),
        CreateDataset(),
        IngestFiles(),
        ReportResults(),
    ]).run(ctx)
