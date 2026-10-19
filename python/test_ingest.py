"""
Set up steps:

1. gcloud config set project broad-duos-dev
2. Get Tessera credentials: gcloud container clusters get-credentials tessera --region us-central1
3. Port forward: kubectl port-forward svc/tessera-api 8080:8080 -n tessera
4. When creating datasets, we have two options:
    a. Can either use the Tessera managed bucket: broad-duos-dev-tessera-datasets
    b. Can try to use a non Tessera managed GCP bucket, by giving read and write access to tessera-sa@broad-duos-dev.iam.gserviceaccount.com


Test/dev script for iterating on a DataIngest ingestion pipeline.

Pipeline steps (see the list passed to `IngestPipeline` in `__main__`):
  1. ListSourceFiles      - find all files at the given source location(s)
  2. CreateDataset        - create a dataset backed by a storage bucket (no path/prefix)
  3. IngestFiles          - register the files found in step 1 into the dataset
  4. CreateFileInfoTable  - create a schema version adding a `file_info` table
  5. IngestFileInfoTable  - build a manifest of file metadata and ingest it into `file_info`
  6. CreateSnapshot       - snapshot everything ingested into the dataset so far
  7. VerifySnapshot       - verify the snapshot was created successfully
  8. ReportResults        - report on the dataset, its files, tables, and snapshot,
                            alongside the originally expected files

Steps are plain classes with a `run(ctx)` method sharing an `IngestContext`, so
adding, removing, or reordering steps going forward is just editing the list
passed to `IngestPipeline` below. File discovery (and file metadata lookups) go
through `GCPCloudFunctions`; everything else (datasets, schemas, ingestion,
snapshots, jobs) goes through `DataIngest`.
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
    schema_id: Optional[str] = None
    snapshot_id: Optional[str] = None
    snapshot: Optional[dict] = None


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


class CreateFileInfoTable(IngestStep):
    """Create a schema version that adds a `file_info` table to the dataset."""

    name = "create_file_info_table"

    FILE_INFO_DDL = (
        "CREATE TABLE file_info ("
        "file_path TEXT, "
        "file_name TEXT, "
        "size_in_bytes BIGINT, "
        "md5_hash TEXT"
        ")"
    )

    def run(self, ctx: IngestContext) -> None:
        schema = ctx.ingest.create_schema_version(dataset_id=ctx.dataset_id, ddl=self.FILE_INFO_DDL).json()
        ctx.schema_id = schema["id"]
        logging.info(f"Created schema version {ctx.schema_id} with table 'file_info'")


class IngestFileInfoTable(IngestStep):
    """Build a manifest of file metadata (path, name, size, md5) for every
    file found by `ListSourceFiles` and ingest it into the `file_info` table.
    """

    name = "ingest_file_info_table"
    MANIFEST_FILE_NAME = "_file_info_manifest.json"

    def run(self, ctx: IngestContext) -> None:
        if not ctx.expected_files:
            logging.warning("No files to build a file_info manifest from, skipping")
            return

        file_metadata = ctx.gcp.load_blobs_from_full_paths_multithreaded(ctx.expected_files)
        rows = [
            {
                "file_path": info["path"],
                "file_name": info["name"],
                "size_in_bytes": info["size_in_bytes"],
                "md5_hash": info["md5_hash"],
            }
            for info in file_metadata
        ]

        bucket_name = ctx.storage_bucket.removeprefix("gs://").strip("/")
        manifest_uri = f"gs://{bucket_name}/{self.MANIFEST_FILE_NAME}"
        # ingest_tabular_data's JSON format expects newline-delimited JSON, not a JSON array.
        manifest_contents = "\n".join(json.dumps(row) for row in rows)
        ctx.gcp.write_to_gcp_file(manifest_uri, manifest_contents)

        job = ctx.ingest.ingest_tabular_data(
            dataset_id=ctx.dataset_id,
            sources=[{
                "uri": manifest_uri,
                "targetTable": "file_info",
                "format": "JSON",
                "schema": {"id": ctx.schema_id},
            }],
        ).json()
        ctx.ingest.wait_for_job(job["jobId"])
        logging.info(f"Ingested {len(rows)} row(s) into table 'file_info'")


class CreateSnapshot(IngestStep):
    """Create a snapshot of everything ingested into the dataset so far."""

    name = "create_snapshot"

    def run(self, ctx: IngestContext) -> None:
        snapshot_name = f"{ctx.dataset_name}_snapshot"
        job = ctx.ingest.create_snapshot(
            dataset_id=ctx.dataset_id,
            name=snapshot_name,
            schema_id=ctx.schema_id,
        ).json()
        result = ctx.ingest.wait_for_job(job["jobId"])
        ctx.snapshot_id = result["id"]
        logging.info(f"Created snapshot '{snapshot_name}' ({ctx.snapshot_id})")


class VerifySnapshot(IngestStep):
    """Verify the snapshot was created successfully and lists everything expected."""

    name = "verify_snapshot"

    def run(self, ctx: IngestContext) -> None:
        snapshot = ctx.ingest.get_snapshot(ctx.snapshot_id).json()
        if snapshot.get("id") != ctx.snapshot_id:
            raise Exception(f"Snapshot {ctx.snapshot_id} could not be verified: {snapshot}")
        ctx.snapshot = snapshot
        logging.info(f"Verified snapshot {ctx.snapshot_id} was created successfully")


class ReportResults(IngestStep):
    """Report on the dataset, its files, tables, and snapshot, alongside the
    originally expected files.
    """

    name = "report_results"

    def run(self, ctx: IngestContext) -> None:
        dataset = ctx.ingest.get_dataset(ctx.dataset_id).json()
        registered_files = ctx.ingest.list_files(ctx.dataset_id).json()
        tables = ctx.ingest.list_schema_versions(ctx.dataset_id).json()
        snapshot = ctx.snapshot if ctx.snapshot is not None else (
            ctx.ingest.get_snapshot(ctx.snapshot_id).json() if ctx.snapshot_id else None
        )

        report = {
            "dataset": dataset,
            "expected_files": ctx.expected_files,
            "registered_files": registered_files,
            "tables": tables,
            "snapshot": snapshot,
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
        CreateFileInfoTable(),
        IngestFileInfoTable(),
        CreateSnapshot(),
        VerifySnapshot(),
        ReportResults(),
    ]).run(ctx)
