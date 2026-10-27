"""
Content-based functional test harness for the data ingest platform.

Unlike endpoint tests, this checks that the objects the platform creates actually match what was put in:
source files -> dataset -> snapshot -> export. Point it at different shapes of data (primary key violations,
orphaned foreign keys, very long/wide tables, ...) and see how the platform handles them.

Set up steps are the same as `test_ingest.py` (gcloud project, Tessera credentials, port-forward to the API).

Inputs:
  --input-location     gs:// directory the platform can READ, laid out as:
                           <input-location>/data_files/...    files to register with the dataset
                           <input-location>/tabular_data/...  <table>.csv | <table>.json | <table>.parquet
                       The base name of each tabular file is the target table; the extension is the format.
                       (.json files must be newline-delimited JSON.)
  --schema-ddl         gs:// path to the schema DDL file for the dataset
  --storage-location   gs:// bucket (optionally with a prefix) the platform can WRITE to
  --cleanup
                       Delete the created snapshots and dataset at the end (default: no cleanup). Without this flag,
                       everything stays behind, and the Request column of the results CSV holds the exact
                       requests used so you can replay them (e.g. re-run an ingest request repeatedly).

Tasks (each is skipped if something it depends on failed):
  1. create_dataset              (failure skips everything)
  2. create_schema               (failure skips tabular ingest and snapshots)
  3. register_files              input file count vs. registered file count
  4. ingest_tabular:<file>       one task per file; input record count vs. ingested record count (when reported)
  5. create_full_snapshot        per-table snapshot record counts vs. source records (failure skips 7 and 8)
  6. create_query_snapshot       `SELECT * FROM <first table>`; that table must match source, all others empty
  7. export_table_csv:<table>    CSV record count vs. snapshot table record count
  8. export_snapshot_pfb         export is created and downloadable
  9. cleanup                     only with --cleanup

Output: a CSV with columns Task, Status, Request, Response, Results/Notes, Duration (s) written incrementally
(so a crash or dropped port-forward still leaves results behind), plus a `<output>.state.json` with the IDs of
everything created. Status is PASS, FAIL (check didn't match expectations), ERROR (a call failed) or SKIPPED.

"""
import argparse
import csv
import io
import json
import logging
import re
import time
import os
import requests
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Callable, Optional

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.ingest_util import DataIngest

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

PASS, FAIL, ERROR, SKIPPED = "PASS", "FAIL", "ERROR", "SKIPPED"
TSV_FIELDS = ["Task", "Status", "Request", "Response", "Results/Notes", "Duration (s)"]
FORMATS = {
    ".csv": "CSV",
    ".json": "JSON",
    ".parquet": "PARQUET"
}


def _split_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Split a `gs://bucket/prefix` URI into `(bucket, prefix)`."""
    bucket, _, prefix = gs_uri.removeprefix("gs://").partition("/")
    return bucket, prefix


def _find_list(payload: Any, keys: tuple[str, ...] = ("files", "items", "data", "results")) -> list:
    """Pull a list out of a response that is either a bare list or a dict wrapping one."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def _find_urls(payload: Any) -> list[str]:
    """Recursively collect every http(s) URL in a response (signed URLs are nested differently per endpoint)."""
    if isinstance(payload, str):
        return [payload] if payload.startswith(("http://", "https://")) else []
    if isinstance(payload, dict):
        return [url for value in payload.values() for url in _find_urls(value)]
    if isinstance(payload, list):
        return [url for value in payload for url in _find_urls(value)]
    return []


def _find_count(payload: Any) -> Optional[int]:
    """Best-effort search of a job result for a reported record count."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, int) and not isinstance(value, bool) and re.search(
                r"(record|row).*(count|ingested|loaded)|(count|ingested|loaded).*(record|row)|^(records|rows)$",
                key, re.IGNORECASE
            ):
                return value
        for value in payload.values():
            found = _find_count(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        counts = [_find_count(item) for item in payload]
        if counts and all(count is not None for count in counts):
            return sum(counts)  # type: ignore[arg-type]
    return None


def count_records(data: bytes, extension: str) -> int:
    """Count the records in a CSV (minus header), newline-delimited JSON (or JSON array), or Parquet payload."""
    if extension == ".csv":
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig")))
        return max(sum(1 for _ in reader) - 1, 0)
    if extension == ".json":
        text = data.decode("utf-8").strip()
        if text.startswith("["):
            return len(json.loads(text))
        return sum(1 for line in text.splitlines() if line.strip())
    if extension == ".parquet":
        import pyarrow.parquet as pq  # imported lazily; only needed for parquet inputs/snapshot partitions
        return pq.ParquetFile(io.BytesIO(data)).metadata.num_rows
    raise ValueError(f"Unsupported file extension '{extension}'")


def parse_tables(ddl: str) -> list[str]:
    """Table names from `CREATE TABLE` statements in the DDL, in declaration order."""
    pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:"[^"]+"|[\w$]+)(?:\.(?:"[^"]+"|[\w$]+))*)'
    return [m.group(1).split(".")[-1].strip('"') for m in re.finditer(pattern, ddl, re.IGNORECASE)]


class RecordingRequestUtil:
    """Wraps the DataIngest request util so every call (request AND response) can be reported per task."""

    def __init__(self, inner: Any):
        self._inner = inner
        self.calls: list[dict] = []

    def run_request(self, *args: Any, **kwargs: Any) -> Any:
        entry = {
            "method": kwargs.get("method"),
            "uri": kwargs.get("uri"),
            "params": kwargs.get("params"),
            "body": kwargs.get("data"),
        }
        self.calls.append(entry)
        try:
            response = self._inner.run_request(*args, **kwargs)
        except Exception as e:
            entry["error"] = str(e)
            raise
        entry["status_code"] = getattr(response, "status_code", None)
        entry["response"] = getattr(response, "text", None)
        return response

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def drain(self) -> list[dict]:
        calls, self.calls = self.calls, []
        return calls


@dataclass
class TaskResult:
    task: str
    status: str
    request: str = ""
    response: str = ""
    notes: str = ""
    duration: float = 0.0


@dataclass
class TestContext:
    storage_location: str
    input_location: str
    schema_ddl_path: str
    cleanup: bool
    poll_interval: int
    validate_foreign_keys: bool
    run_name: str

    gcp: GCPCloudFunctions = field(default_factory=GCPCloudFunctions)
    ingest: DataIngest = field(default_factory=DataIngest)

    ddl: Optional[str] = None
    tables: list[str] = field(default_factory=list)
    dataset_id: Optional[str] = None
    schema_id: Optional[str] = None
    full_snapshot_id: Optional[str] = None
    query_snapshot_id: Optional[str] = None
    # table name -> total records across its source files
    source_counts: dict[str, int] = field(default_factory=dict)
    # table name -> total records the platform reported ingesting (only if it reported)
    ingested_counts: dict[str, int] = field(default_factory=dict)
    # snapshot table -> record count in the full snapshot
    full_snapshot_counts: dict[str, int] = field(default_factory=dict)
    # snapshot table -> record count in each table's CSV export
    export_counts: dict[str, int] = field(default_factory=dict)


class Tester:
    """Runs tasks, captures requests/responses, tracks what succeeded, and streams results to a CSV."""

    def __init__(self, ctx: TestContext, output_path: str):
        self.ctx = ctx
        self.output_path = output_path
        self.results: list[TaskResult] = []
        self.recorder = RecordingRequestUtil(ctx.ingest.request_util)
        ctx.ingest.request_util = self.recorder  # type: ignore[assignment]
        with open(output_path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=TSV_FIELDS, delimiter="\t").writeheader()

    # ------------------------------------------------------------------ reporting

    def _record(self, result: TaskResult) -> TaskResult:
        self.results.append(result)
        with open(self.output_path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=TSV_FIELDS, delimiter="\t").writerow(
                {
                    "Task": result.task,
                    "Status": result.status,
                    "Request": json.dumps(result.request),
                    "Response": json.dumps(result.response),
                    "Results/Notes": result.notes,
                    "Duration (s)": f"{result.duration:.1f}",
                }
            )
        logging.info(f"[{result.status}] {result.task}: {result.notes}")
        self._write_state()
        return result

    def _write_state(self) -> None:
        c = self.ctx
        with open(f"{os.path.basename(self.output_path)}.state.json", "w") as f:
            json.dump({
                "run_name": c.run_name, "dataset_id": c.dataset_id, "schema_id": c.schema_id,
                "full_snapshot_id": c.full_snapshot_id, "query_snapshot_id": c.query_snapshot_id,
                "storage_location": c.storage_location, "input_location": c.input_location,
            }, f, indent=4)

    def _summarize_calls(self, calls: list[dict]) -> tuple[str, str]:
        """Request/response columns. Job-status polling is collapsed to just the final poll."""
        is_poll = lambda call: call["method"] == "GET" and "/jobs/" in (call["uri"] or "")  # noqa: E731
        main = [call for call in calls if not is_poll(call)]
        polls = [call for call in calls if is_poll(call)]
        request = [{k: call[k] for k in ("method", "uri", "params", "body") if call.get(k)} for call in main]
        response = [
            {"status_code": call.get("status_code"), "body": call.get("response") or call.get("error")}
            for call in main
        ]
        if polls:
            response.append({"final_job_status": polls[-1].get("response") or polls[-1].get("error")})
        return json.dumps(request, default=str, sort_keys=True), json.dumps(response, default=str, sort_keys=True)

    # ---------------------------------------------------------------------- tasks

    def skip(self, task: str, reason: str) -> TaskResult:
        return self._record(TaskResult(task=task, status=SKIPPED, notes=reason))

    def run(self, task: str, fn: Callable[[], tuple[str, str]]) -> TaskResult:
        """Run `fn`, which does the API calls/comparisons and returns `(PASS|FAIL, notes)`. Any exception is ERROR."""
        self.recorder.drain()
        start = time.time()
        try:
            status, notes = fn()
        except Exception as e:
            status, notes = ERROR, f"{type(e).__name__}: {e}"
        request, response = self._summarize_calls(self.recorder.drain())
        return self._record(TaskResult(
            task=task, status=status, request=request, response=response, notes=notes,
            duration=time.time() - start,
        ))

    def wait(self, response: requests.Response) -> Any:
        return self.ctx.ingest.wait_for_job(response.json()["jobId"], poll_interval=self.ctx.poll_interval)

    @staticmethod
    def compare(label: str, expected: Optional[int], actual: Optional[int]) -> tuple[bool, str]:
        if expected is None or actual is None:
            return True, f"{label}: expected={expected} actual={actual} (not comparable)"
        return expected == actual, f"{label}: expected={expected} actual={actual}"

    # ---------------------------------------------------------------- input layout

    def _list(self, subdir: str) -> list[str]:
        bucket, prefix = _split_gs_uri(self.ctx.input_location.rstrip("/") + f"/{subdir}/")
        found = self.ctx.gcp.list_bucket_contents(bucket_name=bucket, prefix=prefix, file_name_only=True)
        return sorted(item["path"] for item in found if not item["path"].endswith("/"))

    def _download(self, url_or_path: str) -> bytes:
        if url_or_path.startswith("gs://"):
            return self.ctx.gcp.load_blob_from_full_path(url_or_path).download_as_bytes()
        response = requests.get(url_or_path, timeout=600)
        response.raise_for_status()
        return response.content

    # -------------------------------------------------------------------- the flow

    def create_dataset(self) -> bool:
        c = self.ctx
        bucket, prefix = _split_gs_uri(c.storage_location)
        storage = {"cloud": "GCP", "bucket": bucket, **({"prefix": prefix.strip("/") + "/"} if prefix else {})}

        def fn() -> tuple[str, str]:
            c.dataset_id = self.wait(c.ingest.create_dataset(name=c.run_name, storage=storage))["id"]
            return PASS, f"Created dataset '{c.run_name}' ({c.dataset_id}) with storage {storage}"

        return self.run("create_dataset", fn).status == PASS

    def create_schema(self) -> bool:
        c = self.ctx

        def fn() -> tuple[str, str]:
            c.ddl = c.gcp.read_file(c.schema_ddl_path)
            c.tables = parse_tables(c.ddl)
            c.schema_id = c.ingest.create_schema_version(dataset_id=c.dataset_id, ddl=c.ddl).json()["id"]  # type: ignore[arg-type]  # noqa: E501
            if not c.tables:
                return FAIL, f"Schema {c.schema_id} created but no CREATE TABLE statements parsed from the DDL"
            return PASS, f"Created schema {c.schema_id}; tables parsed from DDL: {c.tables}"

        return self.run("create_schema", fn).status == PASS

    def register_files(self) -> None:
        c = self.ctx
        prefix = c.input_location.rstrip("/") + "/data_files/"

        def fn() -> tuple[str, str]:
            paths = self._list("data_files")
            if not paths:
                return FAIL, f"No files found under {prefix}"
            files = [{"uri": p, "path": p.removeprefix(prefix)} for p in paths]
            self.wait(c.ingest.register_files(dataset_id=c.dataset_id, files=files))  # type: ignore[arg-type]
            registered = _find_list(c.ingest.list_files(c.dataset_id).json())  # type: ignore[arg-type]
            ok, notes = self.compare("file count", len(files), len(registered))
            return (PASS if ok else FAIL), notes

        self.run("register_files", fn)

    def ingest_tabular(self) -> None:
        c = self.ctx
        paths = self._list("tabular_data")
        if not paths:
            self.skip("ingest_tabular", f"No files found under {c.input_location.rstrip('/')}/tabular_data/")
            return
        for path in paths:
            self.run(f"ingest_tabular:{PurePosixPath(path).name}", lambda path=path: self._ingest_one(path))

    def _ingest_one(self, path: str) -> tuple[str, str]:
        c = self.ctx
        file = PurePosixPath(path)
        table, extension = file.stem, file.suffix.lower()
        if extension not in FORMATS:
            return ERROR, f"Unsupported extension '{file.suffix}'; expected one of {list(FORMATS)}"
        if table not in c.tables:
            return ERROR, f"Base name '{table}' doesn't match any table in the schema {c.tables}"

        source_count = count_records(self._download(path), extension)
        c.source_counts[table] = c.source_counts.get(table, 0) + source_count
        result = self.wait(c.ingest.ingest_tabular_data(
            dataset_id=c.dataset_id,  # type: ignore[arg-type]
            sources=[{
                "uri": path, "targetTable": table, "format": FORMATS[extension],
                "schema": {"id": c.schema_id, "validateForeignKeys": c.validate_foreign_keys},
            }],
        ))
        ingested = _find_count(result)
        if ingested is not None:
            c.ingested_counts[table] = c.ingested_counts.get(table, 0) + ingested
        ok, notes = self.compare("records (input file vs. reported ingested)", source_count, ingested)
        if ingested is None:
            notes += "; job result did not report a record count, see the snapshot tasks for the verified count"
        return (PASS if ok else FAIL), notes

    def _snapshot_table_count(self, snapshot_id: str, table: str) -> int:
        urls = _find_urls(self.ctx.ingest.get_snapshot_table_signed_urls(snapshot_id, table).json())
        return sum(count_records(self._download(url), ".parquet") for url in urls)

    def create_full_snapshot(self) -> bool:
        c = self.ctx

        def fn() -> tuple[str, str]:
            name = f"{c.run_name}_full"
            c.full_snapshot_id = self.wait(c.ingest.create_snapshot(
                dataset_id=c.dataset_id, name=name, schema_id=c.schema_id  # type: ignore[arg-type]
            ))["id"]
            snapshot_id = c.full_snapshot_id
            problems, lines = [], []
            for table in c.tables:
                actual = self._snapshot_table_count(snapshot_id, table)  # type: ignore[arg-type]
                c.full_snapshot_counts[table] = actual
                ok, line = self.compare(f"{table} (source vs. snapshot)", c.source_counts.get(table, 0), actual)
                lines.append(line)
                if not ok:
                    problems.append(table)
            notes = f"Snapshot {snapshot_id}. " + "; ".join(lines)
            return (FAIL if problems else PASS), notes + (f". MISMATCHED: {problems}" if problems else "")

        return self.run("create_full_snapshot", fn).status != ERROR and c.full_snapshot_id is not None

    def create_query_snapshot(self) -> None:
        c = self.ctx
        first = c.tables[0]

        def fn() -> tuple[str, str]:
            c.query_snapshot_id = self.wait(c.ingest.create_snapshot(
                dataset_id=c.dataset_id, name=f"{c.run_name}_query", schema_id=c.schema_id,  # type: ignore[arg-type]
                queries={first: f"SELECT * FROM {first}"},
            ))["id"]
            problems, lines = [], []
            for table in c.tables:
                expected = c.source_counts.get(table, 0) if table == first else 0
                actual = self._snapshot_table_count(c.query_snapshot_id, table)
                ok, line = self.compare(f"{table} ({'source' if table == first else 'excluded, expected empty'})",
                                        expected, actual)
                lines.append(line)
                if not ok:
                    problems.append(table)
            notes = f"Snapshot {c.query_snapshot_id}. " + "; ".join(lines)
            return (FAIL if problems else PASS), notes + (f". MISMATCHED: {problems}" if problems else "")

        self.run("create_query_snapshot", fn)

    def export_tables(self) -> None:
        c = self.ctx
        for table in c.tables:
            def fn(table: str = table) -> tuple[str, str]:
                self.wait(c.ingest.create_table_export(c.full_snapshot_id, table, "CSV"))  # type: ignore[arg-type]
                urls = _find_urls(c.ingest.get_table_export_signed_url(c.full_snapshot_id, table, "CSV").json())  # type: ignore[arg-type]  # noqa: E501
                if not urls:
                    return FAIL, "Export job succeeded but no signed URL was returned"
                actual = sum(count_records(self._download(url), ".csv") for url in urls)
                c.export_counts[table] = actual
                ok, notes = self.compare(
                    "records (snapshot table vs. CSV export)", c.full_snapshot_counts.get(table), actual
                )
                return (PASS if ok else FAIL), notes

            self.run(f"export_table_csv:{table}", fn)

    def export_snapshot_pfb(self) -> None:
        c = self.ctx

        def fn() -> tuple[str, str]:
            self.wait(c.ingest.create_snapshot_export(c.full_snapshot_id, "PFB"))  # type: ignore[arg-type]
            urls = _find_urls(c.ingest.get_snapshot_export_signed_url(c.full_snapshot_id, "PFB").json())  # type: ignore[arg-type]  # noqa: E501
            if not urls:
                return FAIL, "Export job succeeded but no signed URL was returned"
            with requests.get(urls[0], stream=True, timeout=600) as r:
                size = r.headers.get("Content-Length", "unknown")
                if r.status_code != 200:
                    return FAIL, f"PFB signed URL returned HTTP {r.status_code}"
            return PASS, f"PFB export created and downloadable (HTTP 200, Content-Length={size})"

        self.run("export_snapshot_pfb", fn)

    def cleanup(self) -> None:
        c = self.ctx

        def fn() -> tuple[str, str]:
            notes = []
            for label, snapshot_id in (("query snapshot", c.query_snapshot_id), ("full snapshot", c.full_snapshot_id)):
                if snapshot_id:
                    c.ingest.delete_snapshot(snapshot_id)
                    notes.append(f"deleted {label} {snapshot_id}")
            if c.dataset_id:
                response = c.ingest.delete_dataset(c.dataset_id)
                try:
                    if "jobId" in response.json():
                        self.wait(response)
                except ValueError:
                    pass
                notes.append(f"deleted dataset {c.dataset_id}")
            return PASS, "; ".join(notes) + ". (Registered/source files in the buckets are not deleted.)"

        self.run("cleanup", fn)

    def run_all(self) -> list[TaskResult]:
        c = self.ctx
        if not self.create_dataset():
            for task in ("create_schema", "register_files", "ingest_tabular", "create_full_snapshot",
                         "create_query_snapshot", "export_table_csv", "export_snapshot_pfb"):
                self.skip(task=task, reason="Dataset creation failed")
            return self.results

        schema_ok = self.create_schema()
        self.register_files()
        if schema_ok:
            self.ingest_tabular()
            full_ok = self.create_full_snapshot()
            self.create_query_snapshot()
        else:
            self.skip(task="ingest_tabular", reason="Schema creation failed")
            self.skip(task="create_full_snapshot", reason="Schema creation failed")
            self.skip(task="create_query_snapshot", reason="Schema creation failed")
            full_ok = False

        if full_ok:
            self.export_tables()
            self.export_snapshot_pfb()
        else:
            self.skip(task="export_table_csv", reason="Full snapshot creation failed or was skipped")
            self.skip(task="export_snapshot_pfb", reason="Full snapshot creation failed or was skipped")

        if c.cleanup:
            self.cleanup()
        else:
            logging.info(
                f"Leaving artifacts in place: dataset {c.dataset_id}, schema {c.schema_id}, "
                f"snapshots {c.full_snapshot_id} / {c.query_snapshot_id}. "
                f"Replayable requests are in the Request column of {self.output_path}"
            )
        return self.results


def get_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Content-based functional tests for the data ingest platform")
    parser.add_argument(
        "--input-location",
        required=True,
        help="gs:// directory (readable by the platform) containing 'data_files/' and 'tabular_data/' directories",
    )
    parser.add_argument(
        "--schema-ddl",
        required=True,
        help="gs:// path to the schema DDL file"
    )
    parser.add_argument(
        "--storage-location",
        required=True,
        help="gs://bucket[/prefix] (writable by the platform) to back the new dataset",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete created snapshots and dataset at the end (do not use this flag if you'd like to leave them for investigation)",
    )
    parser.add_argument("--output", default=None, help="Results CSV path (default: ingest_test_results_<run>.csv)")
    parser.add_argument("--poll-interval", type=int, default=10, help="Seconds between job status polls")
    parser.add_argument(
        "--validate-foreign-keys", action="store_true",
        help="Ask the platform to validate foreign keys during tabular ingest (leave off to let orphans through)",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    run_name = f"ingest_functionality_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    context = TestContext(
        storage_location=args.storage_location,
        input_location=args.input_location,
        schema_ddl_path=args.schema_ddl,
        cleanup=args.cleanup,
        poll_interval=args.poll_interval,
        validate_foreign_keys=args.validate_foreign_keys,
        run_name=run_name,
    )
    output_path = args.output or f"ingest_test_results_{run_name}.csv"
    results = Tester(context, output_path).run_all()

    counts = {s: sum(r.status == s for r in results) for s in (PASS, FAIL, ERROR, SKIPPED)}
    logging.info(f"Done: {counts}. Results written to {output_path}")
