from argparse import ArgumentParser, Namespace

from ops_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.token_util import Token
import logging

from python.delete_datset_files_by_id import DeleteDatasetFilesById

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Delete rows from TDR dataset table")
    parser.add_argument("--dataset_id", "-i", required=True)
    parser.add_argument("--table", "-t", required=True)
    parser.add_argument("--ids_to_delete_file", "-if", help="The file containing the ids to delete",
                        required=True)
    parser.add_argument("--id_column_name", "-ic", help="The column name of the id to delete",
                        required=True)
    parser.add_argument("--delete_files", "-df", help="Delete the files associated with the rows",
                        action="store_true")
    parser.add_argument("--service_account_json", "-saj", type=str,
                        help="Path to the service account JSON file. If not provided, will use the default credentials.")
    parser.add_argument("--dry_run", "-n",
                        action="store_true", help="Do not perform deletions; log actions that would be taken.")
    return parser.parse_args()


class GetRowAndFileInfo:
    def __init__(self, ids_to_delete: list[str], id_column_name: str, dataset_id: str, table_name: str, tdr: TDR):
        self.ids_to_delete = ids_to_delete
        self.id_column_name = id_column_name
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.tdr = tdr

    def _fetch_file_ref_columns(self) -> list[str]:
        table_schema = self.tdr.get_table_schema_info(dataset_id=self.dataset_id, table_name=self.table_name)
        return [col['name'] for col in table_schema['columns'] if col['datatype'] == 'fileref']

    def _log_rows_found_info(self, found_row_ids: list[str], file_uuids: set[str]) -> None:
        logging.info(f"Found {len(found_row_ids)} rows to delete")
        not_found_ids = set(self.ids_to_delete) - set(found_row_ids)
        if not_found_ids:
            logging.warning(
                f"Could not find the following {len(not_found_ids)} ids in table {self.table_name}: {not_found_ids}"
            )
        logging.info(f"Found {len(file_uuids)} files linked to the rows to delete")

    def run(self) -> tuple[list[str], set[str]]:
        table_metrics = tdr.get_dataset_table_metrics(dataset_id=dataset_id, target_table_name=table_name)
        # tdr_row_ids to be deleted
        tdr_row_ids = []
        # file uuids to be deleted later if options used
        file_uuids = set()
        # Used to log the ids that were not found
        found_row_ids = []

        # Get the columns whose datatype is filerefs
        file_ref_columns = self._fetch_file_ref_columns()

        for row in table_metrics:
            store = False
            row_file_uuids = []
            for column in row:
                tdr_row_id = row['datarepo_row_id']
                # If the column is a fileref, store the file_uuid
                if column in file_ref_columns:
                    # If the column is a list, store all the file_uuids
                    if isinstance(row[column], list):
                        row_file_uuids.extend(row[column])
                    else:
                        row_file_uuids.append(row[column])
                # If the column is the id column, check if the id is in the ids_to_delete_file
                if column == self.id_column_name:
                    if row[column] in self.ids_to_delete:
                        found_row_ids.append(row[column])
                        store = True
            # If the row is to be deleted, store the file_uuids and tdr_row_id
            if store:
                file_uuids.update(row_file_uuids)
                tdr_row_ids.append(tdr_row_id)
        self._log_rows_found_info(found_row_ids, file_uuids)
        return tdr_row_ids, file_uuids


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    table_name = args.table
    ids_to_delete_file = args.ids_to_delete_file
    id_column_name = args.id_column_name
    delete_files = args.delete_files
    service_account_json = args.service_account_json

    with open(ids_to_delete_file, 'r') as f:
        ids_to_delete = list(set(f.read().splitlines()))
    logging.info(f"Found {len(ids_to_delete)} ids in {ids_to_delete_file} to delete")

    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    # Get the rows to delete and the file_uuids
    tdr_rows_to_delete, file_uuids = GetRowAndFileInfo(
        ids_to_delete=ids_to_delete,
        id_column_name=id_column_name,
        dataset_id=dataset_id,
        table_name=table_name,
        tdr=tdr
    ).run()

    if tdr_rows_to_delete:
        if args.dry_run:
            logging.info(
                f"Dry run: would delete {len(tdr_rows_to_delete)} rows from table {table_name} in dataset {dataset_id}")
        else:
            tdr.soft_delete_entries(dataset_id=dataset_id, table_name=table_name, datarepo_row_ids=tdr_rows_to_delete)
        if delete_files:
            if file_uuids:
                DeleteDatasetFilesById(
                    tdr=tdr,
                    dataset_id=dataset_id,
                    file_id_set=file_uuids,
                    dry_run=args.dry_run
                ).delete_files_and_snapshots()
            else:
                logging.info("No files to delete")
