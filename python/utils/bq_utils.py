from google.cloud import bigquery
from google.auth import default
import logging
from typing import Any


class BigQueryUtil:
    def __init__(self):
        """
        Initialize the BigQuery utility with user authentication.
        """
        self.client = bigquery.Client()

    def upload_data_to_table(self, table_id: str, rows: list[dict]):
        """
        Uploads data directly from a list of dictionaries to a BigQuery table.

        Args:
            table_id (str): BigQuery table ID in the format 'project.dataset.table'.
            rows (list[dict]): List of dictionaries, where each dictionary represents a row of data.
        """
        # Get the BigQuery table reference
        destination_table = self.client.get_table(table_id)
        previous_rows = destination_table.num_rows
        logging.info(f"Currently {previous_rows} rows in {table_id} before upload")

        # Insert rows from the list of dictionaries
        errors = self.client.insert_rows_json(table_id, rows)

        if errors:
            logging.error(f"Encountered errors while inserting rows: {errors}")
        else:
            logging.info(f"Successfully inserted {len(rows)} rows into {table_id}")

        # Get new row count for confirmation
        destination_table = self.client.get_table(table_id)
        new_rows = destination_table.num_rows
        logging.info(f"Table now contains {new_rows} rows after upload")

    def query_table(self, query: str, to_dataframe: bool = False) -> Any:
        """
        Executes a SQL query on a BigQuery table and returns the results .

        Args:
            query (str): SQL query to execute.
            to_dataframe (bool): If True, returns the query results as a Pandas DataFrame. Default is False.

        Returns:
            list[dict]: List of dictionaries, where each dictionary represents a row of query results.
        """
        query_job = self.client.query(query)
        if to_dataframe:
            return query_job.result().to_dataframe()
        return [row for row in query_job.result()]
