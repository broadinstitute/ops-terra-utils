from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
import logging
from typing import Any


class BigQueryUtil:
    def __init__(self, project_id: str):
        """
        Initialize the BigQuery utility with user authentication.
        """
        self.project_id = project_id
        if project_id:
            self.client = bigquery.Client(project=project_id)
        else:
            self.client = bigquery.Client()

    def upload_data_to_table(self, table_id: str, rows: list[dict]) -> None:
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

    def check_permissions_to_project(self, raise_on_other_failure: bool = True) -> bool:
        """
        Checks if the user has permission to access the project.

        Args:
            raise_on_other_failure (bool): If True, raises an error if an unexpected error occurs. Default is True.

        Returns:
            bool: True if the user has permissions, False if a 403 Forbidden error is encountered.
        """
        return self._check_permissions("SELECT 1", raise_on_other_failure)

    def check_permissions_for_query(self, query: str, raise_on_other_failure: bool = True) -> bool:
        """
        Checks if the user has permission to run a specific query.

        Args:
            query (str): SQL query to execute.
            raise_on_other_failure (bool): If True, raises an error if an unexpected error occurs. Default is True.

        Returns:
            bool: True if the user has permissions, False if a 403 Forbidden error is encountered.
        """
        return self._check_permissions(query, raise_on_other_failure)

    def _check_permissions(self, qry: str, raise_on_other_failure: bool = True) -> bool:
        """
        Checks if the user has permission to run queries and access the project.

        Args:
            raise_on_other_failure (bool): If True, raises an error if an unexpected error occurs. Default is True.

        Returns:
            bool: True if the user has permissions, False if a 403 Forbidden error is encountered.
        """
        try:
            # A simple query that should succeed if the user has permissions
            query = qry
            self.client.query(query).result()  # Run a lightweight query
            return True
        except Forbidden:
            logging.warning("403 Permission Denied")
            return False
        except Exception as e:
            logging.error(f"Unexpected error when trying to check permissions for project {self.project_id}. {e}")
            if raise_on_other_failure:
                logging.error("Raising error because raise_on_other_failure is set to True")
                raise e
            else:
                logging.error("Continuing execution because raise_on_other_failure is set to False.")
                return False
