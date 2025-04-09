import json
import logging
import time
from typing import Any, Callable, Optional


class MonitorTDRJob:
    """
    A class to monitor the status of a TDR job until completion.

    Attributes:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
    """

    def __init__(self, tdr: Any, job_id: str, check_interval: int, return_json: bool):
        """
        Initialize the MonitorTDRJob class.

        Args:
            tdr (TDR): An instance of the TDR class.
            job_id (str): The ID of the job to be monitored.
            check_interval (int): The interval in seconds to wait between status checks.
            return_json (bool): Whether to get and return the result of the job as json.
        """
        self.tdr = tdr
        self.job_id = job_id
        self.check_interval = check_interval
        self.return_json = return_json

    def _raise_for_failed_job(self) -> None:
        """
        Raise an error with useful information if the job has failed.

        Raises:
            ValueError: If the job has failed.
        """
        job_result = self.tdr.get_job_result(self.job_id, expect_failure=True)
        raise Exception(
            f"Status code {job_result.status_code}: {job_result.text}")

    def run(self) -> Optional[dict]:
        """
        Monitor the job until completion.

        Returns:
            dict: The result of the job.
        """
        while True:
            ingest_response = self.tdr.get_job_status(self.job_id)
            if ingest_response.status_code == 202:
                logging.info(f"TDR job {self.job_id} is still running")
                # Check every x seconds if ingest is still running
                time.sleep(self.check_interval)
            elif ingest_response.status_code == 200:
                response_json = json.loads(ingest_response.text)
                if response_json["job_status"] == "succeeded":
                    logging.info(f"TDR job {self.job_id} succeeded")
                    if self.return_json:
                        request = self.tdr.get_job_result(self.job_id)
                        return json.loads(request.text)
                    # If not returning json, return None
                    return None
                else:
                    logging.error(f"TDR job {self.job_id} failed")
                    self._raise_for_failed_job()
            else:
                logging.error(f"TDR job {self.job_id} failed")
                self._raise_for_failed_job()


class SubmitAndMonitorMultipleJobs:
    def __init__(
            self, tdr: Any,
            job_function: Callable,
            job_args_list: list[tuple],
            batch_size: int = 100,
            check_interval: int = 5,
            verbose: bool = False
    ):
        """
        Initialize the SubmitAndMonitorMultipleJobs class.

        Args:
            tdr (Any): An instance of the TDR class.
            job_function (Callable): The function to submit a job.
            job_args_list (list[tuple]): A list of tuples containing the arguments for each job.
            batch_size (int, optional): The number of jobs to process in each batch. Defaults to 100.
            check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 5.
            verbose (bool, optional): Whether to log detailed information about each job. Defaults to False.
        """
        self.tdr = tdr
        self.job_function = job_function
        self.job_args_list = job_args_list
        self.batch_size = batch_size
        self.check_interval = check_interval
        self.verbose = verbose

    def run(self) -> None:
        """
        Run the process to submit and monitor multiple jobs in batches.

        Logs the progress and status of each batch and job.

        Returns:
            None
        """
        total_jobs = len(self.job_args_list)
        logging.info(f"Processing {total_jobs} {self.job_function.__name__} jobs in batches of {self.batch_size}")

        # Process jobs in batches
        for i in range(0, total_jobs, self.batch_size):
            job_ids = []
            current_batch = self.job_args_list[i:i + self.batch_size]
            logging.info(
                f"Submitting jobs for batch {i // self.batch_size + 1} with {len(current_batch)} jobs."
            )

            # Submit jobs for the current batch
            for job_args in current_batch:
                # Submit job with arguments and store the job ID
                job_id = self.job_function(*job_args)
                if self.verbose:
                    logging.info(f"Submitted job {job_id} with args {job_args}")
                job_ids.append(job_id)

            # Monitor jobs for the current batch
            logging.info(f"Monitoring {len(current_batch)} jobs in batch {i // self.batch_size + 1}")
            for job_id in job_ids:
                MonitorTDRJob(
                    tdr=self.tdr,
                    job_id=job_id,
                    check_interval=self.check_interval,
                    return_json=False
                ).run()

            logging.info(f"Completed batch {i // self.batch_size + 1} with {len(current_batch)} jobs.")

        logging.info(f"Successfully processed {total_jobs} jobs.")
