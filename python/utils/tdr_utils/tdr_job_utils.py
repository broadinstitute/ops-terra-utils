import json
import logging
import time

from typing import Any


class MonitorTDRJob:
    """
    A class to monitor the status of a TDR job until completion.

    Attributes:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
    """

    def __init__(self, tdr: Any, job_id: str, check_interval: int):
        """
        Initialize the MonitorTDRJob class.

        Args:
            tdr (TDR): An instance of the TDR class.
            job_id (str): The ID of the job to be monitored.
            check_interval (int): The interval in seconds to wait between status checks.
        """
        self.tdr = tdr
        self.job_id = job_id
        self.check_interval = check_interval

    def run(self) -> bool:
        """
        Monitor the job until completion.

        Returns:
            bool: True if the job succeeded, raises an error otherwise.
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
                    return True
                else:
                    logging.error(f"TDR job {self.job_id} failed")
                    job_result = self.tdr.get_job_result(self.job_id)
                    raise ValueError(
                        f"Status code {ingest_response.status_code}: {response_json}\n{job_result}")
            else:
                logging.error(f"TDR job {self.job_id} failed")
                job_result = self.tdr.get_job_result(self.job_id)
                raise ValueError(
                    f"Status code {ingest_response.status_code}: {ingest_response.text}\n{job_result}")
