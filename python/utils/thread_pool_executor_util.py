from concurrent import futures
import logging
from typing import Callable, List, Any


class MultiThreadedJobs:

    def execute_with_retries(
            self,
            function: Callable,
            job_args_list: List[Any],
            max_retries: int
    ) -> bool:
        """Execute a function with retries."""
        retries = 0
        while retries <= max_retries:
            try:
                function(*job_args_list)
                return True
            except Exception as e:
                logging.warning(f"Job failed with error: {e}. Retry {retries + 1}/{max_retries}")
                retries += 1
        return False

    def run_multi_threaded_job_with_no_output(
            self,
            workers: int,
            function: Callable,
            list_of_jobs_args_list: Any,
            max_retries: int = 3,
            fail_on_error: bool = True
    ) -> None:
        """Run jobs in parallel and allow for retries.
           Logs successes and failures, and decides whether to fail or continue after failed jobs.
        """
        logging.info(f'Attempting to run {function.__name__} for total {len(list_of_jobs_args_list)} jobs')

        total_jobs = len(list_of_jobs_args_list)
        completed_jobs = 0
        failed_jobs = 0

        with futures.ThreadPoolExecutor(workers) as pool:
            future_to_job = {
                pool.submit(self.execute_with_retries, function, job_args, max_retries): job_args
                for job_args in list_of_jobs_args_list
            }

            for future in futures.as_completed(future_to_job):
                job_args = future_to_job[future]
                try:
                    result = future.result()
                    if result:
                        completed_jobs += 1
                        logging.info(f"Job {job_args} completed successfully.")
                    else:
                        failed_jobs += 1
                        logging.error(f"Job {job_args} failed after {max_retries} retries.")
                except Exception as e:
                    failed_jobs += 1
                    logging.error(f"Job {job_args} raised an exception: {e}")

        logging.info(f"Successfully ran {completed_jobs}/{total_jobs} jobs")
        logging.info(f"Failed {failed_jobs}/{total_jobs} jobs")

        if failed_jobs > 0:
            if fail_on_error:
                logging.error(f"Exiting due to {failed_jobs} failed jobs.")
                raise Exception(f"{failed_jobs} jobs failed after retries.")
            else:
                logging.warning(f"{failed_jobs} jobs failed, but continuing execution as 'fail_on_error' is False.")
        else:
            logging.info(f"All jobs completed successfully!")
