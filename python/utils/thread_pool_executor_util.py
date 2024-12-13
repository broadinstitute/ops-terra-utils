from concurrent import futures
from typing import Callable, Any, Optional
import logging


class MultiThreadedJobs:

    def execute_with_retries(
            self,
            function: Callable,
            job_args_list: list[Any],
            max_retries: int
    ) -> Any:
        """
        Execute a function with retries.

        Args:
            function (Callable): The function to execute.
            job_args_list (list[Any]): The list of arguments to pass to the function.
            max_retries (int): The maximum number of retries.

        Returns:
            Any: The result of the function if it executes successfully, None otherwise.
        """
        retries = 0
        while retries < max_retries:
            try:
                return function(*job_args_list)
            except Exception as e:
                logging.warning(f"Job failed with error: {e}. Retry {retries + 1}/{max_retries}")
                retries += 1
        return None

    def run_multi_threaded_job(
            self,
            workers: int,
            function: Callable,
            list_of_jobs_args_list: list[Any],
            collect_output: bool = False,
            max_retries: int = 3,
            fail_on_error: bool = True,
            verbose: bool = False,
            jobs_complete_for_logging: int = 500
    ) -> Optional[list[Any]]:
        """
        Run jobs in parallel and allow for retries. Optionally collect outputs of the jobs.

        Args:
            workers (int): The number of worker threads.
            function (Callable): The function to execute.
            list_of_jobs_args_list (list[Any]): The list of job arguments.
            collect_output (bool, optional): Whether to collect and return job outputs. Defaults to False.
            max_retries (int, optional): The maximum number of retries. Defaults to 3.
            fail_on_error (bool, optional): Whether to fail on error. Defaults to True.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            jobs_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 250.

        Returns:
            Optional[list[Any]]: A list of job results if `collect_output` is True, otherwise None.
        """
        total_jobs = len(list_of_jobs_args_list)
        logging.info(f'Attempting to run {function.__name__} for a total of {total_jobs} jobs')

        completed_jobs = 0
        failed_jobs = 0
        # Initialize job results list if output is expected
        job_results = []

        with futures.ThreadPoolExecutor(workers) as pool:
            future_to_job = {
                pool.submit(self.execute_with_retries, function, job_args, max_retries): job_args
                for job_args in list_of_jobs_args_list
            }

            for future in futures.as_completed(future_to_job):
                job_args = future_to_job[future]
                try:
                    result = future.result()
                    # Successful result or no result if not collecting output
                    if result or (result is None and not collect_output):
                        completed_jobs += 1
                        # Log progress every `jobs_complete_for_logging` jobs
                        if completed_jobs % jobs_complete_for_logging == 0:
                            logging.info(f"Completed {completed_jobs}/{total_jobs} jobs")
                        # Log success for each job if verbose
                        if verbose:
                            logging.info(f"Job {job_args} completed successfully.")
                        # Collect result if output is expected
                        if collect_output:
                            job_results.append(result)
                    else:
                        failed_jobs += 1
                        logging.error(f"Job {job_args} failed after {max_retries} retries.")
                except Exception as e:
                    failed_jobs += 1
                    logging.error(f"Job {job_args} raised an exception: {e}")

        logging.info(f"Successfully ran {completed_jobs}/{total_jobs} jobs")
        logging.info(f"Failed {failed_jobs}/{total_jobs} jobs")

        if failed_jobs > 0 and fail_on_error:
            logging.error(f"Exiting due to {failed_jobs} failed jobs.")
            raise Exception(f"{failed_jobs} jobs failed after retries.")

        if collect_output:
            return job_results
        else:
            return None
