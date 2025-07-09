import re
import logging
from typing import Optional, Any
from ops_utils.bq_utils import BigQueryUtil

SAMPLE_TSV = "sample.tsv"


class GetSampleInfo:
    def __init__(self, google_project: str, minimum_run_date: str, maximum_run_date: str, data_type: str):
        self.google_project = google_project
        self.bqclient = BigQueryUtil(project_id=google_project)
        self.minimum_run_date = minimum_run_date
        self.maximum_run_date = maximum_run_date
        self.data_type = data_type

    def _query_for_batch_job_info(self) -> Any:
        """Gets all batch job info from bigquery"""
        query_string = f"""SELECT a.job_id, a.input_path, s.status, s.task_id, a.timestamp as submit_time,
        a.output_path, s.timestamp as task_time, a.sample_id
FROM `{self.google_project}.dragen_illumina.job_array` as a
join `{self.google_project}.dragen_illumina.tasks_status` as s on a.job_id = s.job_id
  and CAST(a.batch_task_index AS STRING)=REGEXP_EXTRACT(task_id, r'group0-(\d+)')
where DATETIME "{self.maximum_run_date}" > a.timestamp
and DATETIME "{self.minimum_run_date}" < a.timestamp"""

        logging.info(f"Querying for jobs status")
        return self.bqclient.query_table(query_string)

    def _create_sample_dict(self, row: dict, sample_info: dict) -> dict:
        """Create an initial sample dict"""
        sample_workflow_dict = {
            'latest_status': row['status'],
            'latest_timestamp': row['task_time'],
            'latest_task_id': row['task_id'],
            'latest_job_id': row['job_id'],
            # Create a dictionary of jobs ids and running time for each one
            'running_time_dict': {row['job_id']: "N/A"},
            # Create a dictionary of jobs ids and earliest time stamp
            'earliest_timestamp_dict': {row['job_id']: row['task_time']},
            # Start a set for total job ids
            'job_ids': {row['job_id']},
            # Replace changed start of cloud path
            'output_path': row['output_path'].replace('s3://', 'gs://')
        }
        # Add all sample info to above dict
        sample_workflow_dict.update(sample_info)
        return sample_workflow_dict

    @staticmethod
    def _get_sample_information_from_cram_path(cram_path: str) -> Optional[dict]:
        """Get sample information from input cram path"""
        # assume path like gs://{bucket}/{sample set}/{project}/{data_type}/{sample}/v{version}/{sample}.cram
        match = re.search(r'gs://\S+/\S+/(\S+)/(\S+)/(\S+)/v(\d+)/\S+.cram', cram_path)
        if match:
            project = match.group(1)
            data_type = match.group(2)
            sample = match.group(3)
            version = match.group(4)
            terra_sample_id = f"{project}_{sample}_v{version}_{data_type}_GCP".replace(".", "_")
            return {
                'terra_sample_id': terra_sample_id,
                'sample_id': f'{project}.{data_type}.{sample}.{version}'
            }
        return None

    def _create_full_samples_dicts(self, query_results: Any) -> dict:
        """Creates full dictionary where key is sample name and value is dict with all jobs info"""
        samples_dict = {}
        for row in query_results:
            if self.data_type == 'bge':
                # Get sample information from cram input
                sample_information = self._get_sample_information_from_cram_path(
                    row['input_path'].replace('s3://', 'gs://')
                )
            else:
                # wgs data_type sample_id is collab_id
                sample_information = {
                    'sample_id': row['sample_id'],
                    'terra_sample_id': row['sample_id']
                }
            if sample_information:
                sample_id = sample_information['sample_id']
                if sample_id not in samples_dict:
                    # Create initial dict for sample
                    samples_dict[sample_id] = self._create_sample_dict(row, sample_information)
                else:
                    # Update existing sample dict with current run information
                    sample_dict = samples_dict[sample_id]
                    # Add to job ids set
                    job_id = row['job_id']
                    sample_dict['job_ids'].add(job_id)
                    # Check if job_id has the earliest time registered
                    if job_id not in sample_dict['earliest_timestamp_dict']:
                        # Register current time as the earliest job time for specific job id
                        sample_dict['earliest_timestamp_dict'][job_id] = row['task_time']
                        sample_dict['running_time_dict'][job_id] = "N/A"
                    else:
                        # Check if task time is the earliest time for this job_id
                        if row['task_time'] < sample_dict['earliest_timestamp_dict'][job_id]:
                            # Set earliest time for this job_id
                            sample_dict['earliest_timestamp_dict'][job_id] = row['task_time']
                            # recalculate running time for specific job
                            sample_dict['running_time_dict'][job_id] = str(
                                sample_dict['latest_timestamp'] - row['task_time'])
                    # If entry is the latest timestamp use this status
                    if row['task_time'] > sample_dict['latest_timestamp']:
                        sample_dict['latest_timestamp'] = row['task_time']
                        sample_dict['latest_status'] = row['status']
                        sample_dict['latest_job_id'] = row['job_id']
                        sample_dict['latest_task_id'] = row['task_id']
                        sample_dict['output_path'] = row['output_path'].replace('s3://', 'gs://')
                        # recalculate running time for latest job
                        sample_dict['running_time_dict'][job_id] = str(
                            row['task_time'] - sample_dict['earliest_timestamp_dict'][job_id])
        return samples_dict

    def run(self) -> dict:
        query_results = self._query_for_batch_job_info()
        return self._create_full_samples_dicts(query_results)


class CreateSampleTsv:
    def __init__(self, samples_dict: dict, output_tsv: str):
        self.samples_dict = samples_dict
        self.output_tsv = output_tsv

    def create_tsv(self) -> None:
        logging.info(f"Creating {self.output_tsv}")
        with open(self.output_tsv, 'w') as f:
            f.write('entity:sample_id\tattempts\tlatest_status\tlatest_task_id\toutput_path\tlast_attempt\trunning_time\n')
            for sample_dict in self.samples_dict.values():
                f.write(
                    f"{sample_dict['terra_sample_id']}\t{len(sample_dict['job_ids'])}\t" +
                    f"{sample_dict['latest_status']}\t{sample_dict['latest_task_id']}\t" +
                    f"{sample_dict['output_path']}\t{sample_dict['latest_timestamp']}\t" +
                    f"{str(sample_dict['running_time_dict'][sample_dict['latest_job_id']])}\n"
                )
