version 1.0

workflow GetFilesNotInDatasetMetadata {
    input {
        String dataset_id
        Int? max_retries
        Int? max_backoff_time
    }

    String docker_image = select_first([docker, "johnscira/test_docker_repo:latest"])

    call GetFilesNotInDataset {
        input:
            dataset_id = dataset_id,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image
    }

}

task GetFilesNotInDataset {
    input {
        String dataset_id
        Int? max_retries
        Int? max_backoff_time
        String docker_image
    }

    command <<<
        python /etc/terra_utils/get_files_not_dataset_metadata.py \
        --dataset_id  ~{dataset_id} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
    >>>

    runtime {
        docker: docker_image
    }
}
