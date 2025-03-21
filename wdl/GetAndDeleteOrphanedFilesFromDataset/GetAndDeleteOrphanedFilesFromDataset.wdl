version 1.0

workflow GetAndDeleteOrphanedFilesFromDataset {
    input {
        String dataset_id
        Int? max_retries
        Int? max_backoff_time
        Int? batch_size_to_list_files
        String? docker
        Int? batch_size_to_delete_files
        Boolean delete_orphaned_files
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call GetFilesNotInDataset {
        input:
            dataset_id = dataset_id,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image,
            delete_orphaned_files = delete_orphaned_files,
            batch_size_to_list_files = batch_size_to_list_files,
            batch_size_to_delete_files = batch_size_to_delete_files
    }

}

task GetFilesNotInDataset {
    input {
        String dataset_id
        Int? max_retries
        Int? max_backoff_time
        String docker_image
        Boolean delete_orphaned_files
        Int? batch_size_to_list_files
        Int? batch_size_to_delete_files
    }

    command <<<
        python /etc/terra_utils/python/get_and_delete_orphaned_files_from_dataset.py \
        --dataset_id  ~{dataset_id} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--batch_size_to_list_files " + batch_size_to_list_files} \
        ~{"--batch_size_to_delete_files " + batch_size_to_delete_files} \
        ~{if delete_orphaned_files then "--delete_orphaned_files" else ""}
    >>>

    runtime {
        docker: docker_image
    }
}
