version 1.0

workflow ExportDataFromSnapshotToBucket {
    input {
        String snapshot_id
        String output_bucket
        String download_type
        Int? max_backoff_time
        Int? max_retries
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call CopyFilesFromSnapshotToBucket {
        input:
            snapshot_id = snapshot_id,
            output_bucket = output_bucket,
            download_type = download_type,
            max_backoff_time = max_backoff_time,
            max_retries = max_retries,
            docker_image = docker_image
    }

}

task CopyFilesFromSnapshotToBucket {
    input {
        String snapshot_id
        String output_bucket
        String download_type
        Int? max_backoff_time
        Int? max_retries
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/export_data_from_snapshot_or_dataset.py \
        --snapshot_id  ~{snapshot_id} \
        --output_bucket  ~{output_bucket} \
        --download_type  ~{download_type} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time}
    >>>

    runtime {
        docker: docker_image
    }

}
