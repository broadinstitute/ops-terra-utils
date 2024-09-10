version 1.0
import "gcp_utils.wdl" as gcp_utils


workflow {
    input:
        String snapshot_id
        String output_bucket
        String download_type
        Int? max_backoff_time
        Int? max_retries
        String? docker

    String docker_image = select_first([docker, "johnscira/test_docker_repo:latest"])

    call GetFileMapping {
        input:
            snapshot_id = snapshot_id,
            output_bucket = output_bucket,
            download_type = download_type,
            max_backoff_time = max_backoff_time,
            max_retries = max_retries,
            docker_image = docker_image
    }

    call gcp_utils.CopySourceToDestFromMappingTsv {
        input:
            mapping_file = GetFileMapping.source_destination_mapping
    }

}

task GetFileMapping {
    input {
        String snapshot_id
        String output_bucket
        String download_type
        Int? max_backoff_time
        Int? max_retries
        String docker_image
    }

    command <<<
        python /etc/terra_utils/export_data_from_snapshot.py \
        --snapshot_id  ~{snapshot_id} \
        --output_bucket  ~{output_bucket} \
        --download_type  ~{download_type} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time}
    >>>

    runtime {
        docker: docker_image
    }

    output {
        File source_destination_mapping = "file_mapping.tsv"
    }

}