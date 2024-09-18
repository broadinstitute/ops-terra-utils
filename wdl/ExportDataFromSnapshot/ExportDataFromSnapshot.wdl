version 1.0
import "../utilities/GcpUtils.wdl" as gcp_utils


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

    call GetFileMapping {
        input:
            snapshot_id = snapshot_id,
            output_bucket = output_bucket,
            download_type = download_type,
            max_backoff_time = max_backoff_time,
            max_retries = max_retries,
            docker_image = docker_image
    }

    call gcp_utils.CopyGCPSourceToDestinationFromMappingTsv {
        input:
            mapping_tsv = GetFileMapping.source_destination_mapping_file
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
        File source_destination_mapping_file = "file_mapping.tsv"
    }

}
