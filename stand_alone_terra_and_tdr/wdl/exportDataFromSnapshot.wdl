version 1.0
import "gcpUtils.wdl" as gcp_utils


workflow ExportDataFromSnapshotToBucket {
    input {
        String snapshot_id
        String output_bucket
        String download_type
        Int? max_backoff_time
        Int? max_retries
        String? docker
    }

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

    Array[Array[String]] mapping_info = read_tsv(GetFileMapping.source_destination_mapping)

    scatter (line in mapping_info) {
        call gcp_utils.CopyGCPFile
            input:
                source_file_path = mapping_info[0]
                destination_file_path = mapping_info[1]
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