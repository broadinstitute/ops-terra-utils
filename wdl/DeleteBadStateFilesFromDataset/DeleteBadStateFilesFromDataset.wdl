version 1.0

workflow DeleteBadStateFilesFromDataset {
    input {
        String dataset_id
        Int? file_query_limit
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call DeleteFilesFromDataset {
        input:
            dataset_id = dataset_id,
            file_query_limit = file_query_limit,
            docker_image = docker_image
    }
}

task DeleteFilesFromDataset {
    input {
        String dataset_id
        Int? file_query_limit
        String? docker_image
    }

    command <<<
        python /etc/terra_utils/delete_files_from_datasets_in_bad_state.py \
        --dataset_id  ~{dataset_id} \
        ~{"--file_query_limit " + file_query_limit} \
    >>>

    runtime {
        docker: docker_image
    }

}
