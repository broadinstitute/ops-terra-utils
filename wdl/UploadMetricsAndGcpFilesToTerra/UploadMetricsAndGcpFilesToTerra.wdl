version 1.0

workflow GCPWorkspaceToDatasetIngest {
    input {
        String billing_project
        String workspace_name
        File metrics_tsv
        String? skip_upload_columns
        Boolean flatten_path
        String? subdir
        String id_column
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call UploadMetricsAndFilesToTerra {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            metrics_tsv = metrics_tsv,
            skip_upload_columns = skip_upload_columns,
            flatten_path = flatten_path,
            subdir = subdir,
            id_column = id_column,
            docker_image = docker_image
    }
}

task UploadMetricsAndFilesToTerra {
    input {
        String billing_project
        String workspace_name
        File metrics_tsv
        String? skip_upload_columns
        Boolean flatten_path
        String? subdir
        String id_column
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/upload_metrics_and_files_to_terra.py \
        --billing_project  ~{billing_project} \
        --workspace_name  "~{workspace_name}" \
        --metrics_tsv  ~{metrics_tsv} \
        --id_column  ~{id_column} \
        ~{if flatten_path then "--flatten_path" else ""} \
        ~{"--skip_upload_columns" + skip_upload_columns} \
        ~{"--subdir " + subdir}
    >>>

    runtime {
		docker: docker_image
	}

}
