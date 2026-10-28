version 1.0

workflow ValidateStagingWorkspace {
    input {
        String billing_project
        String workspace_name
        Boolean check_external_buckets
        String output_dir
        String? docker
        File? data_dictionary_file
        Int? memory_gb
        Int? disk_size_gb
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call ValidateStagingWorkspaceTask {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            check_external_buckets = check_external_buckets,
            output_dir = output_dir_with_slash,
            data_dictionary_file = data_dictionary_file,
            docker_image = docker_image,
            memory_gb = select_first([memory_gb, 8]),
            disk_size_gb = select_first([disk_size_gb, 50])
    }

    # Make sure the output directory ends in a slash so files are copied into it
    String output_dir_with_slash = sub(output_dir, "/+$", "") + "/"

    call CopyOutputs {
        input:
            files_to_copy = flatten(
                [
                    [ValidateStagingWorkspaceTask.summary_statistics, ValidateStagingWorkspaceTask.validation_report],
                    ValidateStagingWorkspaceTask.validation_detail_files
                ]
            ),
            output_dir = output_dir_with_slash
    }

    output {
        File summary_statistics = ValidateStagingWorkspaceTask.summary_statistics
        File validation_report = ValidateStagingWorkspaceTask.validation_report
        Array[File] validation_detail_files = ValidateStagingWorkspaceTask.validation_detail_files
    }
}

task ValidateStagingWorkspaceTask {
    input {
        String billing_project
        String workspace_name
        Boolean check_external_buckets
        String output_dir
        File? data_dictionary_file
        String docker_image
        Int memory_gb
        Int disk_size_gb
    }

    command <<<
        python /etc/terra_utils/python/terra_summary_statistics.py \
        --billing_project ~{billing_project} \
        --workspace_name ~{workspace_name} \
        --output_dir ~{output_dir} \
        ~{if check_external_buckets then "--check_external_buckets" else ""} \
        ~{"--data_dictionary_file " + data_dictionary_file}
    >>>

    output {
        File summary_statistics = select_first(glob("*.summary_stats.*.tsv"))
        File validation_report = select_first(glob("*.validation_report.*.md"))
        Array[File] validation_detail_files = glob("*.validation_detail.*.tsv")
    }

    runtime {
        docker: docker_image
        memory: "~{memory_gb} GB"
        disks: "local-disk ~{disk_size_gb} HDD"
    }
}

task CopyOutputs {
    input {
        Array[File] files_to_copy
        String output_dir
    }

    command <<<
        gcloud storage cp ~{sep=" " files_to_copy} ~{output_dir}
    >>>

    runtime {
        docker: "google/cloud-sdk:latest"
    }
}
