version 1.0

workflow ValidateStagingWorkspace {
    input {
        String billing_project
        String workspace_name
        Boolean check_external_buckets
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
            data_dictionary_file = data_dictionary_file,
            docker_image = docker_image,
            memory_gb = select_first([memory_gb, 8]),
            disk_size_gb = select_first([disk_size_gb, 50])
    }

    # Get the gs:// path of the summary statistics file and the directory all outputs were written to
    String summary_statistics_path = ValidateStagingWorkspaceTask.summary_statistics
    String summary_statistics_name = basename(summary_statistics_path)
    String output_dir = sub(summary_statistics_path, summary_statistics_name, "")

    # The report is written before the final location of the outputs is known so fill that in here
    call FinalizeReport {
        input:
            report = ValidateStagingWorkspaceTask.validation_report,
            output_dir = output_dir,
            docker_image = docker_image
    }

    if (defined(data_dictionary_file)) {
        # Get the file directory of the data_dictionary_file as a string
        String data_dictionary_string = select_first([data_dictionary_file, ""])
        # Get the file name of the data_dictionary_file
        String data_dictionary_file_name = basename(data_dictionary_string)
        # Get the file directory of the data_dictionary_file by removing the file name
        String data_dictionary_file_dir = sub(data_dictionary_string, data_dictionary_file_name, "")
        call CopyOutputsToDataDictionaryDir {
            input:
                files_to_copy = flatten(
                    [
                        [ValidateStagingWorkspaceTask.summary_statistics, FinalizeReport.final_report],
                        ValidateStagingWorkspaceTask.validation_detail_files
                    ]
                ),
                data_dictionary_file_dir = data_dictionary_file_dir
        }
    }

    output {
        File summary_statistics = ValidateStagingWorkspaceTask.summary_statistics
        File validation_report = FinalizeReport.final_report
        Array[File] validation_detail_files = ValidateStagingWorkspaceTask.validation_detail_files
    }
}

task ValidateStagingWorkspaceTask {
    input {
        String billing_project
        String workspace_name
        Boolean check_external_buckets
        File? data_dictionary_file
        String docker_image
        Int memory_gb
        Int disk_size_gb
    }

    command <<<
        python /etc/terra_utils/python/terra_summary_statistics.py \
        --billing_project ~{billing_project} \
        --workspace_name ~{workspace_name} \
        --output_location '{{OUTPUT_DIR}}' \
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

task FinalizeReport {
    input {
        File report
        String output_dir
        String docker_image
    }

    String report_name = basename(report)

    command <<<
        sed 's|{{OUTPUT_DIR}}|~{output_dir}|g' ~{report} > ~{report_name}
    >>>

    output {
        File final_report = report_name
    }

    runtime {
        docker: docker_image
    }
}

task CopyOutputsToDataDictionaryDir {
    input {
        Array[File] files_to_copy
        String data_dictionary_file_dir
    }

    command <<<
        echo "Copying outputs to the same directory as the data dictionary file"
        gcloud storage cp ~{sep=" " files_to_copy} ~{data_dictionary_file_dir}
    >>>

    runtime {
        docker: "google/cloud-sdk:latest"
    }
}
