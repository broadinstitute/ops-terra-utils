version 1.0

workflow ValidateStagingWorkspace {
    input {
        String billing_project
        String workspace_name
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
            summary_statistics = ValidateStagingWorkspaceTask.summary_statistics,
            validation_report = ValidateStagingWorkspaceTask.validation_report,
            validation_detail_files = ValidateStagingWorkspaceTask.validation_detail_files,
            data_dictionary_file = data_dictionary_file,
            output_dir = output_dir_with_slash
    }

    output {
        String summary_statistics = CopyOutputs.summary_statistics_path
        String validation_report = CopyOutputs.validation_report_path
        Array[String] validation_detail_files = CopyOutputs.validation_detail_file_paths
        String data_dictionary_file_path = CopyOutputs.data_dictionary_file_path
    }
}

task ValidateStagingWorkspaceTask {
    input {
        String billing_project
        String workspace_name
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
        File summary_statistics
        File validation_report
        Array[File] validation_detail_files
        File? data_dictionary_file
        String output_dir
    }

    command <<<
        set -e
        gcloud storage cp ~{summary_statistics} ~{validation_report} ~{sep=" " validation_detail_files} ~{output_dir}
        # Copy the data dictionary file (if provided) so it is saved alongside the outputs it was used for
        ~{"gcloud storage cp " + data_dictionary_file + " " + output_dir}

        # Write the final location of each file so they can be used as outputs
        echo "~{output_dir}$(basename ~{summary_statistics})" > summary_statistics_path.txt
        echo "~{output_dir}$(basename ~{validation_report})" > validation_report_path.txt
        # Empty if no data dictionary file was provided
        touch data_dictionary_file_path.txt
        ~{"echo " + output_dir + "$(basename " + data_dictionary_file + ") > data_dictionary_file_path.txt"}
        touch validation_detail_file_paths.txt
        for detail_file in ~{sep=" " validation_detail_files}; do
            echo "~{output_dir}$(basename "$detail_file")" >> validation_detail_file_paths.txt
        done
    >>>

    output {
        String summary_statistics_path = read_string("summary_statistics_path.txt")
        String validation_report_path = read_string("validation_report_path.txt")
        Array[String] validation_detail_file_paths = read_lines("validation_detail_file_paths.txt")
        String data_dictionary_file_path = read_string("data_dictionary_file_path.txt")
    }

    runtime {
        docker: "google/cloud-sdk:latest"
    }
}
