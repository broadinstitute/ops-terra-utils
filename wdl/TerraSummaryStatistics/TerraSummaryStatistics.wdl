version 1.0

workflow TerraSummaryStatistics {
    input {
        String billing_project
        String workspace_name
        String? docker
        File? data_dictionary_file
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call TerraSummaryStatisticsTask {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            data_dictionary_file = data_dictionary_file,
            docker_image = docker_image
    }

    #Boolean data_dictionary_file_exists =
    if (defined(data_dictionary_file)) {
        # Get the file directory of the data_dictionary_file as a string
        String data_dictionary_string = select_first([data_dictionary_file, ""])
        # Get the file name of the data_dictionary_file
        String data_dictionary_file_name = basename(data_dictionary_string)
        # Get the file directory of the data_dictionary_file by removing the file name
        String data_dictionary_file_dir = sub(data_dictionary_string, data_dictionary_file_name, "")
        call CopyDataDictionary {
            input:
                summary_file = TerraSummaryStatisticsTask.summary_statistics,
                data_dictionary_file_dir = data_dictionary_file_dir
        }
    }

    output {
        File summary_statistics = TerraSummaryStatisticsTask.summary_statistics
    }
}

task TerraSummaryStatisticsTask {
    input {
        String billing_project
        String workspace_name
        File? data_dictionary_file
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/terra_summary_statistics.py \
        --billing_project ~{billing_project} \
        --workspace_name ~{workspace_name} \
        ~{"--data_dictionary_file " + data_dictionary_file}
    >>>

    output {
        File summary_statistics = "~{billing_project}.~{workspace_name}.summary_stats.tsv"
    }

    runtime {
        docker: docker_image
    }
}

task CopyDataDictionary {
    input {
        File summary_file
        String data_dictionary_file_dir
    }

    command <<<
        gcloud storage cp ~{summary_file} ~{data_dictionary_file_dir}
    >>>

    runtime {
        docker: "google/cloud-sdk:latest"
    }
}
