version 1.0


workflow TerraSummaryStatistics {
    input {
        String billing_project
        String workspace_name
        String? data_dictionary_file
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call TerraSummaryStatisticsTask {
        input:
            billing_project = billing_project,
            workspace_name = workspace_name,
            data_dictionary_file = data_dictionary_file,
            docker_image = docker_image
    }

    if (data_dictionary_file) {
        call CopyDataDictionary {
            input:
                summary_file = TerraSummaryStatisticsTask.summary_statistics,
                data_dictionary_file = data_dictionary_file
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
        String? data_dictionary_file
    }

    command <<<
        directory=$(dirname "$~{data_dictionary_file}")
        gcloud storage cp ~{summary_file} "$directory"/
    >>>

    runtime {
        docker: "google/cloud-sdk:latest"
    }
}
