version 1.0

workflow TdrSummaryStatistics {
    input {
        String? dataset_id
        String? snapshot_id
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call TdrSummaryStatisticsTask {
        input:
            dataset_id = dataset_id,
            snapshot_id = snapshot_id,
            docker_image = docker_image
    }

    output {
        File summary_statistics = TdrSummaryStatisticsTask.summary_statistics
    }
}

task TdrSummaryStatisticsTask {
    input {
        String? dataset_id
        String? snapshot_id
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/tdr_summary_statistics.py \
        ~{"--dataset_id " + dataset_id} \
        ~{"--snapshot_id " + snapshot_id}
    >>>

    output {
        File summary_statistics = "summary_statistics.tsv"
    }

    runtime {
        docker: docker_image
    }
}
