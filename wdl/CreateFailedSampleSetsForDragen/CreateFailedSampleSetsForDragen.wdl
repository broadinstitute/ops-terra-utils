version 1.0

workflow CreateFailedSampleSetsForDragen {
    input {
        String workspace_name
        String billing_project
        String sample_set_append
        Int max_per_sample_set = 2000
        String? docker
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call CreateFailedSampleSetsForDragenTask {
        input:
            workspace_name = workspace_name,
            billing_project = billing_project,
            sample_set_append = sample_set_append,
            max_per_sample_set = max_per_sample_set,
            docker_name = docker_name
    }
}

task CreateFailedSampleSetsForDragenTask {
    input {
        String workspace_name
        String billing_project
        String sample_set_append
        Int max_per_sample_set
        String docker_name
    }

    command <<<
    python3 /etc/terra_utils/python/dragen/create_failed_sample_sets_for_dragen.py \
        --workspace_name ~{workspace_name} \
        --billing_project ~{billing_project} \
        --sample_set_append ~{sample_set_append} \
        --max_per_sample_set ~{max_per_sample_set} \
        --upload
    >>>

    runtime {
        docker: docker_name
    }
}
