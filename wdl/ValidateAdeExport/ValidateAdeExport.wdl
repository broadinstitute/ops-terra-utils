version 1.0

workflow ValidateAdeExport {
    input {
        String snapshot_id
        String billing_project
        File azul_token
        Boolean allow_controlled_access = false
        Boolean keep_workspace = false
        String? docker
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call ValidateAdeExportTask {
        input:
            snapshot_id = snapshot_id,
            billing_project = billing_project,
            azul_token = azul_token,
            allow_controlled_access = allow_controlled_access,
            keep_workspace = keep_workspace,
            docker_image = docker_image
    }

    output {
        Boolean validation_passed = ValidateAdeExportTask.validation_passed
        File validation_failures = ValidateAdeExportTask.validation_failures
        String kept_workspace_name = ValidateAdeExportTask.kept_workspace_name
    }
}

task ValidateAdeExportTask {
    input {
        String snapshot_id
        String billing_project
        File azul_token
        Boolean allow_controlled_access
        Boolean keep_workspace
        String docker_image
    }

    command <<<
        python /etc/terra_utils/python/validate_ade_export.py \
        --snapshot_id ~{snapshot_id} \
        --billing_project ~{billing_project} \
        --azul_token_file ~{azul_token} \
        ~{true="--allow_controlled_access" false="" allow_controlled_access} \
        ~{true="--keep_workspace" false="" keep_workspace}
    >>>

    output {
        # The failures file is empty when every validation passed
        Boolean validation_passed = size("validation_failures.txt") == 0
        File validation_failures = "validation_failures.txt"
        # Empty if the workspace was deleted
        String kept_workspace_name = read_string("workspace_name.txt")
    }

    runtime {
        docker: docker_image
        memory: "4 GB"
        disks: "local-disk 20 HDD"
    }
}
