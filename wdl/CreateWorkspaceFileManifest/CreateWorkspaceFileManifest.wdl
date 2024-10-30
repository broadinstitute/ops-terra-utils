version 1.0

workflow CreateWorkspaceFileManifest {
    input {
        String workspace_name
        String billing_project
        String? extension_exclude_list
        String? extension_include_list
        String? docker
    }

    String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


    call CreateManifest {
        input:
            workspace_name=workspace_name,
            billing_project=billing_project,
            extension_exclude_list=extension_exclude_list,
            extension_include_list=extension_include_list,
            docker_image=docker_name
    }
}

task CreateManifest {
    input {
        String workspace_name
        String billing_project
        String? extension_exclude_list
        String? extension_include_list
        String docker_image
    }

    command <<<
        python /etc/local_repo/python/create_workspace_file_manifest.py \
        --workspace_name  ~{workspace_name} \
        --billing_project  ~{billing_project} \
        ~{"--extension_exclude_list " + extension_exclude_list} \
        ~{"--extension_include_list " + extension_include_list}
    >>>

    runtime {
        docker: docker_image
    }

}