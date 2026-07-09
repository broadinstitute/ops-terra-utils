version 1.0

workflow CreateWorkspaceFileManifest {
    input {
        String workspace_name
        String billing_project
        String? extension_exclude_list
        String? extension_include_list
        String? strings_to_exclude
        String? docker
        String? additional_external_paths
        Boolean include_external_files = false
    }

    String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


    call CreateManifest {
        input:
            workspace_name=workspace_name,
            billing_project=billing_project,
            extension_exclude_list=extension_exclude_list,
            extension_include_list=extension_include_list,
            strings_to_exclude=strings_to_exclude,
            docker_image=docker_name,
            include_external_files=include_external_files,
            additional_external_paths=additional_external_paths
    }
}

task CreateManifest {
    input {
        String workspace_name
        String billing_project
        String? extension_exclude_list
        String? extension_include_list
        String? strings_to_exclude
        String? additional_external_paths
        String docker_image
        Boolean include_external_files
    }

    command <<<
        python /etc/terra_utils/python/create_workspace_file_manifest.py \
        --workspace_name  ~{workspace_name} \
        --billing_project  ~{billing_project} \
        ~{"--extension_exclude_list " + extension_exclude_list} \
        ~{"--extension_include_list " + extension_include_list} \
        ~{"--strings_to_exclude " + strings_to_exclude} \
        ~{"--additional_external_paths " + additional_external_paths} \
        ~{if include_external_files then "--include_external_files" else ""}
    >>>

    runtime {
        docker: docker_image
    }

}
