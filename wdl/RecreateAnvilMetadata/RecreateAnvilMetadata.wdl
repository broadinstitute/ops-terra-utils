version 1.0

workflow RecreateAnvilMetadata {
  input {
    String workspace_name
    String billing_project
    String dataset_id
    Boolean force
    String? tables_to_ignore
    String? table_prefix_to_ignore
    String? docker
  }

  String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


  call RecreateAnvilMetadataTask {
    input:
      workspace_name = workspace_name,
      billing_project = billing_project,
      dataset_id = dataset_id,
      force = force,
      tables_to_ignore = tables_to_ignore,
      table_prefix_to_ignore = table_prefix_to_ignore,
      docker_name = docker_name
  }
}

task RecreateAnvilMetadataTask {
  input {
    String workspace_name
    String billing_project
    String dataset_id
    Boolean force
    String? tables_to_ignore
    String? table_prefix_to_ignore
    String docker_name
  }

  command <<<
    python3 /etc/terra_utils/python/recreate_anvil_metadata.py \
      --workspace_name ~{workspace_name} \
      --billing_project ~{billing_project} \
      --dataset_id ~{dataset_id} \
      ~{"--tables_to_ignore " + tables_to_ignore} \
      ~{"--table_prefix_to_ignore " + table_prefix_to_ignore} \
      ~{if force then '--force' else ''}
  >>>

  runtime {
    docker: docker_name
  }
}
