version 1.0

workflow ingest_dragen_data_to_tdr {
    input {
        String   sample_set
        String   target_table_name
        String?  docker
        String  dataset_id
        # Use if you want bulk mode used for ingest
        Boolean  bulk_mode = false
        Boolean  dry_run = false
        Boolean  filter_entity_already_in_dataset = false
        Int      batch_size = 500
        Int      waiting_time_to_poll = 180
        String   update_strategy = "replace" # append, replace, or merge
        String   billing_project
        String   workspace_name
        String   unique_id_field = "sample_id"
        String   table_name = "sample"
        String   dragen_version = "07.021.604.3.7.8"
    }

    String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call ingest_dragen_data_to_tdr {
        input:
            sample_set = sample_set,
            target_table_name = target_table_name,
            docker_name = docker_name,
            dataset_id = dataset_id,
            bulk_mode = bulk_mode,
            dry_run = dry_run,
            filter_entity_already_in_dataset = filter_entity_already_in_dataset,
            batch_size = batch_size,
            waiting_time_to_poll = waiting_time_to_poll,
            update_strategy = update_strategy,
            billing_project = billing_project,
            workspace_name = workspace_name,
            unique_id_field = unique_id_field,
            table_name = table_name,
            dragen_version = dragen_version
    }
}

task ingest_dragen_data_to_tdr {
        input {
            String  sample_set
            String  target_table_name
            String docker_name
            String dataset_id
            Boolean bulk_mode = false
            Boolean dry_run = false
            Boolean filter_entity_already_in_dataset = false
            Int     batch_size = 500
            Int     waiting_time_to_poll = 180
            String  update_strategy = "replace" # append, replace, or merge
            String  billing_project
            String  workspace_name
            String  unique_id_field = "sample_id"
            String  table_name = "sample"
            String  dragen_version = "07.021.604.3.7.8"
        }

        command {
            python /etc/terra_utils/python/dragen/populate_dragen_reprocessing_status.py \
                --sample_set ~{sample_set} \
                --workspace_name ~{workspace_name} \
                --billing_project ~{billing_project} \
                --dataset_id ~{dataset_id} \
                --unique_id_field ~{unique_id_field} \
                --table_name ~{table_name} \
                --ingest_waiting_time_poll ~{waiting_time_to_poll} \
                --dragen_version ~{dragen_version} \
                --ingest_batch_size ~{batch_size} \
                --update_strategy ~{update_strategy} \
                ~{if bulk_mode then "--bulk_mode" else ""}
                ~{if dry_run then "--dry_run" else ""} \
                ~{if filter_entity_already_in_dataset then "--filter_existing_ids" else ""}
        }

        runtime {
            docker: docker_name
        }

        output {
            File    ingest_logs = stdout()
        }
}
