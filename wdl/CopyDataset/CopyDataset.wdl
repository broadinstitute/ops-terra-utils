version 1.0

workflow CopyDataset {
    input {
		String new_billing_profile
		String orig_dataset_id
		String new_dataset_name
		Boolean bulk_mode
		Boolean continue_if_exists
		Int? waiting_time_to_poll
		Int? ingest_batch_size
		String? update_strategy
		String? docker
		Boolean filter_out_entity_already_in_dataset
	}

	String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])


	call RunCopyDataset {
		input:
			new_billing_profile=new_billing_profile,
			orig_dataset_id=orig_dataset_id,
			ingest_batch_size=ingest_batch_size,
			update_strategy=update_strategy,
			new_dataset_name=new_dataset_name,
			waiting_time_to_poll=waiting_time_to_poll,
			bulk_mode=bulk_mode,
			continue_if_exists=continue_if_exists,
			docker_name=docker_name,
			filter_out_entity_already_in_dataset=filter_out_entity_already_in_dataset
	}
}

task RunCopyDataset {
	input {
		String new_billing_profile
		String orig_dataset_id
		String docker_name
		String new_dataset_name
		Int? ingest_batch_size
		String? update_strategy
		Int? waiting_time_to_poll
		Boolean bulk_mode
		Boolean continue_if_exists
		Boolean filter_out_entity_already_in_dataset
	}

	command <<<
		python /etc/terra_utils/python/copy_dataset.py \
		--new_billing_profile ~{new_billing_profile} \
		--orig_dataset_id ~{orig_dataset_id} \
		--new_dataset_name ~{new_dataset_name} \
		~{"--ingest_batch_size " + ingest_batch_size} \
		~{"--update_strategy " + update_strategy} \
		~{"--waiting_time_to_poll " + waiting_time_to_poll} \
		~{if bulk_mode then "--bulk_mode" else ""} \
		~{if filter_out_entity_already_in_dataset then "--filter_out_existing_ids" else ""} \
		~{if continue_if_exists then "--continue_if_exists" else ""}
	>>>

	runtime {
		docker: docker_name
	}
}
