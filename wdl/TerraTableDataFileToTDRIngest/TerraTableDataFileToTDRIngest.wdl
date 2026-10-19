version 1.0

workflow TerraTableDataFileToTDRIngest {
    input {
        String input_file
        String dataset_id
        String target_table_name
        String? primary_key
        String? update_strategy
        String? records_to_ingest
        Boolean bulk_mode
        Boolean filter_existing_ids
        Boolean check_existing_ingested_files
        Boolean all_fields_non_required
        Int? max_retries
        Int? max_backoff_time
        Int? batch_size
        String? docker
        Boolean force_disparate_rows_to_string = true
        Boolean trunc_and_reload
        Boolean ignore_existing_schema_mismatch = false
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    # Size the disk from the input file (read from the GCS path without localizing it) plus a 10% buffer, with a
    # 10 GB minimum
    File input_file_for_size = input_file
    Int disk_size_gb = ceil(size(input_file_for_size, "GB") * 1.1) + 10
    # The file is held in memory as a list of dicts and again as a dataframe for schema inference, which takes several
    # times the file size in RAM. Use 8x the file size, with a 16 GB minimum
    Int memory_gb = if ceil(size(input_file_for_size, "GB") * 8) > 16 then ceil(size(input_file_for_size, "GB") * 8) else 16

    call IngestTabularFileToDataset {
        input:
            disk_size_gb = disk_size_gb,
            memory_gb = memory_gb,
            input_file = input_file,
            dataset_id = dataset_id,
            target_table_name = target_table_name,
            primary_key = primary_key,
            update_strategy = update_strategy,
            records_to_ingest = records_to_ingest,
            bulk_mode = bulk_mode,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            docker_image = docker_image,
            filter_existing_ids = filter_existing_ids,
            batch_size = batch_size,
            check_existing_ingested_files = check_existing_ingested_files,
            all_fields_non_required = all_fields_non_required,
            force_disparate_rows_to_string = force_disparate_rows_to_string,
            trunc_and_reload = trunc_and_reload,
            ignore_existing_schema_mismatch = ignore_existing_schema_mismatch
    }
}

task IngestTabularFileToDataset {
    input {
        String input_file
        String dataset_id
        String target_table_name
        String? primary_key
        String? update_strategy
        String? records_to_ingest
        Boolean bulk_mode
        Int? max_retries
        Int? max_backoff_time
        String docker_image
        Int disk_size_gb
        Int memory_gb
        Boolean filter_existing_ids
        Boolean check_existing_ingested_files
        Boolean all_fields_non_required
        Int? batch_size
        Boolean force_disparate_rows_to_string
        Boolean trunc_and_reload
        Boolean ignore_existing_schema_mismatch
    }

    command <<<
        python /etc/terra_utils/python/tabular_file_to_dataset_ingest.py \
        --input_file "~{input_file}" \
        --dataset_id ~{dataset_id} \
        --target_table_name ~{target_table_name} \
        ~{"--primary_key " + primary_key} \
        ~{"--update_strategy " + update_strategy} \
        ~{if bulk_mode then "--bulk_mode" else ""} \
        ~{"--max_retries " + max_retries} \
        ~{"--max_backoff_time " + max_backoff_time} \
        ~{"--records_to_ingest " + records_to_ingest} \
        ~{if filter_existing_ids then "--filter_existing_ids" else ""} \
        ~{if check_existing_ingested_files then "--check_existing_ingested_files" else ""} \
        ~{"--batch_size " + batch_size} \
        ~{if all_fields_non_required then "--all_fields_non_required" else ""} \
        ~{if force_disparate_rows_to_string then "--force_disparate_rows_to_string" else ""} \
        ~{if trunc_and_reload then "--trunc_and_reload" else ""} \
        ~{if ignore_existing_schema_mismatch then "--ignore_existing_schema_mismatch" else ""}
    >>>

    runtime {
        docker: docker_image
        memory: "~{memory_gb} GB"
        disks: "local-disk ~{disk_size_gb} HDD"
    }
}
