version 1.0

workflow GetTDRSchemaJson {
    input {
        File? input_metadata_tsv
        String? billing_project
        String? workspace_name
        String? terra_table_names
        Boolean force_disparate_rows_to_string
        String? docker
    }
    String docker_name = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call GenerateSchemaJson {
        input:
            input_metadata_tsv = input_metadata_tsv,
            billing_project = billing_project,
            workspace_name = workspace_name,
            terra_table_names = terra_table_names,
            docker_name = docker_name,
            force_disparate_rows_to_string = force_disparate_rows_to_string
    }

    output {
        File tdr_schema_json = GenerateSchemaJson.tdr_schema_json
    }
}

task GenerateSchemaJson {
    input {
        File? input_metadata_tsv
        String? billing_project
        String? workspace_name
        String? terra_table_names
        String docker_name
        Boolean force_disparate_rows_to_string
    }

    command <<<
        python /etc/terra_utils/python/generate_tdr_schema_json.py \
            ~{"--input_tsv " + input_metadata_tsv} \
            ~{"--billing_project " + billing_project} \
            ~{"--workspace_name " + workspace_name} \
            ~{"--terra_table_names " + terra_table_names} \
            ~{if force_disparate_rows_to_string then "--force_disparate_rows_to_string" else ""}
    >>>

    output {
        File tdr_schema_json = "schema.json"
    }

    runtime {
		docker: docker_name
	}
}
