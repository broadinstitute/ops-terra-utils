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

    call ValidateInputs {
        input:
            input_metadata_tsv = input_metadata_tsv,
            billing_project = billing_project,
            workspace_name = workspace_name,
            terra_table_names = terra_table_names,
            docker_name = docker_name
    }

    call GenerateSchemaJson {
        input:
            # Set so this step only runs if the input is validated
            input_validated = ValidateInputs.validated,
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

task ValidateInputs {
    input {
        File? input_metadata_tsv
        String? billing_project
        String? workspace_name
        String? terra_table_names
        String docker_name
    }

    command <<<
    set -euo pipefail

    python3 <<CODE
    tsv = "~{input_metadata_tsv}"
    billing_project = "~{billing_project}"
    workspace_name = "~{workspace_name}"
    terra_table_names = "~{terra_table_names}"

    terra_params = [billing_project, workspace_name, terra_table_names]

    if tsv and any(terra_params):
        raise ValueError(
            "If the 'input_metadata_tsv' is provided, none of the terra parameters can also be provided. Please "
            "leave 'billing_project', 'workspace_name' and 'terra_table_name' all blank if providing a tsv as input."
        )
    elif not tsv and not all(terra_params):
        raise ValueError(
            "If using the Terra workspace table as input, the 'billing_project', 'workspace_name' and "
            "'terra_table_names' must ALL be provided"
        )
    if (tsv and not any(terra_params)) or (not tsv and all(terra_params)):
        print("Input parameters validated, continuing")

    CODE
    >>>

    runtime {
		docker: docker_name
	}

    output {
        Boolean validated = true
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
        Boolean input_validated
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
