version 1.0

task CopySourceToDestFromMappingTsv {
    input {
        File mapping_file
    }

    command <<<
        set -euo pipefail

        # Define the path to the TSV file
        tsv_file=mapping_file

        while IFS=$'\t' read -r source_file dest_file; do
          gsutil cp "$source_file" "$dest_file"

        done < "$tsv_file"

    >>>

    runtime {
        docker: "gcr.io/gcp-runtimes/ubuntu_16_0_4:latest"
    }

}