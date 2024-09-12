version 1.0

workflow CopyGCPSourceToDestinationFromMappingTsv {
    input {
        File mapping_tsv
    }

    Array[Array[String]] mapping_info = read_tsv(mapping_tsv)

    scatter (line in mapping_info) {
        call CopyGCPFiles {
            input:
                source_file_path = line[0]
                destination_file_path = line[1]
        }
    }

}

task CopyGCPFiles {
    input {
        String source_file_path
        String destination_file_path
    }

    command <<<
        gsutil cp source_file_path destination_file_path
    >>>

    runtime {
        docker: "gcr.io/gcp-runtimes/ubuntu_16_0_4:latest"
    }

}
