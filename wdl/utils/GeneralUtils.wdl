version 1.0

task ConvertToFile {
    input {
        String cloud_path
    }

    command <<<
        echo "Localized file path: ~{cloud_path}"
    >>>

    output {
        File localized_file = "~{cloud_path}"
    }
}
