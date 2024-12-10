task ConvertToFile {
    input {
        String cloud_path  # Cloud file path as a string
    }

    command <<<
        echo "Localized file path: ~{cloud_path}"
    >>>

    output {
        File localized_file = "~{cloud_path}"  # Automatically localizes it
    }
}
