version 1.0

task GcloudRsync {
    input {
        String source
        String destination
        String? exclude_regex
    }

    command <<<
        # Note that this is a Python regular expression, not a pure wildcard pattern. For example,
        # matching a string ending in "abc" is .*abc$ rather than *abc. Also note that the exclude path is relative,
        # as opposed to absolute (similar to Linux rsync and tar exclude options).
        gcloud storage rsync --recursive ~{source} ~{destination} ~{"--exclude=\"" + exclude_regex + "\""}
    >>>

    runtime {
        docker: "google/cloud-sdk:latest"
    }

}
