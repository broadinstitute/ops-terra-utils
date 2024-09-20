version 1.0

workflow TestPythonScript {
    input {
        String script_name
        File input_json
    }
    call FormatArgs {
        input: input_json=input_json
    }

    call RunPythonScript {
        input: script_name=script_name, script_args=FormatArgs.formatted_args
    }
        }


task FormatArgs {
    input {
        File input_json
    }

    command <<<
        python /etc/local_repo/dev/transform_json_to_input_args.py -i ~{input_json}
    >>>

    output{
        String formatted_args = read_string(stdout())
    }
}


task RunPythonScript {
    input {
        String script_name
        String script_args
    }

    command <<<
        python /etc/local_repo/python/~{script_name} ~{script_args}
    >>>

}
