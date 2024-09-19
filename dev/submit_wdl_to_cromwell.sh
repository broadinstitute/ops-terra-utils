#!/bin/bash

# Argument checking
if [ $# -ne 2 ]
then
    echo "Usage: $0 <script_name> <input_json>"
    exit 1
fi

PYTHON_SCRIPT=$1
INPUT_JSON=$2
JSON_OUPUT_PATH=dev/wrapper_input.json

WRAPPER_JSON=$(jq -n \
					--arg TestPythonScript.script_name "$PYTHON_SCRIPT" \
					--arg TestPythonScript.input_json "/etc/local_repo/$INPUT_JSON" \
					'$ARGS.named' )

echo "$WRAPPER_JSON" > "$JSON_OUPUT_PATH"

CROMSHELL_JOB_SUBMISSION="$(cromshell --no_turtle -mc submit dev/wrapper_wdl.wdl $JSON_OUPUT_PATH)"
JOB_ID="$(echo "$CROMSHELL_JOB_SUBMISSION" | jq -r '.id')"
SUBMISSION_STATUS="$(echo "$CROMSHELL_JOB_SUBMISSION" | jq -r '.status')"

if [[ "$SUBMISSION_STATUS" == "Submitted" ]]; then
	echo "Job submitted successfully. Job ID: ${JOB_ID}"
	echo "You can check the status of the job by running: cromshell status ${JOB_ID}"
else
	echo "Submission status is $SUBMISSION_STATUS"
	echo "Job submission failed. Please review command output."
fi

# Clean up after submission
rm dev/wrapper_input.json
