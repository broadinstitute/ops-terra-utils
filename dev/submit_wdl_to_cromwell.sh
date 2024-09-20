#!/bin/bash

# Argument checking
if [[ !($# -ne 3 || $# -ne 1) ]]; then
	echo " $# "
    echo "Usage: $0 <submit> <script_name> <input_json>"
	echo "Usage: $0 <monitor>"
    exit 1
fi

ACTION=$1
PYTHON_SCRIPT=$2
INPUT_JSON=$3
JSON_OUPUT_PATH=dev/wrapper_input.json

if [[ "$ACTION" =~ ^(monitor|submit)$ ]]; then
	if [[ "$ACTION" == "submit" ]]; then
		WRAPPER_JSON=$(jq -n \
							--arg TestPythonScript.script_name "$PYTHON_SCRIPT" \
							--arg TestPythonScript.input_json "/etc/local_repo/$INPUT_JSON" \
							'$ARGS.named' )

		echo "$WRAPPER_JSON" > "$JSON_OUPUT_PATH"

		CROMSHELL_JOB_SUBMISSION="$(cromshell --no_turtle -mc submit dev/Wrapper.wdl $JSON_OUPUT_PATH)"
		JOB_ID="$(echo "$CROMSHELL_JOB_SUBMISSION" | jq -r '.id')"
		SUBMISSION_STATUS="$(echo "$CROMSHELL_JOB_SUBMISSION" | jq -r '.status')"

		if [[ "$SUBMISSION_STATUS" == "Submitted" ]]; then
			echo "Job submitted successfully. Job ID: ${JOB_ID}"
			echo "You can check the status of the job by running: cromshell status ${JOB_ID}"
			echo "{\"job_id\": \"${JOB_ID}\"}" > .job_id
		else
			echo "Submission status is $SUBMISSION_STATUS"
			echo "Job submission failed. Please review command output."
		fi

		# Clean up after submission
		rm dev/wrapper_input.json
	fi
	if [[ "$ACTION" == "monitor" ]]; then
		JOBID=$(cat .job_id | jq -r '.job_id')
		echo "Checking status of job $JOBID"
		cromshell status $JOBID
	fi
	else :
		echo "Invalid action. Please use 'submit' or 'monitor'."
		exit 1
fi
