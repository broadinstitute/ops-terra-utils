"""
To run locally do python3 /path/to/script.py -a required_arg -t -b optional_arg -c choice1.
You may need to install the required packages with pip install -r requirements.txt. If attempting to run with Terra
will need to set up wdl pointing towards script.
"""
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.tdr_utils.tdr_ingest_utils import BatchIngest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.vars import GCP


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="description of script")
    parser.add_argument("--required_arg_a", "-a", required=True)
    parser.add_argument("--set_boolean", "-t", action="store_true", help="Set this flag to true")
    parser.add_argument("--optional_arg_b", "-b", default="default_value",
                        help="Optional argument with default value", required=False)
    parser.add_argument("--arg_c", "-c", choices=['choice1', 'choice2'],
                        help="arg with specific choices", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    # Get arguments
    required_arg_a = args.required_arg_a
    set_boolean = args.set_boolean
    optional_arg_b = args.optional_arg_b
    arg_c = args.arg_c

    logging.info("Collecting arguments and logging something")

    # Create token object. This gets your token for the API calls and auto refreshes when needed
    token = Token()
    # Create request object to make API calls and pass in token
    # Can optionally pass in max_retries and max_backoff_time to control retries and backoff time.
    # Defaults to 5 retries and 5 minutes max backoff if not supplied
    request_util = RunRequest(token=token, max_retries=5, max_backoff_time=60)
    # Create TDR and Terra objects to interact with the TDR and Terra with the request_util object
    tdr = TDR(request_util=request_util)
    terra = TerraWorkspace(
        request_util=request_util,
        billing_project="some_billing_project",
        workspace_name="some_workspace_name"
    )

    # You can now use tdr or terra objects to interact with the TDR or Terra like below
    workspace_bucket = terra.get_workspace_bucket()
    dataset_files = tdr.get_dataset_files(dataset_id="some_dataset_uuid")

    # There is also more utils located in /python/utils/ that you can use, such as the GCP object,
    # azure, csv as well as more specific terra and tdr ones in their respective folders
    # Example of using tdr with other class to batch ingest data to
    ingest_metrics = [
        {'header-1': 'value1', 'header-2': 'value2'},
        {'header-1': 'value3', 'header-2': 'value4'}
    ]
    BatchIngest(
        ingest_metadata=ingest_metrics,
        tdr=tdr,
        target_table_name="some_table",
        dataset_id="some_dataset_uuid",
        batch_size=1000,
        bulk_mode=True,
    ).run()
