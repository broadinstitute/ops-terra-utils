"""Script to add or remove a user from a Terra group."""

from argparse import ArgumentParser, Namespace
from ops_utils.terra_util import TerraGroups
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

ADD = "add"
REMOVE = "remove"
MEMBER = "member"
ADMIN = "admin"


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("-g", "--group_name", type=str, required=True)
    parser.add_argument("-u", "--user_email", type=str, required=True)
    parser.add_argument("-a", "--action", choices=[ADD, REMOVE], required=True)
    parser.add_argument("-r", "--role", choices=[MEMBER, ADMIN], required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    group_name = args.group_name
    user_email = args.user_email
    action = args.action
    role = args.role

    token = Token()
    request_util = RunRequest(token=token)
    terra_groups = TerraGroups(request_util=request_util)

    if action == ADD:
        terra_groups.add_user_to_group(group=group_name, email=user_email, role=role, continue_if_exists=True)
        logging.info(f"Added {user_email} to {group_name} with {role} role.")
    elif action == 'remove':
        terra_groups.remove_user_from_group(group=group_name, email=user_email, role=role)
        logging.info(f"Removed {user_email} from {group_name}.")
