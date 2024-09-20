import json
from argparse import ArgumentParser, Namespace


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Transform template input json to input arguments")
    parser.add_argument("-i", "--input_json", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    with open(args.input_json) as f:
        input_json = json.load(f)

    for arg in input_json:
        boolean_args = []
        value = arg.value.lower()
        if value in ['true', 'false']:
            if value == 'true':
                boolean_args.append(f"--{arg.key[arg.key.find('.')+1:]} ")
            input_json.pop(arg.key)

    non_boolean_args = [f"--{key[key.find('.')+1:]} {value} " for key, value in input_json.items()]
    formatted_args = boolean_args + non_boolean_args
    print(''.join(formatted_args))
