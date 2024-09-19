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

    formatted_args = [f"--{key[key.find('.')+1:]} {value} " for key, value in input_json.items()]
    print(''.join(formatted_args))
