import json
from argparse import ArgumentParser, Namespace


def get_args() -> Namespace:
    parser = ArgumentParser(description="Transform template input json to input arguments")
    parser.add_argument("-i", "--input_json", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    with open(args.input_json) as f:
        input_json = json.load(f)

    formatted_args = []
    for k, v in input_json.items():
        param_name = k.split(".")[1]
        if v in ["true", "false"]:
            formatted_args.append(f"--{param_name}")
        else:
            formatted_args.append(f"--{param_name} {v}")
    print(" ".join(formatted_args))
