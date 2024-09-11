import argparse


def hello_world(change_type, billing_project, workspace_name, move_or_copy_attachment, delete_attachment, metadata_attachment):
    print(f"Change type: {change_type}")
    print(f"Billing project: {billing_project}")
    print(f"workspace_name: {workspace_name}")
    print(f"move_or_copy_attachment: {move_or_copy_attachment}")
    print(f"delete_attachment: {delete_attachment}")
    print(f"metadata_attachment: {metadata_attachment}")
    return "hello"


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--change_type", choices=["delete", "transfer", "metadata"], required=False)
    parser.add_argument("-b", "--billing_project", required=False)
    parser.add_argument("-w", "--workspace_name", required=False)
    parser.add_argument("-d", "--delete_attachment", required=False)
    parser.add_argument("-a", "--move_or_copy_attachment", required=False)
    parser.add_argument("-m", "--metadata_attachment", required=False)
    args, _ = parser.parse_known_args()

    res = hello_world(
        change_type=args.change_type,
        billing_project=args.billing_project,
        workspace_name=args.workspace_name,
        delete_attachment=args.delete_attachment,
        move_or_copy_attachment=args.move_or_copy_attachment,
        metadata_attachment=args.metadata_attachment,
    )
    print(res)