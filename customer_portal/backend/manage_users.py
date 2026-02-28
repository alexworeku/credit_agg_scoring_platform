from __future__ import annotations

import argparse
import getpass

from customer_portal.backend.auth import create_user, list_users


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage portal login users.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_cmd = subparsers.add_parser("create", help="Create a new portal login user")
    create_cmd.add_argument("--username", required=True)
    create_cmd.add_argument("--customer-id", required=True, type=int)
    create_cmd.add_argument("--full-name", default=None)
    create_cmd.add_argument(
        "--password",
        default=None,
        help="Password (omit to be prompted securely)",
    )

    subparsers.add_parser("list", help="List portal users")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "list":
        users = list_users()
        if not users:
            print("No users found.")
            return
        for user in users:
            print(
                f"user_id={user.user_id} username={user.username} "
                f"customer_id={user.customer_id} active={user.is_active} full_name={user.full_name}"
            )
        return

    if args.command == "create":
        password = args.password or getpass.getpass("Password: ")
        user = create_user(
            username=args.username,
            password=password,
            customer_id=args.customer_id,
            full_name=args.full_name,
            is_active=True,
        )
        print(
            f"Created user_id={user.user_id} username={user.username} "
            f"customer_id={user.customer_id}"
        )


if __name__ == "__main__":
    main()

