from __future__ import annotations

import sys
from pathlib import Path

from config import DEFAULT_APP_HOME, ensure_user_env_file
from server import main as server_main


def _print_init_instructions(env_path: Path) -> None:
    print(f"Created or verified local Nubra MCP config at: {env_path}")
    print("Next steps:")
    print("1. Fill in PHONE and MPIN in that file if you want preconfigured auth defaults.")
    print("2. Keep NUBRA_ENV=UAT while validating trading tools.")
    print("3. In Codex, register a stdio MCP server with command: nubra-mcp")
    print("4. You can also skip preconfig and enter phone, OTP, and MPIN interactively in chat.")


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "init":
        force = "--force" in args[1:]
        env_path = ensure_user_env_file(force=force)
        _print_init_instructions(env_path)
        return

    if args and args[0] == "serve":
        args = args[1:]
    elif not args:
        args = ["--transport", "stdio"]

    server_main(args)


if __name__ == "__main__":
    main()
