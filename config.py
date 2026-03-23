from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


def _load_repo_env_file(filename: str = ".env") -> None:
    env_path = Path(__file__).resolve().parent / filename
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_repo_env_file()


@dataclass(slots=True)
class Settings:
    phone: str = ""
    mpin: str = ""
    environment: str = "PROD"
    default_exchange: str = "NSE"
    log_level: str = "INFO"
    host: str = "127.0.0.1"
    port: int = 8000
    mcp_path: str = "/mcp"
    auth_state_file: str = "auth_state.json"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            phone=os.getenv("PHONE", "").strip(),
            mpin=os.getenv("MPIN", "").strip(),
            environment=os.getenv("NUBRA_ENV", "PROD").strip().upper(),
            default_exchange=os.getenv("NUBRA_DEFAULT_EXCHANGE", "NSE").strip().upper(),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            host=os.getenv("HOST", "127.0.0.1").strip(),
            port=int(os.getenv("PORT", "8000")),
            mcp_path=os.getenv("MCP_PATH", "/mcp").strip(),
            auth_state_file=os.getenv("AUTH_STATE_FILE", "auth_state.json").strip(),
        )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
