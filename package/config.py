from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_APP_HOME = Path(os.getenv("NUBRA_MCP_HOME", "")).expanduser() if os.getenv("NUBRA_MCP_HOME") else Path.home() / ".nubra-mcp"
CWD_ROOT = Path.cwd()


def _load_env_file(path: Path, *, original_env_keys: set[str]) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key or key in original_env_keys:
            continue
        os.environ[key] = value


def _detect_config_root() -> Path:
    repo_env = REPO_ROOT / ".env"
    cwd_env = CWD_ROOT / ".env"
    if repo_env.exists():
        return REPO_ROOT
    if cwd_env.exists():
        return CWD_ROOT
    return DEFAULT_APP_HOME


def _load_known_env_files() -> None:
    original_env_keys = set(os.environ)
    _load_env_file(DEFAULT_APP_HOME / ".env", original_env_keys=original_env_keys)
    if CWD_ROOT != DEFAULT_APP_HOME:
        _load_env_file(CWD_ROOT / ".env", original_env_keys=original_env_keys)
    if REPO_ROOT not in {DEFAULT_APP_HOME, CWD_ROOT}:
        _load_env_file(REPO_ROOT / ".env", original_env_keys=original_env_keys)


_load_known_env_files()


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

    @property
    def auth_state_path(self) -> Path:
        raw_path = Path(self.auth_state_file)
        if raw_path.is_absolute():
            return raw_path
        return _detect_config_root() / raw_path


def ensure_user_env_file(force: bool = False) -> Path:
    app_home = DEFAULT_APP_HOME
    app_home.mkdir(parents=True, exist_ok=True)
    env_path = app_home / ".env"
    if env_path.exists() and not force:
        return env_path

    example_path = REPO_ROOT / ".env.example"
    if example_path.exists():
        env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        env_path.write_text(
            "PHONE=\nMPIN=\nNUBRA_ENV=UAT\nNUBRA_DEFAULT_EXCHANGE=NSE\nLOG_LEVEL=INFO\nHOST=127.0.0.1\nPORT=8000\nMCP_PATH=/mcp\nAUTH_STATE_FILE=auth_state.json\n",
            encoding="utf-8",
        )
    return env_path


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
