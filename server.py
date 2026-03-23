from __future__ import annotations

r"""
Run locally:

1. Create and activate a Python 3.11 virtualenv.
2. Install dependencies:
   pip install -r requirements.txt
3. Export environment variables:
   Optional overrides for hardcoded defaults:
   PHONE, MPIN, NUBRA_ENV, PORT, HOST, MCP_PATH
4. Start the HTTP server:
   python server.py --transport streamable-http
5. Or start stdio transport for local MCP clients:
   python server.py --transport stdio

HTTP endpoints:
- GET /health
- MCP streamable HTTP endpoint mounted at /mcp by default

Useful local helpers:
- PowerShell health check: `.\check_health.ps1`
- Example MCP client config: `mcp-client-config.example.json`
"""

import argparse
import logging
from typing import Any

import uvicorn
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP

from config import Settings, configure_logging
from nubra_client import NubraClient, NubraService
from tools import account, analytics, auth, options, orders, quotes, talib_tools

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Production-grade Nubra MCP server")
    parser.add_argument("--transport", choices=["stdio", "streamable-http", "sse"], default="streamable-http")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--mcp-path", default=None)
    return parser


def register_tools(mcp: FastMCP, service: NubraService) -> None:
    auth.register(mcp, service)
    quotes.register(mcp, service)
    options.register(mcp, service)
    account.register(mcp, service)
    analytics.register(mcp, service)
    talib_tools.register(mcp, service)
    orders.register(mcp, service)


def create_app(settings: Settings, mcp: FastMCP) -> FastAPI:
    app = FastAPI(title="Nubra MCP Server", version="1.0.0")

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "name": "nubra-mcp-server",
            "status": "ok",
            "transport": "streamable-http",
            "mcp_path": settings.mcp_path,
        }

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "environment": settings.environment,
            "auth_mode": "interactive_otp_mpin",
            "trading_enabled": False,
            "mcp_path": settings.mcp_path,
        }

    app.mount(settings.mcp_path, mcp.streamable_http_app())
    return app


def main() -> None:
    args = build_parser().parse_args()
    settings = Settings.from_env()

    if args.host:
        settings.host = args.host
    if args.port:
        settings.port = args.port
    if args.mcp_path:
        settings.mcp_path = args.mcp_path

    configure_logging(settings.log_level)

    logger.info("Starting Nubra MCP server in %s using SDK TOTP auto-login", settings.environment)
    client = NubraClient(settings)
    service = NubraService(client)

    mcp = FastMCP(
        "nubra-mcp-server",
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.mcp_path,
    )
    register_tools(mcp, service)

    if args.transport == "stdio":
        mcp.run(transport="stdio")
        return

    if args.transport == "sse":
        mcp.run(transport="sse", mount_path=settings.mcp_path)
        return

    app = create_app(settings, mcp)
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
