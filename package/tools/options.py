from __future__ import annotations

from typing import Any

from nubra_client import NubraAPIError, NubraService


def _success(tool: str, data: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "tool": tool, "data": data}


def _failure(tool: str, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, NubraAPIError):
        return {
            "ok": False,
            "tool": tool,
            "error": {
                "message": str(exc),
                "status_code": exc.status_code,
                "details": exc.details,
            },
        }
    return {"ok": False, "tool": tool, "error": {"message": str(exc)}}


def register(mcp: Any, service: NubraService) -> None:
    exchange = "NSE"

    @mcp.tool()
    def get_option_chain(symbol: str, expiry: str | None = None) -> dict[str, Any]:
        """Get the full Nubra NSE option chain snapshot, including IV, Greeks, OI, and volume."""
        try:
            return _success("get_option_chain", service.option_chain(symbol, exchange=exchange, expiry=expiry))
        except Exception as exc:
            return _failure("get_option_chain", exc)
