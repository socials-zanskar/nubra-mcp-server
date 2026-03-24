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
    @mcp.tool()
    def get_holdings() -> dict[str, Any]:
        """Fetch the current holdings snapshot, including invested value, current value, haircut, margin benefit, and pledge-related fields."""
        try:
            return _success("get_holdings", service.get_holdings())
        except Exception as exc:
            return _failure("get_holdings", exc)


    @mcp.tool()
    def get_funds() -> dict[str, Any]:
        """Fetch the account funds and margin snapshot for read-only balance and utilization questions."""
        try:
            return _success("get_funds", service.get_funds())
        except Exception as exc:
            return _failure("get_funds", exc)
