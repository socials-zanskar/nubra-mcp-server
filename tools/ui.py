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
    def open_nubracollector_ui() -> dict[str, Any]:
        """Launch the NubraCollector desktop UI for live websocket collection and export workflows. If the current Nubra MCP session is valid, the UI reuses it automatically; otherwise the UI falls back to its own login flow."""
        try:
            return _success("open_nubracollector_ui", service.open_nubracollector_ui())
        except Exception as exc:
            return _failure("open_nubracollector_ui", exc)

    @mcp.tool()
    def open_backtest_ui() -> dict[str, Any]:
        """Launch the desktop backtest UI. It reuses the current Nubra MCP auth state and runs NubraOSS-style strategy backtests using Nubra historical data only."""
        try:
            return _success("open_backtest_ui", service.open_backtest_ui())
        except Exception as exc:
            return _failure("open_backtest_ui", exc)
