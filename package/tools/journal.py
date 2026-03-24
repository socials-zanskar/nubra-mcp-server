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
    def generate_trade_journal_summary(
        date_from: str,
        date_to: str,
        group_by_tag: bool = True,
    ) -> dict[str, Any]:
        """Summarize executed orders over a date range, grouped by tag by default."""
        try:
            return _success(
                "generate_trade_journal_summary",
                service.generate_trade_journal_summary(
                    date_from=date_from,
                    date_to=date_to,
                    group_by_tag=group_by_tag,
                ),
            )
        except Exception as exc:
            return _failure("generate_trade_journal_summary", exc)
