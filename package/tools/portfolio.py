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
    def get_portfolio_summary(
        include_holdings: bool = True,
        include_positions: bool = True,
        include_funds: bool = True,
    ) -> dict[str, Any]:
        """Get a one-shot account overview with top gainers, losers, largest exposures, and margin/risk flags."""
        try:
            return _success(
                "get_portfolio_summary",
                service.get_portfolio_summary(
                    include_holdings=include_holdings,
                    include_positions=include_positions,
                    include_funds=include_funds,
                ),
            )
        except Exception as exc:
            return _failure("get_portfolio_summary", exc)

    @mcp.tool()
    def get_top_exposures(limit: int = 5, group_by: str = "symbol") -> dict[str, Any]:
        """Rank the account's largest exposures grouped by symbol, asset_type, or product."""
        try:
            return _success("get_top_exposures", service.get_top_exposures(limit=limit, group_by=group_by))
        except Exception as exc:
            return _failure("get_top_exposures", exc)

    @mcp.tool()
    def get_account_health_report(
        include_margin: bool = True,
        include_pledgeable_holdings: bool = True,
    ) -> dict[str, Any]:
        """Summarize account health with funds, margin pressure, MTM, and pledgeable holdings."""
        try:
            return _success(
                "get_account_health_report",
                service.get_account_health_report(
                    include_margin=include_margin,
                    include_pledgeable_holdings=include_pledgeable_holdings,
                ),
            )
        except Exception as exc:
            return _failure("get_account_health_report", exc)

    @mcp.tool()
    def generate_portfolio_report() -> dict[str, Any]:
        """Generate a structured portfolio analysis report. No input arguments are required; the tool uses the authenticated account snapshot."""
        try:
            return _success("generate_portfolio_report", service.generate_portfolio_report())
        except Exception as exc:
            return _failure("generate_portfolio_report", exc)

    @mcp.tool()
    def export_portfolio_report_html() -> dict[str, Any]:
        """Generate a portfolio analysis report, save it as a local HTML file, and return file metadata for a fast clickable link in chat. No input arguments are required."""
        try:
            return _success("export_portfolio_report_html", service.export_portfolio_report_html())
        except Exception as exc:
            return _failure("export_portfolio_report_html", exc)

    @mcp.tool()
    def export_portfolio_report_image() -> dict[str, Any]:
        """Generate a visual portfolio dashboard, save it as a local PNG file, and return file metadata for a fast clickable link in chat. No input arguments are required."""
        try:
            return _success("export_portfolio_report_image", service.export_portfolio_report_image())
        except Exception as exc:
            return _failure("export_portfolio_report_image", exc)
