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
    def get_portfolio_sector_exposure(
        include_holdings: bool = True,
        include_positions: bool = True,
    ) -> dict[str, Any]:
        """Group current portfolio exposure by the closest available sector-like classification from Nubra portfolio data."""
        try:
            return _success(
                "get_portfolio_sector_exposure",
                service.get_portfolio_sector_exposure(
                    include_holdings=include_holdings,
                    include_positions=include_positions,
                ),
            )
        except Exception as exc:
            return _failure("get_portfolio_sector_exposure", exc)

    @mcp.tool()
    def get_portfolio_concentration_risk(top_n: int = 5) -> dict[str, Any]:
        """Measure single-name concentration and HHI concentration risk across the current portfolio snapshot."""
        try:
            return _success("get_portfolio_concentration_risk", service.get_portfolio_concentration_risk(top_n=top_n))
        except Exception as exc:
            return _failure("get_portfolio_concentration_risk", exc)

    @mcp.tool()
    def get_portfolio_rolling_drawdown(
        timeframe: str = "1d",
        start_date: str = "",
        end_date: str = "",
        include_holdings: bool = True,
        include_positions: bool = True,
    ) -> dict[str, Any]:
        """Build a historical drawdown series for the current portfolio using current quantities applied across Nubra historical closes."""
        try:
            return _success(
                "get_portfolio_rolling_drawdown",
                service.get_portfolio_rolling_drawdown(
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    include_holdings=include_holdings,
                    include_positions=include_positions,
                ),
            )
        except Exception as exc:
            return _failure("get_portfolio_rolling_drawdown", exc)

    @mcp.tool()
    def get_portfolio_correlation_matrix(
        timeframe: str = "1d",
        start_date: str = "",
        end_date: str = "",
        include_holdings: bool = True,
        include_positions: bool = True,
    ) -> dict[str, Any]:
        """Compute a return-correlation matrix for current portfolio constituents using Nubra historical closes."""
        try:
            return _success(
                "get_portfolio_correlation_matrix",
                service.get_portfolio_correlation_matrix(
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    include_holdings=include_holdings,
                    include_positions=include_positions,
                ),
            )
        except Exception as exc:
            return _failure("get_portfolio_correlation_matrix", exc)

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
