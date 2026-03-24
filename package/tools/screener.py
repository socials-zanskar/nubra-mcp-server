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
    def resolve_instrument_smart(
        query: str,
        instrument_type: str | None = None,
        expiry: str | None = None,
        option_type: str | None = None,
        strike_price: float | None = None,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Resolve a symbol or option query using fuzzy matching plus optional expiry/strike filters."""
        try:
            return _success(
                "resolve_instrument_smart",
                service.resolve_instrument_smart(
                    query,
                    instrument_type=instrument_type,
                    expiry=expiry,
                    option_type=option_type,
                    strike_price=strike_price,
                    limit=limit,
                ),
            )
        except Exception as exc:
            return _failure("resolve_instrument_smart", exc)

    @mcp.tool()
    def scan_watchlist(
        symbols: list[str],
        scan_type: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run a reusable watchlist scan such as volume_spike, rsi_threshold, ema_crossover, oi_wall, or return_rank."""
        try:
            return _success("scan_watchlist", service.scan_watchlist(symbols=symbols, scan_type=scan_type, params=params))
        except Exception as exc:
            return _failure("scan_watchlist", exc)

    @mcp.tool()
    def get_historical_chart_summary(
        symbol: str,
        timeframe: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Summarize the chart structure over a historical window with trend, support, resistance, and volatility context."""
        try:
            return _success(
                "get_historical_chart_summary",
                service.get_historical_chart_summary(
                    symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("get_historical_chart_summary", exc)
