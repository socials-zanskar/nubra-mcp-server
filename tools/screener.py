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
        """Run a reusable watchlist scan such as volume_spike, volume_breakout, rsi_threshold, ema_crossover, oi_wall, or return_rank."""
        try:
            return _success("scan_watchlist", service.scan_watchlist(symbols=symbols, scan_type=scan_type, params=params))
        except Exception as exc:
            return _failure("scan_watchlist", exc)

    @mcp.tool()
    def find_gap_up_candidates(
        symbols: list[str],
        timeframe: str = "1d",
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        min_gap_pct: float = 1.0,
        require_green_candle: bool = False,
    ) -> dict[str, Any]:
        """Find symbols opening above the previous close by a configurable percentage using Nubra historical data."""
        try:
            return _success(
                "find_gap_up_candidates",
                service.find_gap_up_candidates(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    min_gap_pct=min_gap_pct,
                    require_green_candle=require_green_candle,
                ),
            )
        except Exception as exc:
            return _failure("find_gap_up_candidates", exc)

    @mcp.tool()
    def find_unusual_volume(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "STOCK",
        lookback_bars: int = 20,
        min_spike_ratio: float = 1.5,
        min_zscore: float = 2.0,
    ) -> dict[str, Any]:
        """Find symbols whose latest volume is unusual relative to recent history using both ratio and z-score checks."""
        try:
            return _success(
                "find_unusual_volume",
                service.find_unusual_volume(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    lookback_bars=lookback_bars,
                    min_spike_ratio=min_spike_ratio,
                    min_zscore=min_zscore,
                ),
            )
        except Exception as exc:
            return _failure("find_unusual_volume", exc)

    @mcp.tool()
    def find_oi_build_up(
        symbols: list[str],
        expiry: str | None = None,
        min_combined_oi_change_pct: float = 5.0,
    ) -> dict[str, Any]:
        """Screen underlyings for ATM option open-interest build-up using current Nubra option-chain OI versus previous OI."""
        try:
            return _success(
                "find_oi_build_up",
                service.find_oi_build_up(
                    symbols,
                    expiry=expiry,
                    min_combined_oi_change_pct=min_combined_oi_change_pct,
                ),
            )
        except Exception as exc:
            return _failure("find_oi_build_up", exc)

    @mcp.tool()
    def find_iv_expansion(
        symbols: list[str],
        timeframe: str = "1d",
        start_date: str = "",
        end_date: str = "",
        lookback_bars: int = 10,
        min_expansion_ratio: float = 1.1,
    ) -> dict[str, Any]:
        """Screen symbols for ATM implied-volatility expansion using Nubra option historical data."""
        try:
            return _success(
                "find_iv_expansion",
                service.find_iv_expansion(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    lookback_bars=lookback_bars,
                    min_expansion_ratio=min_expansion_ratio,
                ),
            )
        except Exception as exc:
            return _failure("find_iv_expansion", exc)

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
