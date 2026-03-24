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
    def summarize_symbol_indicators(
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        indicators: dict[str, dict[str, Any]] | None = None,
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Compute TA-Lib indicators for one symbol over a historical window and return the latest indicator snapshot."""
        try:
            return _success(
                "summarize_symbol_indicators",
                service.summarize_symbol_indicators(
                    symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    indicators=indicators,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("summarize_symbol_indicators", exc)


    @mcp.tool()
    def scan_indicator_threshold(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        indicator: str,
        value: float,
        operator: str = ">=",
        params: dict[str, Any] | None = None,
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Scan a symbol list for a TA-Lib indicator meeting a numeric threshold such as RSI >= 60."""
        try:
            return _success(
                "scan_indicator_threshold",
                service.scan_indicator_threshold(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    indicator=indicator,
                    params=params,
                    operator=operator,
                    value=value,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("scan_indicator_threshold", exc)


    @mcp.tool()
    def scan_indicator_crossover(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        fast_indicator: str,
        slow_indicator: str,
        fast_params: dict[str, Any] | None = None,
        slow_params: dict[str, Any] | None = None,
        direction: str = "bullish",
        lookback_bars: int = 5,
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Scan symbols for recent bullish or bearish crossovers such as SMA(20) over SMA(50) on 1d candles."""
        try:
            return _success(
                "scan_indicator_crossover",
                service.scan_indicator_crossover(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    fast_indicator=fast_indicator,
                    fast_params=fast_params,
                    slow_indicator=slow_indicator,
                    slow_params=slow_params,
                    direction=direction,
                    lookback_bars=lookback_bars,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("scan_indicator_crossover", exc)
