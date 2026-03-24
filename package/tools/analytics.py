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
    def find_delta_neutral_pairs(
        symbol: str,
        expiry: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        """Find CE/PE combinations whose combined delta is closest to zero for the selected underlying."""
        try:
            return _success(
                "find_delta_neutral_pairs",
                service.find_delta_neutral_pairs(symbol, exchange=exchange, expiry=expiry, top_k=top_k),
            )
        except Exception as exc:
            return _failure("find_delta_neutral_pairs", exc)


    @mcp.tool()
    def calculate_option_greeks(
        symbol: str,
        expiry: str | None = None,
    ) -> dict[str, Any]:
        """Return strike-wise option Greeks from the Nubra option chain snapshot for analytics workflows."""
        try:
            return _success("calculate_option_greeks", service.calculate_option_greeks(symbol, exchange=exchange, expiry=expiry))
        except Exception as exc:
            return _failure("calculate_option_greeks", exc)


    @mcp.tool()
    def find_symbols_with_rising_greeks(
        symbols: list[str],
        timeframe: str = "5m",
        start_date: str | None = None,
        end_date: str | None = None,
        instrument_type: str = "STOCK",
        intraday: bool = True,
    ) -> dict[str, Any]:
        """For a list of symbols, use ATM option historical Greeks to identify rising delta and vega over a UTC window. If dates are omitted, the service uses a recent default window."""
        try:
            return _success(
                "find_symbols_with_rising_greeks",
                service.find_symbols_with_rising_greeks(
                    symbols=symbols,
                    timeframe=timeframe,
                    start_date=start_date or "",
                    end_date=end_date or "",
                    exchange=exchange,
                    instrument_type=instrument_type,
                    intraday=intraday,
                ),
            )
        except Exception as exc:
            return _failure("find_symbols_with_rising_greeks", exc)


    @mcp.tool()
    def analyze_option_greek_changes(
        symbols: list[str],
        greek: str = "vega",
        timeframe: str = "5m",
        start_date: str | None = None,
        end_date: str | None = None,
        baseline: str = "open",
        compare_to: str = "latest",
        intraday: bool = True,
    ) -> dict[str, Any]:
        """Use this for intraday options greek-change questions on stocks. If dates are omitted, the service uses a recent default window."""
        try:
            return _success(
                "analyze_option_greek_changes",
                service.analyze_option_greek_changes(
                    symbols=symbols,
                    greek=greek,
                    timeframe=timeframe,
                    start_date=start_date or "",
                    end_date=end_date or "",
                    baseline=baseline,
                    compare_to=compare_to,
                    exchange=exchange,
                    intraday=intraday,
                ),
            )
        except Exception as exc:
            return _failure("analyze_option_greek_changes", exc)


    @mcp.tool()
    def compare_symbols_performance(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Compare a symbol list over a historical window and rank by percentage return."""
        try:
            return _success(
                "compare_symbols_performance",
                service.compare_symbols_performance(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=exchange,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("compare_symbols_performance", exc)


    @mcp.tool()
    def rank_symbols_by_return(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        """Rank symbols by return over a requested historical window."""
        try:
            return _success(
                "rank_symbols_by_return",
                service.rank_symbols_by_return(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=exchange,
                    instrument_type=instrument_type,
                ),
            )
        except Exception as exc:
            return _failure("rank_symbols_by_return", exc)


    @mcp.tool()
    def find_volume_spikes(
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "STOCK",
        lookback_bars: int = 20,
        min_spike_ratio: float = 1.5,
    ) -> dict[str, Any]:
        """Find symbols whose latest volume is materially above their recent average volume."""
        try:
            return _success(
                "find_volume_spikes",
                service.find_volume_spikes(
                    symbols,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=exchange,
                    instrument_type=instrument_type,
                    lookback_bars=lookback_bars,
                    min_spike_ratio=min_spike_ratio,
                ),
            )
        except Exception as exc:
            return _failure("find_volume_spikes", exc)


    @mcp.tool()
    def summarize_option_chain(
        symbol: str,
        expiry: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        """Summarize an option chain with ATM context and the top call and put OI strikes."""
        try:
            return _success(
                "summarize_option_chain",
                service.summarize_option_chain(symbol, exchange=exchange, expiry=expiry, top_k=top_k),
            )
        except Exception as exc:
            return _failure("summarize_option_chain", exc)


    @mcp.tool()
    def find_oi_walls(
        symbols: list[str],
        expiry: str | None = None,
        top_k: int = 3,
        max_distance_pct: float = 2.5,
    ) -> dict[str, Any]:
        """Find symbols whose current price is near high-open-interest call or put walls in the option chain."""
        try:
            return _success(
                "find_oi_walls",
                service.find_oi_walls(
                    symbols,
                    exchange=exchange,
                    expiry=expiry,
                    top_k=top_k,
                    max_distance_pct=max_distance_pct,
                ),
            )
        except Exception as exc:
            return _failure("find_oi_walls", exc)
