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
    def get_instrument_details(symbol: str) -> dict[str, Any]:
        """Use this when a user asks for ref_id, tick size, lot size, nubra name, or NSE instrument details for a stock, future, or option."""
        try:
            return _success("get_instrument_details", service.get_instrument_details(symbol, exchange=exchange))
        except Exception as exc:
            return _failure("get_instrument_details", exc)


    @mcp.tool()
    def find_instruments(
        symbol: str | None = None,
        asset: str | None = None,
        derivative_type: str | None = None,
        option_type: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Search the instruments master when a user wants matching tradable instruments or needs multiple results."""
        try:
            return _success(
                "find_instruments",
                service.find_instruments(
                    exchange=exchange,
                    symbol=symbol,
                    asset=asset,
                    derivative_type=derivative_type,
                    option_type=option_type,
                    limit=limit,
                ),
            )
        except Exception as exc:
            return _failure("find_instruments", exc)


    @mcp.tool()
    def find_index_details(
        query: str,
        limit: int = 10,
        instrument_limit: int = 10,
    ) -> dict[str, Any]:
        """Search Nubra's public index master first, then compare the closest index rows against the exchange instrument master."""
        try:
            return _success(
                "find_index_details",
                service.find_index_details(
                    query,
                    exchange=exchange,
                    limit=limit,
                    instrument_limit=instrument_limit,
                ),
            )
        except Exception as exc:
            return _failure("find_index_details", exc)


    @mcp.tool()
    def get_quote(symbol: str, levels: int = 5) -> dict[str, Any]:
        """Get the latest NSE order-book quote for a cash, futures, or options symbol."""
        try:
            return _success("get_quote", service.quote_by_symbol(symbol, exchange=exchange, levels=levels))
        except Exception as exc:
            return _failure("get_quote", exc)


    @mcp.tool()
    def get_current_price(symbol: str) -> dict[str, Any]:
        """Get the latest NSE current-price snapshot for a stock, index, or option symbol without requiring order-book depth."""
        try:
            return _success("get_current_price", service.current_price_by_symbol(symbol, exchange=exchange))
        except Exception as exc:
            return _failure("get_current_price", exc)


    @mcp.tool()
    def get_yesterday_change(symbol: str) -> dict[str, Any]:
        """Use this when a user asks how much an NSE stock, index, or option changed over yesterday or from the previous close."""
        try:
            return _success("get_yesterday_change", service.yesterday_change(symbol, exchange=exchange))
        except Exception as exc:
            return _failure("get_yesterday_change", exc)


    @mcp.tool()
    def get_historical_data(
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str = "INDEX",
        intraday: bool | None = None,
    ) -> dict[str, Any]:
        """Fetch structured OHLCV and analytics fields over a UTC window. Allowed intervals: 1m, 2m, 3m, 5m, 15m, 30m, 1h, 1d, 1w, 1mt. Here m=minute, h=hour, d=day, w=week, mt=month."""
        try:
            payload = service.historical_data(
                symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                instrument_type=instrument_type,
                intraday=intraday,
            )
            return _success("get_historical_data", payload)
        except Exception as exc:
            return _failure("get_historical_data", exc)
