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
    def run_backtest(
        symbol: str,
        timeframe: str,
        strategy_type: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        strategy_params: dict[str, Any] | None = None,
        initial_cash: float = 100000.0,
        fees: float = 0.001,
    ) -> dict[str, Any]:
        """Run a backtest. Required inputs: symbol, timeframe, strategy_type. Supported strategy_type values are ma_crossover and rsi. Optional strategy_params examples: {'fast_window': 20, 'slow_window': 50} or {'rsi_window': 14, 'oversold': 30, 'overbought': 70}."""
        try:
            return _success(
                "run_backtest",
                service.run_backtest(
                    symbol,
                    timeframe=timeframe,
                    strategy_type=strategy_type,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    strategy_params=strategy_params,
                    initial_cash=initial_cash,
                    fees=fees,
                ),
            )
        except Exception as exc:
            return _failure("run_backtest", exc)

    @mcp.tool()
    def run_ma_crossover_backtest(
        symbol: str,
        timeframe: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        fast_window: int = 20,
        slow_window: int = 50,
        initial_cash: float = 100000.0,
        fees: float = 0.001,
    ) -> dict[str, Any]:
        """Run a moving-average crossover backtest. Required inputs: symbol and timeframe. Optional inputs: start_date, end_date, fast_window, slow_window, initial_cash, fees."""
        try:
            return _success(
                "run_ma_crossover_backtest",
                service.run_ma_crossover_backtest(
                    symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    fast_window=fast_window,
                    slow_window=slow_window,
                    initial_cash=initial_cash,
                    fees=fees,
                ),
            )
        except Exception as exc:
            return _failure("run_ma_crossover_backtest", exc)

    @mcp.tool()
    def run_rsi_backtest(
        symbol: str,
        timeframe: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        rsi_window: int = 14,
        oversold: float = 30.0,
        overbought: float = 70.0,
        initial_cash: float = 100000.0,
        fees: float = 0.001,
    ) -> dict[str, Any]:
        """Run an RSI-based backtest. Required inputs: symbol and timeframe. Optional inputs: start_date, end_date, rsi_window, oversold, overbought, initial_cash, fees."""
        try:
            return _success(
                "run_rsi_backtest",
                service.run_rsi_backtest(
                    symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    rsi_window=rsi_window,
                    oversold=oversold,
                    overbought=overbought,
                    initial_cash=initial_cash,
                    fees=fees,
                ),
            )
        except Exception as exc:
            return _failure("run_rsi_backtest", exc)

    @mcp.tool()
    def export_backtest_report_html(
        symbol: str,
        timeframe: str,
        strategy_type: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        strategy_params: dict[str, Any] | None = None,
        initial_cash: float = 100000.0,
        fees: float = 0.001,
    ) -> dict[str, Any]:
        """Export a backtest report as a local HTML file. Required inputs: symbol, timeframe, strategy_type."""
        try:
            return _success(
                "export_backtest_report_html",
                service.export_backtest_report_html(
                    symbol,
                    timeframe=timeframe,
                    strategy_type=strategy_type,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    strategy_params=strategy_params,
                    initial_cash=initial_cash,
                    fees=fees,
                ),
            )
        except Exception as exc:
            return _failure("export_backtest_report_html", exc)

    @mcp.tool()
    def export_backtest_equity_curve_image(
        symbol: str,
        timeframe: str,
        strategy_type: str,
        start_date: str = "",
        end_date: str = "",
        instrument_type: str = "STOCK",
        strategy_params: dict[str, Any] | None = None,
        initial_cash: float = 100000.0,
        fees: float = 0.001,
    ) -> dict[str, Any]:
        """Export the backtest equity curve as a local PNG file. Required inputs: symbol, timeframe, strategy_type."""
        try:
            return _success(
                "export_backtest_equity_curve_image",
                service.export_backtest_equity_curve_image(
                    symbol,
                    timeframe=timeframe,
                    strategy_type=strategy_type,
                    start_date=start_date,
                    end_date=end_date,
                    instrument_type=instrument_type,
                    strategy_params=strategy_params,
                    initial_cash=initial_cash,
                    fees=fees,
                ),
            )
        except Exception as exc:
            return _failure("export_backtest_equity_curve_image", exc)
