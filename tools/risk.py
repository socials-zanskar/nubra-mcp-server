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
    def get_position_risk_report(
        include_options_greeks: bool = True,
        stress_move_pct: float = 1.0,
    ) -> dict[str, Any]:
        """Summarize open-position risk, directional bias, biggest losers, and simple stress impact."""
        try:
            return _success(
                "get_position_risk_report",
                service.get_position_risk_report(
                    include_options_greeks=include_options_greeks,
                    stress_move_pct=stress_move_pct,
                ),
            )
        except Exception as exc:
            return _failure("get_position_risk_report", exc)

    @mcp.tool()
    def get_option_strategy_snapshot(
        symbol: str,
        expiry: str | None = None,
        strategy_type: str = "straddle",
    ) -> dict[str, Any]:
        """Build a read-only live option strategy snapshot such as an ATM straddle or strangle."""
        try:
            return _success(
                "get_option_strategy_snapshot",
                service.get_option_strategy_snapshot(symbol, expiry=expiry, strategy_type=strategy_type),
            )
        except Exception as exc:
            return _failure("get_option_strategy_snapshot", exc)

    @mcp.tool()
    def compare_option_expiries(
        symbol: str,
        expiries: list[str] | None = None,
        top_k_strikes: int = 5,
    ) -> dict[str, Any]:
        """Compare option-chain structure, IV, OI, and liquidity across expiries for one underlying."""
        try:
            return _success(
                "compare_option_expiries",
                service.compare_option_expiries(symbol, expiries=expiries, top_k_strikes=top_k_strikes),
            )
        except Exception as exc:
            return _failure("compare_option_expiries", exc)
