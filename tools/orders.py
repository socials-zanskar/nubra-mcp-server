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
    def get_margin(
        orders: list[dict[str, Any]],
        with_portfolio: bool = True,
        with_legs: bool = False,
        is_basket: bool = False,
        basket_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Estimate margin using Nubra's orders/v2/margin_required API. Use total_margin as the final required margin."""
        try:
            return _success(
                "get_margin",
                service.get_margin(
                    exchange=exchange,
                    orders=orders,
                    with_portfolio=with_portfolio,
                    with_legs=with_legs,
                    is_basket=is_basket,
                    basket_params=basket_params,
                ),
            )
        except Exception as exc:
            return _failure("get_margin", exc)


    @mcp.tool()
    def get_atm_straddle_margins(
        symbols: list[str],
        expiry: str | None = None,
        lots: int = 1,
        order_side: str = "ORDER_SIDE_SELL",
        order_delivery_type: str = "ORDER_DELIVERY_TYPE_CNC",
        with_portfolio: bool = True,
        with_legs: bool = False,
    ) -> dict[str, Any]:
        """Estimate ATM straddle margin for one or more underlyings. Defaults to a short straddle using the nearest available expiry."""
        try:
            return _success(
                "get_atm_straddle_margins",
                service.estimate_atm_straddle_margin(
                    symbols,
                    exchange=exchange,
                    expiry=expiry,
                    lots=lots,
                    order_side=order_side,
                    order_delivery_type=order_delivery_type,
                    with_portfolio=with_portfolio,
                    with_legs=with_legs,
                ),
            )
        except Exception as exc:
            return _failure("get_atm_straddle_margins", exc)


    @mcp.tool()
    def get_orders(live: bool = False, executed: bool = False, tag: str | None = None) -> dict[str, Any]:
        """Fetch the Nubra day order book, optionally filtered to live, executed, or tagged orders."""
        try:
            return _success("get_orders", service.get_orders(live=live, executed=executed, tag=tag))
        except Exception as exc:
            return _failure("get_orders", exc)


    @mcp.tool()
    def get_strategy_pnl() -> dict[str, Any]:
        """Group executed orders by tag and infer strategy-level P&L from positions matched by ref_id."""
        try:
            return _success("get_strategy_pnl", service.strategy_pnl_summary())
        except Exception as exc:
            return _failure("get_strategy_pnl", exc)


    @mcp.tool()
    def get_positions() -> dict[str, Any]:
        """Fetch the current Nubra portfolio positions snapshot for the authenticated account."""
        try:
            return _success("get_positions", service.get_positions())
        except Exception as exc:
            return _failure("get_positions", exc)
