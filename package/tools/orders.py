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
    def preview_uat_order(order: dict[str, Any]) -> dict[str, Any]:
        """Preview a single UAT order in a clean table-ready format before placement. Use this first so the user can clearly verify symbol, side, quantity, price, and product."""
        try:
            return _success("preview_uat_order", service.preview_order(order))
        except Exception as exc:
            return _failure("preview_uat_order", exc)

    @mcp.tool()
    def place_uat_order(order: dict[str, Any]) -> dict[str, Any]:
        """Place a single order through Nubra UAT only. Use preview_uat_order first to show a table-style confirmation. PROD trading is strictly blocked."""
        try:
            return _success("place_uat_order", service.place_order(order))
        except Exception as exc:
            return _failure("place_uat_order", exc)

    @mcp.tool()
    def modify_uat_order(order: dict[str, Any]) -> dict[str, Any]:
        """Modify a single existing order through Nubra UAT only. PROD trading is strictly blocked."""
        try:
            return _success("modify_uat_order", service.modify_order(order))
        except Exception as exc:
            return _failure("modify_uat_order", exc)

    @mcp.tool()
    def cancel_uat_order(order_id: int) -> dict[str, Any]:
        """Cancel a single existing order through Nubra UAT only. PROD trading is strictly blocked."""
        try:
            return _success("cancel_uat_order", service.cancel_order(order_id))
        except Exception as exc:
            return _failure("cancel_uat_order", exc)

    @mcp.tool()
    def square_off_uat_position(
        symbol: str | None = None,
        ref_id: int | None = None,
        quantity: int | None = None,
    ) -> dict[str, Any]:
        """Square off an open position through Nubra UAT only. PROD trading is strictly blocked."""
        try:
            return _success(
                "square_off_uat_position",
                service.square_off_position(symbol=symbol, ref_id=ref_id, exchange=exchange, quantity=quantity),
            )
        except Exception as exc:
            return _failure("square_off_uat_position", exc)

    @mcp.tool()
    def place_uat_options_strategy(
        legs: list[dict[str, Any]],
        basket_name: str,
        tag: str | None = None,
        multiplier: int = 1,
        sign_style: str = "buy_positive",
        lots: int = 1,
        default_order_qty: int | None = None,
        order_delivery_type: str = "ORDER_DELIVERY_TYPE_CNC",
    ) -> dict[str, Any]:
        """Place a custom options basket through Nubra UAT only. PROD trading is strictly blocked."""
        try:
            return _success(
                "place_uat_options_strategy",
                service.place_options_strategy(
                    legs=legs,
                    basket_name=basket_name,
                    tag=tag,
                    exchange=exchange,
                    multiplier=multiplier,
                    sign_style=sign_style,
                    lots=lots,
                    default_order_qty=default_order_qty,
                    order_delivery_type=order_delivery_type,
                    environment="UAT",
                ),
            )
        except Exception as exc:
            return _failure("place_uat_options_strategy", exc)

    @mcp.tool()
    def place_uat_named_option_strategy(
        strategy: str,
        underlying: str,
        expiry_date: str,
        side: str = "sell",
        expiry_type: str | None = None,
        lots: int = 1,
        order_qty: int | None = None,
        basket_name: str | None = None,
        tag: str | None = None,
        multiplier: int = 1,
        sign_style: str | None = None,
        center_strike: int | float | None = None,
        call_strike: int | float | None = None,
        put_strike: int | float | None = None,
        lower_put_strike: int | float | None = None,
        upper_call_strike: int | float | None = None,
        order_delivery_type: str = "ORDER_DELIVERY_TYPE_CNC",
    ) -> dict[str, Any]:
        """Place a named options strategy through Nubra UAT only. PROD trading is strictly blocked."""
        try:
            return _success(
                "place_uat_named_option_strategy",
                service.place_named_option_strategy(
                    strategy=strategy,
                    underlying=underlying,
                    expiry_date=expiry_date,
                    exchange=exchange,
                    side=side,
                    expiry_type=expiry_type,
                    lots=lots,
                    order_qty=order_qty,
                    basket_name=basket_name,
                    tag=tag,
                    multiplier=multiplier,
                    sign_style=sign_style,
                    center_strike=center_strike,
                    call_strike=call_strike,
                    put_strike=put_strike,
                    lower_put_strike=lower_put_strike,
                    upper_call_strike=upper_call_strike,
                    order_delivery_type=order_delivery_type,
                    environment="UAT",
                ),
            )
        except Exception as exc:
            return _failure("place_uat_named_option_strategy", exc)


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
