from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import time as dt_time
from hashlib import sha1
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
from talib import abstract as _talib_abstract


RhsKind = Literal["number", "indicator", "range", "none"]
OperandCategory = Literal["price_level", "oscillator_bounded", "oscillator_unbounded", "volume_level"]
OperatorId = Literal[
    "greater_than",
    "less_than",
    "greater_equal",
    "less_equal",
    "equal",
    "crosses_above",
    "crosses_below",
    "up_by",
    "down_by",
    "within_range",
]
Side = Literal["BUY", "SELL"]
ExitMode = Literal["condition", "sl_tgt", "both"]
HoldingType = Literal["positional", "intraday"]
ExecutionStyle = Literal["same_bar_close", "next_bar_open"]
ConflictResolution = Literal["stop", "target"]

IST_TZ = "Asia/Kolkata"
IST = ZoneInfo(IST_TZ)
TRADING_SESSION_MINUTES = 375


@dataclass(frozen=True)
class ParamSpec:
    key: str
    label: str
    kind: Literal["int", "float", "source", "enum", "output"]
    default: int | float | str
    min_value: int | float | None = None
    max_value: int | float | None = None
    choices: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IndicatorSpec:
    type: str
    label: str
    category: OperandCategory
    params: tuple[ParamSpec, ...]
    outputs: tuple[str, ...] = field(default_factory=tuple)
    default_output: str | None = None
    has_source: bool = False
    multi_output: bool = False


SOURCE_CHOICES_PRICE: tuple[str, ...] = ("open", "high", "low", "close", "hl2", "hlc3", "ohlc4")
SOURCE_CHOICES_PRICE_VOL: tuple[str, ...] = (*SOURCE_CHOICES_PRICE, "volume")
MA_TYPE_CHOICES: tuple[str, ...] = ("SMA", "EMA", "WMA", "DEMA", "TEMA", "TRIMA", "KAMA", "MAMA", "T3")
OFFSET_SPEC = ParamSpec(key="offset", label="Offset", kind="int", default=0, min_value=0, max_value=500)


INDICATOR_CATALOG: dict[str, IndicatorSpec] = {
    "PRICE": IndicatorSpec(
        type="PRICE",
        label="Price",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "VOLUME": IndicatorSpec(type="VOLUME", label="Volume", category="volume_level", params=(OFFSET_SPEC,)),
    "RSI": IndicatorSpec(
        type="RSI",
        label="RSI",
        category="oscillator_bounded",
        params=(
            ParamSpec(key="length", label="Length", kind="int", default=14, min_value=2, max_value=500),
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "SMA": IndicatorSpec(
        type="SMA",
        label="SMA",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE_VOL),
            ParamSpec(key="period", label="Period", kind="int", default=9, min_value=1, max_value=500),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "EMA": IndicatorSpec(
        type="EMA",
        label="EMA",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE_VOL),
            ParamSpec(key="period", label="Period", kind="int", default=9, min_value=1, max_value=500),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "WMA": IndicatorSpec(
        type="WMA",
        label="WMA",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE_VOL),
            ParamSpec(key="period", label="Period", kind="int", default=9, min_value=1, max_value=500),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "VWAP": IndicatorSpec(
        type="VWAP",
        label="VWAP",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="hlc3", choices=SOURCE_CHOICES_PRICE),
            ParamSpec(key="anchor", label="Anchor", kind="enum", default="session", choices=("session", "week", "month")),
            OFFSET_SPEC,
        ),
        has_source=True,
    ),
    "BB": IndicatorSpec(
        type="BB",
        label="Bollinger Bands",
        category="price_level",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE_VOL),
            ParamSpec(key="length", label="Length", kind="int", default=20, min_value=2, max_value=500),
            ParamSpec(key="std_dev_up", label="StdDev Up", kind="float", default=2.0, min_value=0.1, max_value=10.0),
            ParamSpec(key="std_dev_down", label="StdDev Down", kind="float", default=2.0, min_value=0.1, max_value=10.0),
            ParamSpec(key="ma_type", label="MA Type", kind="enum", default="SMA", choices=MA_TYPE_CHOICES),
            OFFSET_SPEC,
        ),
        outputs=("upper_band", "middle_band", "lower_band"),
        default_output="middle_band",
        has_source=True,
        multi_output=True,
    ),
    "PSAR": IndicatorSpec(
        type="PSAR",
        label="Parabolic SAR",
        category="price_level",
        params=(
            ParamSpec(key="start", label="Start", kind="float", default=0.02, min_value=0.0, max_value=1.5),
            ParamSpec(key="increment", label="Increment", kind="float", default=0.02, min_value=0.0, max_value=1.0),
            ParamSpec(key="max_value", label="Max", kind="float", default=0.2, min_value=0.0, max_value=5.0),
            OFFSET_SPEC,
        ),
    ),
    "MACD": IndicatorSpec(
        type="MACD",
        label="MACD",
        category="oscillator_unbounded",
        params=(
            ParamSpec(key="source", label="Source", kind="source", default="close", choices=SOURCE_CHOICES_PRICE),
            ParamSpec(key="fast_length", label="Fast", kind="int", default=12, min_value=2, max_value=500),
            ParamSpec(key="slow_length", label="Slow", kind="int", default=26, min_value=3, max_value=500),
            ParamSpec(key="signal_length", label="Signal", kind="int", default=9, min_value=1, max_value=500),
            OFFSET_SPEC,
        ),
        outputs=("macd_line", "signal_line", "histogram"),
        default_output="macd_line",
        has_source=True,
        multi_output=True,
    ),
    "STOCH": IndicatorSpec(
        type="STOCH",
        label="Stochastic",
        category="oscillator_bounded",
        params=(
            ParamSpec(key="k_length", label="K Length", kind="int", default=14, min_value=1, max_value=500),
            ParamSpec(key="smooth_k", label="Smooth K", kind="int", default=3, min_value=1, max_value=200),
            ParamSpec(key="d_length", label="D Length", kind="int", default=3, min_value=1, max_value=200),
            ParamSpec(key="k_ma_type", label="K MA", kind="enum", default="SMA", choices=MA_TYPE_CHOICES),
            ParamSpec(key="d_ma_type", label="D MA", kind="enum", default="SMA", choices=MA_TYPE_CHOICES),
            OFFSET_SPEC,
        ),
        outputs=("k_line", "d_line"),
        default_output="k_line",
        multi_output=True,
    ),
    "CCI": IndicatorSpec(
        type="CCI",
        label="CCI",
        category="oscillator_unbounded",
        params=(ParamSpec(key="length", label="Length", kind="int", default=20, min_value=2, max_value=500), OFFSET_SPEC),
    ),
    "ADX": IndicatorSpec(
        type="ADX",
        label="ADX",
        category="oscillator_bounded",
        params=(ParamSpec(key="length", label="Length", kind="int", default=14, min_value=2, max_value=500), OFFSET_SPEC),
        outputs=("adx_value", "plus_di", "minus_di"),
        default_output="adx_value",
        multi_output=True,
    ),
    "ATR": IndicatorSpec(
        type="ATR",
        label="ATR",
        category="oscillator_unbounded",
        params=(ParamSpec(key="length", label="Length", kind="int", default=14, min_value=1, max_value=500), OFFSET_SPEC),
    ),
    "OBV": IndicatorSpec(type="OBV", label="OBV", category="volume_level", params=(OFFSET_SPEC,)),
}

OPERATOR_LABELS: dict[OperatorId, str] = {
    "greater_than": ">",
    "less_than": "<",
    "greater_equal": ">=",
    "less_equal": "<=",
    "equal": "=",
    "crosses_above": "crosses above",
    "crosses_below": "crosses below",
    "up_by": "up by",
    "down_by": "down by",
    "within_range": "within range",
}
COMPARISON_OPERATORS: tuple[OperatorId, ...] = (
    "greater_than",
    "less_than",
    "greater_equal",
    "less_equal",
    "equal",
    "crosses_above",
    "crosses_below",
)
DELTA_OPERATORS: tuple[OperatorId, ...] = ("up_by", "down_by")
RANGE_OPERATORS: tuple[OperatorId, ...] = ("within_range",)


def catalog_payload() -> dict[str, Any]:
    return {
        "indicators": [
            {
                "type": spec.type,
                "label": spec.label,
                "category": spec.category,
                "has_source": spec.has_source,
                "multi_output": spec.multi_output,
                "outputs": list(spec.outputs),
                "default_output": spec.default_output,
                "params": [
                    {
                        "key": param.key,
                        "label": param.label,
                        "kind": param.kind,
                        "default": param.default,
                        "min_value": param.min_value,
                        "max_value": param.max_value,
                        "choices": list(param.choices),
                    }
                    for param in spec.params
                ],
            }
            for spec in INDICATOR_CATALOG.values()
        ],
        "operators": [{"id": op, "label": OPERATOR_LABELS[op]} for op in COMPARISON_OPERATORS + DELTA_OPERATORS + RANGE_OPERATORS],
        "rhs_rules": {
            "price_level": {"default_kind": "indicator", "allow_number": True, "indicator_categories": ["price_level"]},
            "volume_level": {"default_kind": "indicator", "allow_number": True, "indicator_categories": ["volume_level"]},
            "oscillator_bounded": {"default_kind": "number", "allow_number": True, "indicator_categories": []},
            "oscillator_unbounded": {"default_kind": "number", "allow_number": True, "indicator_categories": []},
        },
        "delta_operators": list(DELTA_OPERATORS),
        "range_operators": list(RANGE_OPERATORS),
        "comparison_operators": list(COMPARISON_OPERATORS),
    }


def default_strategy_template() -> dict[str, Any]:
    return {
        "instruments": ["RELIANCE"],
        "chart": {"interval": "1d"},
        "entry": {
            "side": "BUY",
            "conditions": {
                "logic": "AND",
                "items": [
                    {
                        "lhs": {"type": "EMA", "params": {"source": "close", "period": 9}},
                        "op": "crosses_above",
                        "rhs": {"type": "EMA", "params": {"source": "close", "period": 21}},
                    }
                ],
            },
        },
        "exit": {
            "mode": "both",
            "conditions": {
                "logic": "AND",
                "items": [
                    {
                        "lhs": {"type": "EMA", "params": {"source": "close", "period": 9}},
                        "op": "crosses_below",
                        "rhs": {"type": "EMA", "params": {"source": "close", "period": 21}},
                    }
                ],
            },
            "stop_loss_pct": 2.0,
            "target_pct": 4.0,
        },
        "execute": {
            "initial_capital": 100000,
            "capital_allocation": {"mode": "split_total"},
            "start_date": "2026-02-01",
            "end_date": "2026-05-05",
            "start_time": "09:15",
            "end_time": "15:30",
            "holding_type": "positional",
            "exchange": "NSE",
            "instrument_type": "STOCK",
            "execution_style": "same_bar_close",
            "stop_target_conflict": "stop",
        },
    }


@dataclass(frozen=True)
class IndicatorExpr:
    type: str
    params: tuple[tuple[str, Any], ...]
    output: str | None
    offset: int

    @property
    def params_dict(self) -> dict[str, Any]:
        return dict(self.params)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "IndicatorExpr":
        indicator_type = str(payload.get("type", "")).upper()
        spec = INDICATOR_CATALOG.get(indicator_type)
        if spec is None:
            raise ValueError(f"Unknown indicator '{indicator_type}'.")

        raw_params = payload.get("params") or {}
        if not isinstance(raw_params, dict):
            raise ValueError(f"Indicator {indicator_type} params must be an object.")

        normalized: dict[str, Any] = {}
        offset = int(raw_params.get("offset", payload.get("offset", 0)) or 0)
        for param_spec in spec.params:
            if param_spec.key == "offset":
                continue
            value = raw_params.get(param_spec.key, param_spec.default)
            if param_spec.kind == "int":
                value = int(value)
            elif param_spec.kind == "float":
                value = float(value)
            else:
                value = str(value)
            normalized[param_spec.key] = value

        output = payload.get("output") or raw_params.get("output")
        if spec.multi_output:
            if output is None:
                output = spec.default_output
            if output not in spec.outputs:
                raise ValueError(f"Invalid output '{output}' for indicator {indicator_type}. Allowed: {list(spec.outputs)}.")
        else:
            output = None

        return cls(
            type=indicator_type,
            params=tuple(sorted(normalized.items())),
            output=output,
            offset=max(offset, 0),
        )


@dataclass(frozen=True)
class NumberOperand:
    value: float


@dataclass(frozen=True)
class RangeOperand:
    low: float
    high: float


Operand = IndicatorExpr | NumberOperand | RangeOperand


@dataclass(frozen=True)
class Condition:
    lhs: IndicatorExpr
    operator: OperatorId
    rhs: Operand


@dataclass(frozen=True)
class ConditionGroup:
    logic: str
    items: list[Any]


ConditionNode = Condition | ConditionGroup


def _coerce_number(value: Any) -> float:
    if isinstance(value, dict) and "value" in value:
        value = value["value"]
    return float(value)


def _rhs_indicator_family(expr: IndicatorExpr) -> str:
    indicator_type = expr.type.upper()
    source = str(expr.params_dict.get("source", "close")).lower()
    output = expr.output or ""
    if indicator_type in {"VOLUME", "OBV"}:
        return "volume"
    if indicator_type in {"SMA", "EMA", "WMA"} and source == "volume":
        return "volume"
    if indicator_type in {"PRICE", "SMA", "EMA", "WMA", "VWAP", "BB", "PSAR"}:
        return "price"
    if indicator_type == "MACD":
        return "macd"
    if indicator_type == "STOCH":
        return "stoch"
    if indicator_type == "ADX" and output in {"plus_di", "minus_di"}:
        return "adx_pair"
    return "number_only"


def _parse_indicator_or_number(rhs_payload: Any, lhs: IndicatorExpr) -> Operand:
    if isinstance(rhs_payload, dict) and rhs_payload.get("type"):
        expr = IndicatorExpr.from_dict(rhs_payload)
        lhs_family = _rhs_indicator_family(lhs)
        rhs_family = _rhs_indicator_family(expr)
        if lhs_family == "number_only":
            raise ValueError(f"RHS for {lhs.type} must be a number, not an indicator.")
        if lhs_family == "price" and rhs_family != "price":
            raise ValueError(f"RHS indicator must be price-like when LHS is {lhs.type}.")
        if lhs_family == "volume" and rhs_family != "volume":
            raise ValueError(f"RHS indicator must be volume-like when LHS is {lhs.type}.")
        if lhs_family == "macd" and expr.type.upper() != "MACD":
            raise ValueError("MACD conditions can only compare against another MACD output or a number.")
        if lhs_family == "stoch" and expr.type.upper() != "STOCH":
            raise ValueError("Stochastic conditions can only compare against another STOCH output or a number.")
        if lhs_family == "adx_pair":
            if expr.type.upper() != "ADX" or (expr.output or "") not in {"plus_di", "minus_di"}:
                raise ValueError("ADX +/-DI conditions can only compare against another ADX DI output or a number.")
        return expr
    return NumberOperand(value=_coerce_number(rhs_payload))


def parse_condition(payload: dict[str, Any]) -> Condition:
    lhs_payload = payload.get("lhs")
    if not isinstance(lhs_payload, dict):
        raise ValueError("Condition lhs must be an indicator object.")
    lhs = IndicatorExpr.from_dict(lhs_payload)
    operator_raw = payload.get("op") or payload.get("operator") or ""
    operator = str(operator_raw).lower()
    if operator not in set(COMPARISON_OPERATORS + DELTA_OPERATORS + RANGE_OPERATORS):
        raise ValueError(f"Unknown operator '{operator}'.")
    rhs_payload = payload.get("rhs")
    if operator in RANGE_OPERATORS:
        if not isinstance(rhs_payload, dict) or "low" not in rhs_payload or "high" not in rhs_payload:
            raise ValueError("within_range requires rhs={low, high}.")
        rhs: Operand = RangeOperand(low=float(rhs_payload["low"]), high=float(rhs_payload["high"]))
    elif operator in DELTA_OPERATORS:
        rhs = NumberOperand(value=_coerce_number(rhs_payload))
    else:
        rhs = _parse_indicator_or_number(rhs_payload, lhs)
    return Condition(lhs=lhs, operator=operator, rhs=rhs)  # type: ignore[arg-type]


def parse_condition_node(payload: Any) -> ConditionNode:
    if isinstance(payload, list):
        return ConditionGroup(logic="AND", items=[parse_condition_node(item) for item in payload])
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid condition node: expected dict, got {type(payload).__name__}.")
    if "logic" in payload and "items" in payload:
        logic_raw = str(payload["logic"]).upper()
        if logic_raw not in {"AND", "OR"}:
            raise ValueError(f"ConditionGroup logic must be 'AND' or 'OR', got '{logic_raw}'.")
        items_raw = payload.get("items") or []
        if not isinstance(items_raw, list):
            raise ValueError("ConditionGroup.items must be a list.")
        return ConditionGroup(logic=logic_raw, items=[parse_condition_node(item) for item in items_raw])
    return parse_condition(payload)


def iter_expressions_from_node(node: ConditionNode) -> list[IndicatorExpr]:
    out: list[IndicatorExpr] = []
    if isinstance(node, Condition):
        out.append(node.lhs)
        if isinstance(node.rhs, IndicatorExpr):
            out.append(node.rhs)
    else:
        for item in node.items:
            out.extend(iter_expressions_from_node(item))
    return out


def _value_at(df: pd.DataFrame, index: int, expr: IndicatorExpr) -> float | None:
    column = column_name_for(expr)
    target_index = index - int(expr.offset)
    if target_index < 0 or target_index >= len(df):
        return None
    value = df[column].iloc[target_index]
    if pd.isna(value):
        return None
    return float(value)


def _operand_value(df: pd.DataFrame, index: int, operand: Operand) -> float | None:
    if isinstance(operand, IndicatorExpr):
        return _value_at(df, index, operand)
    if isinstance(operand, NumberOperand):
        return operand.value
    return None


def evaluate_condition(df: pd.DataFrame, index: int, condition: Condition) -> bool:
    if index < 0 or index >= len(df):
        return False
    lhs_now = _value_at(df, index, condition.lhs)
    if lhs_now is None:
        return False
    op = condition.operator
    if op == "within_range" and isinstance(condition.rhs, RangeOperand):
        return condition.rhs.low <= lhs_now <= condition.rhs.high
    if op in DELTA_OPERATORS and isinstance(condition.rhs, NumberOperand):
        prev_index = index - 1 - int(condition.lhs.offset)
        if prev_index < 0 or prev_index >= len(df):
            return False
        column = column_name_for(condition.lhs)
        prev_raw = df[column].iloc[prev_index]
        if pd.isna(prev_raw):
            return False
        delta = lhs_now - float(prev_raw)
        return delta >= condition.rhs.value if op == "up_by" else (-delta) >= condition.rhs.value
    rhs_now = _operand_value(df, index, condition.rhs)
    if rhs_now is None:
        return False
    if op == "greater_than":
        return lhs_now > rhs_now
    if op == "less_than":
        return lhs_now < rhs_now
    if op == "greater_equal":
        return lhs_now >= rhs_now
    if op == "less_equal":
        return lhs_now <= rhs_now
    if op == "equal":
        return abs(lhs_now - rhs_now) < 1e-9
    if op in {"crosses_above", "crosses_below"}:
        if index == 0:
            return False
        lhs_prev = _value_at(df, index - 1, condition.lhs)
        rhs_prev = _operand_value(df, index - 1, condition.rhs)
        if lhs_prev is None or rhs_prev is None:
            return False
        return (lhs_prev <= rhs_prev and lhs_now > rhs_now) if op == "crosses_above" else (lhs_prev >= rhs_prev and lhs_now < rhs_now)
    return False


def evaluate_node(df: pd.DataFrame, index: int, node: ConditionNode) -> bool:
    if isinstance(node, Condition):
        return evaluate_condition(df, index, node)
    if node.logic == "AND":
        return len(node.items) > 0 and all(evaluate_node(df, index, item) for item in node.items)
    return any(evaluate_node(df, index, item) for item in node.items)


def _computation_signature(expr: IndicatorExpr) -> str:
    parts = [expr.type.upper(), expr.output or "-"]
    parts.extend(f"{key}={value}" for key, value in expr.params)
    return "|".join(parts)


def _signature_to_column(signature: str) -> str:
    return f"ind_{sha1(signature.encode('utf-8')).hexdigest()[:12]}"


def column_name_for(expr: IndicatorExpr) -> str:
    if expr.type.upper() == "VOLUME":
        return "volume"
    return _signature_to_column(_computation_signature(expr))


def talib_function_name_for_expr(expr: IndicatorExpr) -> str:
    indicator_type = expr.type.upper()
    if indicator_type == "ADX":
        output = expr.output or "adx_value"
        return {"adx_value": "ADX", "plus_di": "PLUS_DI", "minus_di": "MINUS_DI"}[output]
    return {
        "RSI": "RSI",
        "SMA": "SMA",
        "EMA": "EMA",
        "WMA": "WMA",
        "BB": "BBANDS",
        "PSAR": "SAR",
        "MACD": "MACD",
        "STOCH": "STOCH",
        "CCI": "CCI",
        "ATR": "ATR",
        "OBV": "OBV",
    }[indicator_type]


def _talib_params_for(expr: IndicatorExpr) -> dict[str, Any]:
    indicator_type = expr.type.upper()
    params = expr.params_dict
    if indicator_type == "RSI":
        return {"timeperiod": int(params.get("length", 14))}
    if indicator_type in {"SMA", "EMA", "WMA"}:
        return {"timeperiod": int(params.get("period", 9))}
    if indicator_type == "BB":
        return {
            "timeperiod": int(params.get("length", 20)),
            "nbdevup": float(params.get("std_dev_up", 2.0)),
            "nbdevdn": float(params.get("std_dev_down", 2.0)),
            "matype": {"SMA": 0, "EMA": 1, "WMA": 2, "DEMA": 3, "TEMA": 4, "TRIMA": 5, "KAMA": 6, "MAMA": 7, "T3": 8}.get(str(params.get("ma_type", "SMA")).upper(), 0),
        }
    if indicator_type == "PSAR":
        return {"acceleration": float(params.get("increment", 0.02)), "maximum": float(params.get("max_value", 0.2))}
    if indicator_type == "MACD":
        return {
            "fastperiod": int(params.get("fast_length", 12)),
            "slowperiod": int(params.get("slow_length", 26)),
            "signalperiod": int(params.get("signal_length", 9)),
        }
    if indicator_type == "STOCH":
        return {
            "fastk_period": int(params.get("k_length", 14)),
            "slowk_period": int(params.get("smooth_k", 3)),
            "slowk_matype": {"SMA": 0, "EMA": 1, "WMA": 2, "DEMA": 3, "TEMA": 4, "TRIMA": 5, "KAMA": 6, "MAMA": 7, "T3": 8}.get(str(params.get("k_ma_type", "SMA")).upper(), 0),
            "slowd_period": int(params.get("d_length", 3)),
            "slowd_matype": {"SMA": 0, "EMA": 1, "WMA": 2, "DEMA": 3, "TEMA": 4, "TRIMA": 5, "KAMA": 6, "MAMA": 7, "T3": 8}.get(str(params.get("d_ma_type", "SMA")).upper(), 0),
        }
    if indicator_type in {"CCI", "ADX", "ATR"}:
        return {"timeperiod": int(params.get("length", 14))}
    if indicator_type == "OBV":
        return {}
    return {}


def required_history_bars(expr: IndicatorExpr, interval: str | None = None) -> int:
    indicator_type = expr.type.upper()
    if indicator_type == "PRICE":
        return expr.offset
    if indicator_type == "VOLUME":
        base = 1 if interval and interval_is_intraday(interval) else 0
        return base + expr.offset
    if indicator_type == "VWAP":
        base = 1 if interval and interval_is_intraday(interval) else 0
        return base + expr.offset
    fn = _talib_abstract.Function(talib_function_name_for_expr(expr))
    talib_params = _talib_params_for(expr)
    if talib_params:
        fn.set_parameters(talib_params)
    return int(fn.lookback) + expr.offset


def interval_is_intraday(interval: str) -> bool:
    value = interval.strip().lower()
    return value not in {"1d", "1w", "1mt"} and value.endswith(("s", "m", "h"))


def _source_series(df: pd.DataFrame, source: str) -> pd.Series:
    if source == "hl2":
        return (df["high"] + df["low"]) / 2.0
    if source == "hlc3":
        return (df["high"] + df["low"] + df["close"]) / 3.0
    if source == "ohlc4":
        return (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0
    return df[source]


def inject_indicator_columns(service: Any, df: pd.DataFrame, expressions: list[IndicatorExpr]) -> pd.DataFrame:
    _, add_talib = service._load_talib_helpers()
    out = df.copy()
    dedup: dict[str, IndicatorExpr] = {}
    for expr in expressions:
        key = _computation_signature(expr)
        existing = dedup.get(key)
        if existing is None or expr.offset > existing.offset:
            dedup[key] = expr

    multi_output_map = {
        "BB": {"upper_band": "upperband", "middle_band": "middleband", "lower_band": "lowerband"},
        "MACD": {"macd_line": "macd", "signal_line": "macdsignal", "histogram": "macdhist"},
        "STOCH": {"k_line": "slowk", "d_line": "slowd"},
    }

    for expr in dedup.values():
        indicator_type = expr.type.upper()
        target_col = column_name_for(expr)
        if target_col in out.columns:
            continue
        if indicator_type == "PRICE":
            out[target_col] = _source_series(out, str(expr.params_dict.get("source", "close")))
            continue
        if indicator_type == "VOLUME":
            continue
        if indicator_type == "VWAP":
            source_key = str(expr.params_dict.get("source", "hlc3"))
            anchor = str(expr.params_dict.get("anchor", "session"))
            source = _source_series(out, source_key)
            volume = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
            localized = pd.to_datetime(out["timestamp"]).dt.tz_localize(None)
            if anchor == "session":
                group_keys = localized.dt.strftime("%Y-%m-%d")
            elif anchor == "week":
                iso = localized.dt.isocalendar()
                group_keys = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
            else:
                group_keys = localized.dt.strftime("%Y-%m")
            pv = (source * volume).groupby(group_keys).cumsum()
            vol_cum = volume.groupby(group_keys).cumsum()
            out[target_col] = pv.div(vol_cum.replace(0, pd.NA))
            continue

        talib_name = talib_function_name_for_expr(expr)
        if indicator_type == "ADX":
            output_name = {"adx_value": "adx", "plus_di": "plus_di", "minus_di": "minus_di"}[expr.output or "adx_value"]
            native_col = f"{talib_name.lower()}_{output_name.lower()}" if output_name != "adx" else "adx"
        elif expr.output is None:
            native_col = talib_name.lower()
        else:
            native_col = f"{talib_name.lower()}_{multi_output_map[indicator_type][expr.output].lower()}"

        talib_params = _talib_params_for(expr)
        source = expr.params_dict.get("source") if expr.type in {"RSI", "SMA", "EMA", "WMA", "MACD", "BB"} else None
        working = out
        rename_from_close: str | None = None
        if source and source in {"hl2", "hlc3", "ohlc4", "volume"}:
            working = out.copy()
            working["_orig_close"] = working["close"]
            working["close"] = _source_series(out, str(source))
            rename_from_close = "_orig_close"
        elif source and source != "close":
            working = out.copy()
            working["_orig_close"] = working["close"]
            working["close"] = working[str(source)]
            rename_from_close = "_orig_close"

        enriched = add_talib(working, funcs={talib_name: talib_params})
        if rename_from_close:
            enriched["close"] = enriched[rename_from_close]
            enriched = enriched.drop(columns=[rename_from_close])
        if native_col not in enriched.columns:
            raise ValueError(f"Indicator {expr.type} did not produce expected column '{native_col}'.")
        out[target_col] = enriched[native_col]
    return out


@dataclass(frozen=True)
class CostConfig:
    intraday_brokerage_pct: float = 0.03
    intraday_brokerage_flat: float = 20.0
    delivery_brokerage_pct: float = 0.0
    delivery_brokerage_flat: float = 0.0


@dataclass(frozen=True)
class ParsedStrategy:
    instruments: list[str]
    interval: str
    entry_side: Side
    entry_conditions: ConditionNode
    exit_mode: ExitMode
    exit_conditions: ConditionNode | None
    stop_loss_pct: float | None
    target_pct: float | None
    allocation_mode: str
    initial_capital: float
    capital_per_instrument: float
    capital_by_instrument: dict[str, float]
    start_date: str
    end_date: str
    start_time: str
    end_time: str
    holding_type: HoldingType
    exchange: str
    instrument_type: str
    execution_style: ExecutionStyle
    stop_target_conflict: ConflictResolution
    cost_config: CostConfig | None


@dataclass
class Trade:
    symbol: str
    side: Side
    entry_timestamp: str
    exit_timestamp: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    pnl_pct: float
    bars_held: int
    exit_reason: str
    brokerage: float


@dataclass
class EquityPoint:
    timestamp: str
    equity: float


@dataclass
class InstrumentMetrics:
    starting_capital: float
    ending_capital: float
    gross_profit: float
    gross_loss: float
    net_pnl: float
    return_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate_pct: float
    avg_pnl: float
    avg_pnl_pct: float
    profit_factor: float | None
    max_drawdown_pct: float
    total_brokerage: float


@dataclass
class DailySignalLogRow:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    entry_signal: bool
    exit_signal: bool
    action: str
    position_state: str
    stop_loss_price: float | None
    target_price: float | None


@dataclass
class InstrumentBacktestResult:
    symbol: str
    bars_processed: int
    metrics: InstrumentMetrics
    trades: list[Trade]
    equity_curve: list[EquityPoint]
    triggered_days: list[DailySignalLogRow]
    daily_signal_log: list[DailySignalLogRow]
    warning: str | None = None


def _parse_time_hhmm(value: str) -> dt_time:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time value '{value}'. Expected HH:MM.")
    return dt_time(hour=int(parts[0]), minute=int(parts[1]))


def _parse_condition_group_or_list(payload: Any, field_name: str) -> ConditionNode:
    if payload is None or (isinstance(payload, list) and len(payload) == 0):
        raise ValueError(f"{field_name} must contain at least one condition or group.")
    node = parse_condition_node(payload)
    if isinstance(node, ConditionGroup) and len(node.items) == 0:
        raise ValueError(f"{field_name} must contain at least one condition or group.")
    return node


def parse_strategy(payload: dict[str, Any]) -> ParsedStrategy:
    instruments_raw = payload.get("instruments") or []
    if not isinstance(instruments_raw, list) or not instruments_raw:
        raise ValueError("At least one instrument is required.")
    instruments: list[str] = []
    seen: set[str] = set()
    for item in instruments_raw:
        sym = str(item).strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            instruments.append(sym)
    if not instruments:
        raise ValueError("At least one valid instrument is required.")

    chart_payload = payload.get("chart") or {}
    interval = str(chart_payload.get("interval") or payload.get("interval") or "1d").strip().lower()
    if not interval:
        raise ValueError("chart.interval is required.")

    entry_payload = payload.get("entry") or {}
    entry_side_raw = {"LONG": "BUY", "SHORT": "SELL"}.get(str(entry_payload.get("side") or "BUY").upper(), str(entry_payload.get("side") or "BUY").upper())
    if entry_side_raw not in {"BUY", "SELL"}:
        raise ValueError("Entry side must be BUY or SELL.")
    entry_conditions = _parse_condition_group_or_list(entry_payload.get("conditions"), "entry.conditions")

    exit_payload = payload.get("exit") or {}
    exit_mode_raw = str(exit_payload.get("mode") or "condition").lower()
    if exit_mode_raw not in {"condition", "sl_tgt", "both"}:
        raise ValueError("exit.mode must be one of: condition, sl_tgt, both.")
    exit_conditions: ConditionNode | None = None
    if exit_mode_raw in {"condition", "both"}:
        exit_conditions = _parse_condition_group_or_list(exit_payload.get("conditions"), "exit.conditions")
    stop_loss_pct = float(exit_payload["stop_loss_pct"]) if exit_payload.get("stop_loss_pct") is not None else None
    target_pct = float(exit_payload["target_pct"]) if exit_payload.get("target_pct") is not None else None
    if exit_mode_raw in {"sl_tgt", "both"} and stop_loss_pct is None and target_pct is None:
        raise ValueError("Provide stop_loss_pct and/or target_pct when exit.mode is 'sl_tgt' or 'both'.")

    execute_payload = payload.get("execute") or {}
    capital_allocation_payload = execute_payload.get("capital_allocation") or {}
    allocation_mode = str(capital_allocation_payload.get("mode") or "split_total").lower()
    if allocation_mode not in {"split_total", "per_stock", "custom"}:
        raise ValueError("capital_allocation.mode must be one of: split_total, per_stock, custom.")
    raw_initial_capital = float(execute_payload.get("initial_capital") or 0)
    if raw_initial_capital <= 0:
        raise ValueError("initial_capital must be positive.")

    start_date = str(execute_payload.get("start_date") or "")
    end_date = str(execute_payload.get("end_date") or "")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required.")
    start_time = str(execute_payload.get("start_time") or "09:15")
    end_time = str(execute_payload.get("end_time") or "15:30")

    holding_type = {"longterm": "positional", "intraday": "intraday", "positional": "positional"}.get(str(execute_payload.get("holding_type") or "positional").lower(), "positional")
    if holding_type == "intraday" and not interval_is_intraday(interval):
        raise ValueError(f"holding_type='intraday' requires an intraday chart interval, got '{interval}'.")

    exchange = str(execute_payload.get("exchange") or "NSE").upper()
    instrument_type = str(execute_payload.get("instrument_type") or "STOCK").upper()
    execution_style_raw = str(execute_payload.get("execution_style") or "same_bar_close").lower()
    if execution_style_raw not in {"same_bar_close", "next_bar_open"}:
        raise ValueError("execution_style must be 'same_bar_close' or 'next_bar_open'.")
    conflict_raw = str(execute_payload.get("stop_target_conflict") or "stop").lower()
    if conflict_raw not in {"stop", "target"}:
        raise ValueError("stop_target_conflict must be 'stop' or 'target'.")

    cost_config = None
    if isinstance(execute_payload.get("cost_config"), dict):
        raw_cost = execute_payload["cost_config"]
        cost_config = CostConfig(
            intraday_brokerage_pct=float(raw_cost.get("intraday_brokerage_pct", 0.03)),
            intraday_brokerage_flat=float(raw_cost.get("intraday_brokerage_flat", 20.0)),
            delivery_brokerage_pct=float(raw_cost.get("delivery_brokerage_pct", 0.0)),
            delivery_brokerage_flat=float(raw_cost.get("delivery_brokerage_flat", 0.0)),
        )

    if allocation_mode == "split_total":
        initial_capital = raw_initial_capital
        capital_per_instrument = initial_capital / len(instruments)
        capital_by_instrument = {symbol: capital_per_instrument for symbol in instruments}
    elif allocation_mode == "per_stock":
        per_stock_capital = float(capital_allocation_payload.get("per_stock_capital") or raw_initial_capital)
        if per_stock_capital <= 0:
            raise ValueError("capital_allocation.per_stock_capital must be positive.")
        capital_per_instrument = per_stock_capital
        initial_capital = capital_per_instrument * len(instruments)
        capital_by_instrument = {symbol: capital_per_instrument for symbol in instruments}
    else:
        custom_map = capital_allocation_payload.get("custom_capital_map")
        if not isinstance(custom_map, dict) or not custom_map:
            raise ValueError("capital_allocation.custom_capital_map is required when capital_allocation.mode is 'custom'.")
        capital_by_instrument = {}
        for symbol in instruments:
            parsed_value = float(custom_map.get(symbol))
            if parsed_value <= 0:
                raise ValueError(f"Custom capital for {symbol} must be positive.")
            capital_by_instrument[symbol] = parsed_value
        initial_capital = sum(capital_by_instrument.values())
        capital_per_instrument = initial_capital / len(instruments)

    return ParsedStrategy(
        instruments=instruments,
        interval=interval,
        entry_side=entry_side_raw,  # type: ignore[arg-type]
        entry_conditions=entry_conditions,
        exit_mode=exit_mode_raw,  # type: ignore[arg-type]
        exit_conditions=exit_conditions,
        stop_loss_pct=stop_loss_pct,
        target_pct=target_pct,
        allocation_mode=allocation_mode,
        initial_capital=initial_capital,
        capital_per_instrument=capital_per_instrument,
        capital_by_instrument=capital_by_instrument,
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        holding_type=holding_type,  # type: ignore[arg-type]
        exchange=exchange,
        instrument_type=instrument_type,
        execution_style=execution_style_raw,  # type: ignore[arg-type]
        stop_target_conflict=conflict_raw,  # type: ignore[arg-type]
        cost_config=cost_config,
    )


def _ts_str(ts: Any) -> str:
    return str(ts)


def _position_size(capital: float, entry_price: float) -> float:
    return 0.0 if entry_price <= 0 else capital / entry_price


def _pnl_from(side: Side, entry_price: float, exit_price: float, qty: float) -> float:
    return (exit_price - entry_price) * qty if side == "BUY" else (entry_price - exit_price) * qty


def _calc_brokerage(trade_value: float, is_intraday: bool, cost_config: CostConfig | None) -> float:
    if cost_config is None:
        return 0.0
    if is_intraday:
        pct = cost_config.intraday_brokerage_pct
        flat = cost_config.intraday_brokerage_flat
    else:
        pct = cost_config.delivery_brokerage_pct
        flat = cost_config.delivery_brokerage_flat
    leg = trade_value * pct / 100.0
    return min(leg, flat) if flat > 0 else leg


def resolve_stop_target_exit(*, side: Side, bar_open: float, bar_high: float, bar_low: float, stop_price: float | None, target_price: float | None, conflict_resolution: ConflictResolution) -> tuple[str | None, float | None]:
    if side == "BUY":
        if stop_price is not None and bar_open <= stop_price:
            return "stop_loss", bar_open
        if target_price is not None and bar_open >= target_price:
            return "target", bar_open
        stop_hit = stop_price is not None and bar_low <= stop_price
        target_hit = target_price is not None and bar_high >= target_price
        if stop_hit and target_hit:
            return ("target", target_price) if conflict_resolution == "target" else ("stop_loss", stop_price)
        if stop_hit:
            return "stop_loss", stop_price
        if target_hit:
            return "target", target_price
        return None, None
    if stop_price is not None and bar_open >= stop_price:
        return "stop_loss", bar_open
    if target_price is not None and bar_open <= target_price:
        return "target", bar_open
    stop_hit = stop_price is not None and bar_high >= stop_price
    target_hit = target_price is not None and bar_low <= target_price
    if stop_hit and target_hit:
        return ("target", target_price) if conflict_resolution == "target" else ("stop_loss", stop_price)
    if stop_hit:
        return "stop_loss", stop_price
    if target_hit:
        return "target", target_price
    return None, None


def _max_drawdown_pct(equity_points: list[float]) -> float:
    if not equity_points:
        return 0.0
    peak = equity_points[0]
    max_dd = 0.0
    for value in equity_points:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
    return round(max_dd * 100.0, 4)


def _all_expressions(strategy: ParsedStrategy) -> list[IndicatorExpr]:
    exprs = iter_expressions_from_node(strategy.entry_conditions)
    if strategy.exit_conditions is not None:
        exprs += iter_expressions_from_node(strategy.exit_conditions)
    return exprs


def _warmup_bars_for(expressions: list[IndicatorExpr], interval: str) -> int:
    return 0 if not expressions else max(required_history_bars(expr, interval) for expr in expressions)


def _empty_metrics(capital: float) -> InstrumentMetrics:
    return InstrumentMetrics(capital, capital, 0.0, 0.0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0, 0.0, None, 0.0, 0.0)


def _run_instrument(strategy: ParsedStrategy, df: pd.DataFrame, symbol: str) -> InstrumentBacktestResult:
    requested_start = pd.Timestamp(strategy.start_date).tz_localize(IST) if pd.Timestamp(strategy.start_date).tzinfo is None else pd.Timestamp(strategy.start_date).tz_convert(IST)
    requested_end = pd.Timestamp(strategy.end_date).tz_localize(IST) if pd.Timestamp(strategy.end_date).tzinfo is None else pd.Timestamp(strategy.end_date).tz_convert(IST)
    if requested_end.hour == 0 and requested_end.minute == 0 and requested_end.second == 0:
        requested_end = requested_end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    executable = df[(pd.to_datetime(df["timestamp"]) >= requested_start) & (pd.to_datetime(df["timestamp"]) <= requested_end)].copy()
    if executable.empty:
        return InstrumentBacktestResult(symbol=symbol, bars_processed=0, metrics=_empty_metrics(strategy.capital_by_instrument.get(symbol, strategy.capital_per_instrument)), trades=[], equity_curve=[], triggered_days=[], daily_signal_log=[], warning=f"No rows available for {symbol} in requested range.")

    is_intraday = interval_is_intraday(strategy.interval) or strategy.holding_type == "intraday"
    start_time = _parse_time_hhmm(strategy.start_time)
    end_time = _parse_time_hhmm(strategy.end_time)

    absolute_indices = executable.index.to_list()
    trades: list[Trade] = []
    daily_log: list[DailySignalLogRow] = []
    capital = strategy.capital_by_instrument.get(symbol, strategy.capital_per_instrument)
    starting_capital = capital
    first_ts = _ts_str(executable["timestamp"].iloc[0])
    equity_curve: list[EquityPoint] = [EquityPoint(timestamp=first_ts, equity=round(capital, 4))]

    in_position = False
    entry_bar_local: int | None = None
    entry_price = 0.0
    qty = 0.0
    entry_time_str = ""
    stop_loss_price: float | None = None
    target_price: float | None = None
    pending_entry = False
    pending_exit = False
    entry_signal_hits = 0
    blocked_entry_signals = 0

    def _compute_sl_tgt(price: float) -> tuple[float | None, float | None]:
        if strategy.entry_side == "BUY":
            sl = price * (1 - strategy.stop_loss_pct / 100.0) if strategy.stop_loss_pct else None
            tgt = price * (1 + strategy.target_pct / 100.0) if strategy.target_pct else None
        else:
            sl = price * (1 + strategy.stop_loss_pct / 100.0) if strategy.stop_loss_pct else None
            tgt = price * (1 - strategy.target_pct / 100.0) if strategy.target_pct else None
        return sl, tgt

    def _brokerage_for(price: float, quantity: float) -> float:
        return round(_calc_brokerage(price * quantity, is_intraday, strategy.cost_config) * 2, 4)

    def _open_position(bar: pd.Series, price: float, local_i: int) -> None:
        nonlocal in_position, entry_bar_local, entry_price, qty, entry_time_str, stop_loss_price, target_price
        sized = _position_size(capital, price)
        if sized <= 0:
            return
        in_position = True
        entry_bar_local = local_i
        entry_price = price
        qty = sized
        entry_time_str = _ts_str(bar["timestamp"])
        stop_loss_price, target_price = _compute_sl_tgt(price) if strategy.exit_mode in {"sl_tgt", "both"} else (None, None)
        equity_curve.append(EquityPoint(timestamp=entry_time_str, equity=round(capital, 4)))

    def _close_position(exit_price: float, exit_bar: pd.Series, reason: str) -> None:
        nonlocal capital, in_position, entry_bar_local, entry_price, qty, entry_time_str, stop_loss_price, target_price
        gross_pnl = _pnl_from(strategy.entry_side, entry_price, exit_price, qty)
        brokerage = _brokerage_for(entry_price, qty)
        net_pnl = gross_pnl - brokerage
        capital += net_pnl
        entry_notional = entry_price * qty
        pnl_pct = (net_pnl / entry_notional * 100.0) if entry_notional > 0 else 0.0
        exit_local = absolute_indices.index(exit_bar.name) if exit_bar.name in absolute_indices else 0
        bars_held = max(0, exit_local - (entry_bar_local or 0))
        trades.append(
            Trade(
                symbol=symbol,
                side=strategy.entry_side,
                entry_timestamp=entry_time_str,
                exit_timestamp=_ts_str(exit_bar["timestamp"]),
                entry_price=round(entry_price, 4),
                exit_price=round(exit_price, 4),
                quantity=round(qty, 4),
                pnl=round(net_pnl, 4),
                pnl_pct=round(pnl_pct, 4),
                bars_held=bars_held,
                exit_reason=reason,
                brokerage=brokerage,
            )
        )
        equity_curve.append(EquityPoint(timestamp=_ts_str(exit_bar["timestamp"]), equity=round(capital, 4)))
        in_position = False
        entry_bar_local = None
        entry_price = 0.0
        qty = 0.0
        entry_time_str = ""
        stop_loss_price = None
        target_price = None

    for local_i, absolute_i in enumerate(absolute_indices):
        bar = df.loc[absolute_i]
        bar_open = float(bar["open"])
        bar_high = float(bar["high"])
        bar_low = float(bar["low"])
        bar_close = float(bar["close"])
        bar_time = pd.Timestamp(bar["timestamp"]).time()
        action_parts: list[str] = []

        session_allows_entry = True
        session_must_close = False
        if is_intraday:
            session_allows_entry = start_time <= bar_time < end_time
            if bar_time >= end_time:
                session_must_close = True
            elif local_i + 1 < len(absolute_indices):
                next_bar = df.loc[absolute_indices[local_i + 1]]
                if pd.Timestamp(bar["timestamp"]).date() != pd.Timestamp(next_bar["timestamp"]).date():
                    session_must_close = True
            else:
                session_must_close = True

        if strategy.execution_style == "next_bar_open":
            if pending_exit and in_position:
                _close_position(bar_open, bar, "exit_condition")
                pending_exit = False
                action_parts.append("exit_settled")
            if pending_entry and not in_position:
                _open_position(bar, bar_open, local_i)
                pending_entry = False
                if in_position:
                    action_parts.append("enter_settled")

        if in_position and strategy.exit_mode in {"sl_tgt", "both"}:
            exit_reason_sl_tgt, exit_price_override = resolve_stop_target_exit(
                side=strategy.entry_side,
                bar_open=bar_open,
                bar_high=bar_high,
                bar_low=bar_low,
                stop_price=stop_loss_price,
                target_price=target_price,
                conflict_resolution=strategy.stop_target_conflict,
            )
            if exit_reason_sl_tgt and exit_price_override is not None:
                _close_position(exit_price_override, bar, exit_reason_sl_tgt)
                action_parts.append(f"exit_{exit_reason_sl_tgt}")

        entry_signal = evaluate_node(df.reset_index(drop=True), list(df.index).index(absolute_i), strategy.entry_conditions) if not in_position else False
        if entry_signal:
            entry_signal_hits += 1
            if not session_allows_entry:
                blocked_entry_signals += 1
        exit_signal = in_position and strategy.exit_mode in {"condition", "both"} and strategy.exit_conditions is not None and evaluate_node(df.reset_index(drop=True), list(df.index).index(absolute_i), strategy.exit_conditions)

        if strategy.execution_style == "same_bar_close":
            if in_position and exit_signal:
                _close_position(bar_close, bar, "exit_condition")
                action_parts.append("exit_condition")
            elif in_position and session_must_close:
                _close_position(bar_close, bar, "session_end")
                action_parts.append("exit_session_end")
            elif not in_position and entry_signal and session_allows_entry:
                _open_position(bar, bar_close, local_i)
                if in_position:
                    action_parts.append(f"enter_{strategy.entry_side.lower()}")
        else:
            if in_position and session_must_close:
                _close_position(bar_close, bar, "session_end")
                action_parts.append("exit_session_end")
                pending_exit = False
            elif not in_position and entry_signal and session_allows_entry:
                pending_entry = True
                action_parts.append("entry_pending")
            elif in_position and exit_signal:
                pending_exit = True
                action_parts.append("exit_pending")

        current_equity = capital + (_pnl_from(strategy.entry_side, entry_price, bar_close, qty) if in_position else 0.0)
        position_state = "flat" if not in_position else ("open_buy" if strategy.entry_side == "BUY" else "open_sell")
        row = DailySignalLogRow(
            timestamp=_ts_str(bar["timestamp"]),
            open=bar_open,
            high=bar_high,
            low=bar_low,
            close=bar_close,
            volume=float(bar["volume"]) if not pd.isna(bar.get("volume", None)) else None,
            entry_signal=entry_signal,
            exit_signal=bool(exit_signal),
            action="|".join(action_parts) if action_parts else "hold",
            position_state=position_state,
            stop_loss_price=round(stop_loss_price, 4) if stop_loss_price else None,
            target_price=round(target_price, 4) if target_price else None,
        )
        daily_log.append(row)
        if not in_position:
            equity_curve.append(EquityPoint(timestamp=_ts_str(bar["timestamp"]), equity=round(current_equity, 4)))

    if in_position and absolute_indices:
        last_bar = df.loc[absolute_indices[-1]]
        _close_position(float(last_bar["close"]), last_bar, "end_of_backtest")

    winning = [t for t in trades if t.pnl > 0]
    losing = [t for t in trades if t.pnl < 0]
    gross_profit = sum(t.pnl for t in winning)
    gross_loss = abs(sum(t.pnl for t in losing))
    net_pnl = sum(t.pnl for t in trades)
    total_brokerage = sum(t.brokerage for t in trades)
    total_trades = len(trades)
    win_rate = (len(winning) / total_trades * 100.0) if total_trades else 0.0
    avg_pnl = net_pnl / total_trades if total_trades else 0.0
    avg_pnl_pct = sum(t.pnl_pct for t in trades) / total_trades if total_trades else 0.0
    profit_factor = round(gross_profit / gross_loss, 4) if gross_loss > 0 else None
    return_pct = (net_pnl / starting_capital * 100.0) if starting_capital else 0.0
    max_dd = _max_drawdown_pct([p.equity for p in equity_curve])

    metrics = InstrumentMetrics(
        starting_capital=round(starting_capital, 4),
        ending_capital=round(capital, 4),
        gross_profit=round(gross_profit, 4),
        gross_loss=round(gross_loss, 4),
        net_pnl=round(net_pnl, 4),
        return_pct=round(return_pct, 4),
        total_trades=total_trades,
        winning_trades=len(winning),
        losing_trades=len(losing),
        win_rate_pct=round(win_rate, 4),
        avg_pnl=round(avg_pnl, 4),
        avg_pnl_pct=round(avg_pnl_pct, 4),
        profit_factor=profit_factor,
        max_drawdown_pct=max_dd,
        total_brokerage=round(total_brokerage, 4),
    )
    warning = None
    if total_trades == 0:
        if entry_signal_hits == 0:
            warning = f"No entry signals were generated for {symbol} in the requested range."
        elif blocked_entry_signals == entry_signal_hits:
            warning = f"Entry signals were generated for {symbol}, but all of them fell outside the allowed session window."
        else:
            warning = f"Entry conditions triggered for {symbol}, but no executable trades were created."
    return InstrumentBacktestResult(
        symbol=symbol,
        bars_processed=len(absolute_indices),
        metrics=metrics,
        trades=trades,
        equity_curve=equity_curve,
        triggered_days=[r for r in daily_log if r.action != "hold"],
        daily_signal_log=daily_log,
        warning=warning,
    )


def _merge_equity_curves(instrument_results: list[InstrumentBacktestResult]) -> list[EquityPoint]:
    curves = []
    for res in instrument_results:
        if res.equity_curve:
            curves.append(pd.Series({p.timestamp: p.equity for p in res.equity_curve}, dtype=float))
    if not curves:
        return []
    combined = pd.concat(curves, axis=1).sort_index().ffill()
    total = combined.sum(axis=1)
    return [EquityPoint(timestamp=str(ts), equity=round(float(eq), 4)) for ts, eq in total.items()]


def render_equity_curve_image(result: dict[str, Any], *, output_root: Path) -> dict[str, Any]:
    import matplotlib.pyplot as plt

    output_root.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.now(tz=IST).strftime("%Y%m%d_%H%M%S")
    image_path = output_root / f"strategy_backtest_curve_{timestamp}.png"
    points = result.get("portfolio", {}).get("equity_curve") or []
    if not points:
        return {"path": "", "exists": False}
    x = [pd.Timestamp(point["timestamp"]) for point in points]
    y = [float(point["equity"]) for point in points]
    fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
    ax.plot(x, y, color="#0b6e4f", linewidth=2.2)
    ax.set_title("Strategy equity curve")
    ax.set_xlabel("Time")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.25, linestyle="--")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(image_path, format="png")
    plt.close(fig)
    return {"path": str(image_path), "exists": True}


def validate_strategy_payload(payload: dict[str, Any]) -> dict[str, Any]:
    strategy = parse_strategy(payload)
    expressions = _all_expressions(strategy)
    return {
        "ok": True,
        "strategy_summary": {
            "instrument_count": len(strategy.instruments),
            "instruments": strategy.instruments,
            "interval": strategy.interval,
            "entry_side": strategy.entry_side,
            "exit_mode": strategy.exit_mode,
            "holding_type": strategy.holding_type,
            "execution_style": strategy.execution_style,
            "initial_capital": strategy.initial_capital,
            "start_date": strategy.start_date,
            "end_date": strategy.end_date,
        },
        "catalog_summary": {
            "indicator_expression_count": len(expressions),
            "warmup_bars_estimate": _warmup_bars_for(expressions, strategy.interval),
        },
    }


def run_strategy_backtest(service: Any, payload: dict[str, Any]) -> dict[str, Any]:
    strategy = parse_strategy(payload)
    expressions = _all_expressions(strategy)
    warmup_bars = _warmup_bars_for(expressions, strategy.interval)
    instrument_results: list[InstrumentBacktestResult] = []
    for symbol in strategy.instruments:
        df = service._historical_to_df(
            symbol,
            timeframe=strategy.interval,
            start_date=strategy.start_date,
            end_date=strategy.end_date,
            exchange=strategy.exchange,
            instrument_type=strategy.instrument_type,
            warmup_bars=warmup_bars,
        )
        enriched = inject_indicator_columns(service, df, expressions)
        instrument_results.append(_run_instrument(strategy, enriched, symbol))

    total_starting = strategy.initial_capital
    total_ending = sum(r.metrics.ending_capital for r in instrument_results)
    total_gross_profit = sum(r.metrics.gross_profit for r in instrument_results)
    total_gross_loss = sum(r.metrics.gross_loss for r in instrument_results)
    total_net_pnl = sum(r.metrics.net_pnl for r in instrument_results)
    total_brokerage = sum(r.metrics.total_brokerage for r in instrument_results)
    total_trades = sum(r.metrics.total_trades for r in instrument_results)
    total_winning = sum(r.metrics.winning_trades for r in instrument_results)
    total_losing = sum(r.metrics.losing_trades for r in instrument_results)
    portfolio_equity_curve = _merge_equity_curves(instrument_results)
    portfolio = {
        "starting_capital": round(total_starting, 4),
        "ending_capital": round(total_ending, 4),
        "gross_profit": round(total_gross_profit, 4),
        "gross_loss": round(total_gross_loss, 4),
        "net_pnl": round(total_net_pnl, 4),
        "return_pct": round((total_net_pnl / total_starting * 100.0) if total_starting else 0.0, 4),
        "total_trades": total_trades,
        "winning_trades": total_winning,
        "losing_trades": total_losing,
        "win_rate_pct": round((total_winning / total_trades * 100.0) if total_trades else 0.0, 4),
        "profit_factor": round(total_gross_profit / total_gross_loss, 4) if total_gross_loss > 0 else None,
        "max_drawdown_pct": _max_drawdown_pct([p.equity for p in portfolio_equity_curve]) if portfolio_equity_curve else 0.0,
        "capital_per_instrument": round(strategy.capital_per_instrument, 4),
        "total_brokerage": round(total_brokerage, 4),
        "equity_curve": [asdict(point) for point in portfolio_equity_curve],
    }
    result = {
        "mode": "backtest",
        "strategy_summary": {
            "instruments": strategy.instruments,
            "interval": strategy.interval,
            "entry_side": strategy.entry_side,
            "exit_mode": strategy.exit_mode,
            "allocation_mode": strategy.allocation_mode,
            "holding_type": strategy.holding_type,
            "execution_style": strategy.execution_style,
            "initial_capital": strategy.initial_capital,
            "start_date": strategy.start_date,
            "end_date": strategy.end_date,
        },
        "portfolio": portfolio,
        "instruments": [asdict(result_item) for result_item in instrument_results],
    }
    result["equity_curve_image"] = render_equity_curve_image(
        result,
        output_root=Path(__file__).resolve().parent / "artifacts" / "backtests",
    )
    return result
