from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from pydantic import BaseModel, Field

from config import Settings

logger = logging.getLogger(__name__)

AUTH_GUIDANCE_MESSAGE = (
    "Session expired or missing. First ask the user for phone number and call send_otp. "
    "Then ask for the OTP and call verify_otp. Then ask for the MPIN and call verify_mpin. "
    "After authentication completes, continue the original task."
)


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", value.strip().upper())


def _lookup_tokens(value: str) -> set[str]:
    normalized = value.strip().upper()
    if not normalized:
        return set()
    parts = re.split(r"[^A-Z0-9]+", normalized)
    tokens = {part for part in parts if part}
    compact = _normalize_lookup_text(normalized)
    if compact:
        tokens.add(compact)
    return tokens


def _candidate_strings(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for value in row.values():
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                values.append(stripped)
    return values

PAISE_PRICE_KEYS = {
    "p",
    "price",
    "ltp",
    "ltpchg",
    "cp",
    "sp",
    "atm",
    "v",
    "value",
    "last_traded_price",
    "last_traded_quantity",
    "current_price",
    "strike_price",
    "order_price",
    "avg_filled_price",
    "last_traded_price_change",
    "avg_price",
    "avg_buy_price",
    "avg_sell_price",
    "pnl",
    "realised_pnl",
    "unrealised_pnl",
    "total_pnl",
    "benchmark_price",
    "cleanup_price",
    "trigger_price",
    "min_prate",
    "max_prate",
    "brokerage",
    "underlying_prev_close",
}

PERCENT_KEYS = {"ltpchg", "last_traded_price_change", "pnl_chg", "total_pnl_chg"}
PRICE_EXCLUDE_KEYS = {
    "ref_id",
    "inst_id",
    "token",
    "qty",
    "order_qty",
    "filled_qty",
    "buy_quantity",
    "sell_quantity",
    "lot_size",
    "volume",
    "oi",
    "open_interest",
    "cumulative_oi",
    "cumulative_call_oi",
    "cumulative_put_oi",
    "cumulative_fut_oi",
    "cumulative_volume",
    "cumulative_volume_premium",
    "cumulative_volume_delta",
    "tick_volume",
    "ltq",
    "last_traded_quantity",
    "quantity",
    "num_orders",
    "o",
    "q",
    "ts",
    "timestamp",
    "time",
    "order_id",
    "exchange_order_id",
    "basket_id",
}
TIME_KEYS = {
    "ts",
    "timestamp",
    "order_time",
    "ack_time",
    "filled_time",
    "last_modified",
}

MONTH_CODE_MONTHLY = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}


def _format_option_strike(strike: Any) -> str:
    value = float(strike)
    if value.is_integer():
        return str(int(value))
    return str(value)

ALLOWED_INTERVALS = {"1m", "2m", "3m", "5m", "15m", "30m", "1h", "1d", "1w", "1mt"}


def _is_price_series(parent_key: str | None, key: str) -> bool:
    if key in PRICE_EXCLUDE_KEYS or key in PERCENT_KEYS:
        return False
    if parent_key in {"open", "high", "low", "close", "l1bid", "l1ask"} and key == "v":
        return True
    if parent_key in {
        "cumulative_volume",
        "tick_volume",
        "cumulative_volume_premium",
        "cumulative_volume_delta",
        "cumulative_oi",
        "cumulative_call_oi",
        "cumulative_put_oi",
        "cumulative_fut_oi",
        "volume",
        "oi",
    } and key == "v":
        return False
    if parent_key in {"theta", "delta", "gamma", "vega", "iv", "iv_mid", "iv_bid", "iv_ask"} and key == "v":
        return False
    return key in PAISE_PRICE_KEYS


def _convert_paise_value(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value / 100
    if isinstance(value, float):
        return round(value / 100, 4)
    return value


def _convert_rupees_input_to_paise(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value * 100
    if isinstance(value, float):
        return int(round(value * 100))
    return value


def _pct_change(current: float | int | None, previous: float | int | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    try:
        return round(((float(current) / float(previous)) - 1.0) * 100.0, 4)
    except Exception:
        return None


def convert_paise_to_rupees(payload: Any, *, key: str | None = None, parent_key: str | None = None) -> Any:
    if isinstance(payload, dict):
        return {
            item_key: convert_paise_to_rupees(item_value, key=item_key, parent_key=key)
            for item_key, item_value in payload.items()
        }
    if isinstance(payload, list):
        return [convert_paise_to_rupees(item, key=key, parent_key=parent_key) for item in payload]
    if _is_price_series(parent_key, key or ""):
        return _convert_paise_value(payload)
    return payload


def _ns_epoch_to_ist(value: int | float) -> str:
    dt = datetime.fromtimestamp(float(value) / 1_000_000_000, tz=timezone.utc).astimezone(ZoneInfo("Asia/Kolkata"))
    return dt.isoformat()


def _iso_utc_to_ist(value: str) -> str | None:
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ZoneInfo("Asia/Kolkata")).isoformat()
    except Exception:
        return None


def add_ist_time_fields(payload: Any) -> Any:
    if isinstance(payload, dict):
        output: dict[str, Any] = {}
        for item_key, item_value in payload.items():
            converted_value = add_ist_time_fields(item_value)
            output[item_key] = converted_value
            if item_key in TIME_KEYS and isinstance(item_value, (int, float)):
                output[f"{item_key}_ist"] = _ns_epoch_to_ist(item_value)
            if item_key in {"market_time"} and isinstance(item_value, str):
                ist_value = _iso_utc_to_ist(item_value)
                if ist_value:
                    output[f"{item_key}_ist"] = ist_value
        return output
    if isinstance(payload, list):
        return [add_ist_time_fields(item) for item in payload]
    return payload


class NubraAPIError(Exception):
    """Raised when Nubra returns a non-success response or invalid payload."""

    def __init__(self, message: str, *, status_code: int | None = None, details: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.details = details


READ_ONLY_TRADING_MESSAGE = "Trading actions are disabled in this MCP. Order placement, cancellation, and square-off are blocked."


class HistoricalQuery(BaseModel):
    exchange: str = "NSE"
    type: str = Field(default="INDEX", description="STOCK, INDEX, OPT, FUT")
    values: list[str]
    fields: list[str] = Field(default_factory=lambda: ["open", "high", "low", "close", "cumulative_volume"])
    startDate: str
    endDate: str
    interval: str = "1m"
    intraDay: bool = False
    realTime: bool = False


def _normalize_nubra_timestamp(value: str, *, is_end: bool) -> str:
    text = value.strip()
    if not text:
        raise ValueError("Timestamp cannot be empty.")
    if "T" in text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "Timestamp must be ISO-8601 UTC like 2026-03-10T03:45:00.000Z."
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    try:
        parsed_date = datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            "Date must be YYYY-MM-DD or full ISO-8601 UTC like 2026-03-10T03:45:00.000Z."
        ) from exc
    if is_end:
        parsed_date = parsed_date + timedelta(days=1) - timedelta(milliseconds=1)
    return parsed_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _normalize_instrument_type(value: str) -> str:
    normalized = value.strip().upper()
    aliases = {
        "OPTION": "OPT",
        "OPTIONS": "OPT",
        "OPT": "OPT",
        "FUTURE": "FUT",
        "FUTURES": "FUT",
        "FUT": "FUT",
        "STOCK": "STOCK",
        "STOCKS": "STOCK",
        "INDEX": "INDEX",
        "INDICES": "INDEX",
    }
    if normalized not in aliases:
        raise ValueError("instrument_type must be one of INDEX, STOCK, OPT, FUT.")
    return aliases[normalized]


class OrderRequest(BaseModel):
    ref_id: int | None = None
    symbol: str | None = None
    exchange: str = "NSE"
    order_type: str = "ORDER_TYPE_REGULAR"
    order_qty: int = Field(gt=0)
    order_side: str = Field(description="ORDER_SIDE_BUY or ORDER_SIDE_SELL")
    order_delivery_type: str = "ORDER_DELIVERY_TYPE_IDAY"
    validity_type: str = "DAY"
    price_type: str = "MARKET"
    order_price: int | None = None
    tag: str | None = None
    algo_params: dict[str, Any] = Field(default_factory=dict)


class InstrumentLookup(BaseModel):
    ref_id: int
    symbol: str
    asset: str
    exchange: str
    derivative_type: str
    option_type: str | None = None
    expiry: int | None = None
    strike_price: int | None = None
    lot_size: int | None = None
    tick_size: int | None = None


@dataclass
class AuthState:
    environment: str = "PROD"
    phone: str | None = None
    device_id: str | None = None
    temp_token: str | None = None
    auth_token: str | None = None
    session_token: str | None = None
    authenticated: bool = False
    last_login_at: str | None = None


class NubraClient:
    """REST-backed wrapper around Nubra authentication, market data, and trading APIs."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.state_file = Path(settings.auth_state_file)
        self.state = self._load_state()
        self._instrument_cache: dict[str, list[dict[str, Any]]] = {}
        self._index_master_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._historical_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.session = requests.Session()

    def _load_state(self) -> AuthState:
        if not self.state_file.exists():
            return AuthState(environment=self.settings.environment)
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
            return AuthState(**payload)
        except Exception:
            return AuthState(environment=self.settings.environment)

    def _save_state(self) -> None:
        self.state_file.write_text(json.dumps(asdict(self.state), indent=2), encoding="utf-8")

    def _base_url(self) -> str:
        return "https://api.nubra.io" if self.state.environment == "PROD" else "https://uatapi.nubra.io"

    def _device_id(self) -> str:
        if not self.state.device_id:
            self.state.device_id = f"{uuid.uuid4()}-nubra-mcp"
            self._save_state()
        return self.state.device_id

    def _headers(self, *, use_auth_token: bool = False, use_session_token: bool = False) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "x-device-id": self._device_id(),
            "Accept": "application/json",
        }
        if use_auth_token:
            if not self.state.auth_token:
                raise NubraAPIError("Authentication token missing. Verify OTP first.")
            headers["Authorization"] = f"Bearer {self.state.auth_token}"
        if use_session_token:
            if not self.state.session_token:
                raise NubraAPIError(AUTH_GUIDANCE_MESSAGE)
            headers["Authorization"] = f"Bearer {self.state.session_token}"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self._base_url().rstrip('/')}/{path.lstrip('/')}"
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=20,
            )
        except requests.RequestException as exc:
            raise NubraAPIError(f"Failed to reach Nubra API: {exc}") from exc

        try:
            payload = response.json() if response.content else {}
        except ValueError:
            payload = {"raw": response.text}

        if not response.ok:
            if response.status_code == 401:
                self.state.session_token = None
                self.state.authenticated = False
                self._save_state()
                raise NubraAPIError(
                    AUTH_GUIDANCE_MESSAGE,
                    status_code=response.status_code,
                    details=payload,
                )
            raise NubraAPIError(
                f"Nubra API returned HTTP {response.status_code}",
                status_code=response.status_code,
                details=payload,
            )
        if not isinstance(payload, (dict, list)):
            raise NubraAPIError("Unexpected Nubra response format", details=payload)
        return payload

    def auth_status(self) -> dict[str, Any]:
        session_active = bool(self.state.session_token and self.state.authenticated)
        return {
            "authenticated": self.state.authenticated,
            "session_active": session_active,
            "requires_login": not session_active,
            "environment": self.state.environment,
            "phone": self.state.phone,
            "device_id": self.state.device_id,
            "last_login_at": self.state.last_login_at,
            "has_temp_token": bool(self.state.temp_token),
            "has_auth_token": bool(self.state.auth_token),
            "has_session_token": bool(self.state.session_token),
            "agent_guidance": (
                "Before using protected tools, call auth_status. If requires_login is true, "
                "ask for phone number, then OTP, then MPIN, and call send_otp, verify_otp, verify_mpin in that order."
            ),
        }

    def set_environment(self, environment: str) -> dict[str, Any]:
        env = environment.strip().upper()
        if env not in {"PROD", "UAT"}:
            raise NubraAPIError("Environment must be PROD or UAT")
        self.state = AuthState(environment=env, phone=self.state.phone)
        self._instrument_cache.clear()
        self._save_state()
        return self.auth_status()

    def send_otp(self, phone: str | None = None, environment: str | None = None) -> dict[str, Any]:
        if environment:
            self.set_environment(environment)
        target_phone = (phone or self.state.phone or self.settings.phone).strip()
        first = self._request(
            "POST",
            "sendphoneotp",
            json_body={"phone": target_phone, "flow": "", "skip_totp": False},
            headers={"Content-Type": "application/json"},
        )
        first_temp_token = first.get("temp_token")
        next_step = first.get("next")
        if not first_temp_token or not next_step:
            raise NubraAPIError("Unexpected send OTP response", details=first)

        if next_step == "VERIFY_TOTP":
            second = self._request(
                "POST",
                "sendphoneotp",
                json_body={"phone": target_phone, "flow": "", "skip_totp": True},
                headers={"Content-Type": "application/json", "x-temp-token": str(first_temp_token)},
            )
            first_temp_token = second.get("temp_token")
            if not first_temp_token:
                raise NubraAPIError("Unexpected TOTP OTP response", details=second)

        self.state.phone = target_phone
        self.state.temp_token = str(first_temp_token)
        self.state.auth_token = None
        self.state.session_token = None
        self.state.authenticated = False
        self._save_state()
        return {
            "message": "OTP sent. Ask the user for the OTP and call verify_otp next. After OTP verification, ask for MPIN and call verify_mpin.",
            "environment": self.state.environment,
            "phone": self.state.phone,
            "device_id": self._device_id(),
            "next_step": "verify_otp",
        }

    def begin_auth_flow(self, phone: str, environment: str | None = None) -> dict[str, Any]:
        payload = self.send_otp(phone=phone, environment=environment)
        return {
            "message": "Authentication started. OTP has been sent to the user's phone number.",
            "environment": payload.get("environment"),
            "phone": payload.get("phone"),
            "device_id": payload.get("device_id"),
            "current_step": "otp_sent",
            "next_step": "ask_for_otp",
            "agent_guidance": "Ask the user for the OTP and call verify_otp. After that, ask for MPIN and call verify_mpin.",
        }

    def verify_otp(self, otp: str, phone: str | None = None) -> dict[str, Any]:
        target_phone = (phone or self.state.phone or self.settings.phone).strip()
        if not self.state.temp_token:
            raise NubraAPIError("No temp token found. Ask for phone number first and call send_otp before verify_otp.")
        payload = self._request(
            "POST",
            "verifyphoneotp",
            json_body={"phone": target_phone, "otp": otp.strip()},
            headers={
                "Content-Type": "application/json",
                "x-device-id": self._device_id(),
                "x-temp-token": self.state.temp_token,
            },
        )
        auth_token = payload.get("auth_token")
        if not auth_token:
            raise NubraAPIError("OTP verified but auth_token missing", details=payload)
        self.state.phone = target_phone
        self.state.auth_token = str(auth_token)
        self._save_state()
        return {
            "message": "OTP verified. Ask the user for MPIN and call verify_mpin next.",
            "environment": self.state.environment,
            "phone": self.state.phone,
            "next_step": "verify_mpin",
        }

    def verify_mpin(self, mpin: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "verifypin",
            json_body={"pin": mpin.strip()},
            headers=self._headers(use_auth_token=True),
        )
        session_token = payload.get("session_token")
        if not session_token:
            raise NubraAPIError("MPIN verified but session_token missing", details=payload)
        self.state.session_token = str(session_token)
        self.state.authenticated = True
        self.state.last_login_at = datetime.utcnow().isoformat() + "Z"
        self._save_state()
        return {
            "message": "Authentication complete. Continue the original user request.",
            "environment": self.state.environment,
            "phone": self.state.phone,
            "authenticated": True,
            "next_step": "resume_original_task",
        }

    def logout(self) -> dict[str, Any]:
        if self.state.session_token:
            try:
                self._request("POST", "logout", headers=self._headers(use_session_token=True))
            except Exception:
                logger.info("Logout API call failed; clearing local auth state anyway.")
        phone = self.state.phone
        environment = self.state.environment
        self.state = AuthState(environment=environment, phone=phone)
        self._instrument_cache.clear()
        self._save_state()
        return {"message": "Logged out.", "environment": environment}

    def _ensure_authenticated(self) -> None:
        if not self.state.session_token:
            raise NubraAPIError(AUTH_GUIDANCE_MESSAGE)

    def get_instruments(self, exchange: str | None = None) -> list[dict[str, Any]]:
        self._ensure_authenticated()
        exchange_name = (exchange or self.settings.default_exchange).upper()
        today = datetime.today().strftime("%Y-%m-%d")
        cache_key = f"{exchange_name}:{today}"
        if cache_key in self._instrument_cache:
            return self._instrument_cache[cache_key]
        payload = self._request(
            "GET",
            f"refdata/refdata/{today}",
            params={"exchange": exchange_name},
            headers=self._headers(use_session_token=True),
        )
        refdata = payload.get("refdata") or []
        if not isinstance(refdata, list):
            raise NubraAPIError("Invalid refdata response", details=payload)
        self._instrument_cache[cache_key] = refdata
        return refdata

    def get_index_master(self) -> list[dict[str, Any]]:
        cached = self._index_master_cache
        if cached and (time.time() - cached[0] < 3600):
            return cached[1]

        try:
            response = self.session.get("https://api.nubra.io/public/indexes?format=csv", timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise NubraAPIError(f"Failed to fetch Nubra index master: {exc}") from exc

        rows = [dict(row) for row in csv.DictReader(io.StringIO(response.text))]
        self._index_master_cache = (time.time(), rows)
        return rows

    def get_instrument_by_ref_id(self, ref_id: int, exchange: str | None = None) -> dict[str, Any]:
        exchange_name = (exchange or self.settings.default_exchange).upper()
        for item in self.get_instruments(exchange_name):
            if int(item.get("ref_id")) == int(ref_id):
                return item
        raise NubraAPIError(f"Unable to resolve ref_id '{ref_id}' on {exchange_name}")

    def get_instrument_by_symbol(self, symbol: str, exchange: str | None = None) -> dict[str, Any]:
        return self.resolve_symbol(symbol, exchange=exchange).model_dump()

    def resolve_symbol(
        self,
        symbol: str,
        *,
        exchange: str | None = None,
        derivative_type: str | None = None,
    ) -> InstrumentLookup:
        target = symbol.strip().upper()
        exchange_name = (exchange or self.settings.default_exchange).upper()
        matches: list[dict[str, Any]] = []
        for item in self.get_instruments(exchange_name):
            stock_name = str(item.get("stock_name", "")).strip().upper()
            asset = str(item.get("asset", "")).strip().upper()
            if target not in {stock_name, asset}:
                continue
            if derivative_type and str(item.get("derivative_type", "")).upper() != derivative_type.upper():
                continue
            matches.append(item)

        if not matches:
            raise NubraAPIError(f"Unable to resolve symbol '{symbol}' on {exchange_name}")

        def _score(item: dict[str, Any]) -> tuple[int, int]:
            exact_symbol = 0 if str(item.get("stock_name", "")).strip().upper() == target else 1
            non_derivative = 0 if not item.get("expiry") else 1
            return (exact_symbol, non_derivative)

        best = sorted(matches, key=_score)[0]
        return InstrumentLookup(
            ref_id=int(best["ref_id"]),
            symbol=str(best.get("stock_name") or target),
            asset=str(best.get("asset") or target),
            exchange=str(best.get("exchange") or exchange_name),
            derivative_type=str(best.get("derivative_type") or ""),
            option_type=(str(best.get("option_type")) if best.get("option_type") else None),
            expiry=int(best["expiry"]) if best.get("expiry") else None,
            strike_price=int(best["strike_price"]) if best.get("strike_price") else None,
            lot_size=int(best["lot_size"]) if best.get("lot_size") else None,
            tick_size=int(best["tick_size"]) if best.get("tick_size") else None,
        )

    def get_quote(self, ref_id: int, *, levels: int = 5) -> dict[str, Any]:
        payload = self._request(
            "GET",
            f"orderbooks/{ref_id}",
            params={"levels": levels},
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_current_price(self, symbol: str, *, exchange: str = "NSE") -> dict[str, Any]:
        exchange_name = exchange.strip().upper()
        params: dict[str, Any] = {}
        if exchange_name and exchange_name != "NSE":
            params["exchange"] = exchange_name
        payload = self._request(
            "GET",
            f"optionchains/{symbol.strip().upper()}/price",
            params=params or None,
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_option_chain(self, symbol: str, *, exchange: str = "NSE", expiry: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"exchange": exchange.upper()}
        if expiry:
            params["expiry"] = expiry
        payload = self._request(
            "GET",
            f"optionchains/{symbol.upper()}",
            params=params,
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_historical_data(self, query: HistoricalQuery) -> dict[str, Any]:
        query_payload = query.model_dump()
        values = list(query_payload["values"])
        batched_values = [values[index:index + 5] for index in range(0, len(values), 5)]
        combined_results: list[dict[str, Any]] = []
        market_time: str | None = None
        message: str | None = None

        for batch in batched_values:
            batch_result = self._get_historical_batch({**query_payload, "values": batch})
            market_time = market_time or batch_result.get("market_time")
            message = message or batch_result.get("message")
            combined_results.extend(batch_result.get("result") or [])

        return {
            "market_time": market_time,
            "message": message or "charts",
            "result": combined_results,
        }

    def _get_historical_batch(self, query_payload: dict[str, Any]) -> dict[str, Any]:
        cached_values: list[dict[str, Any]] = []
        missing_symbols: list[str] = []

        for symbol in query_payload["values"]:
            cache_key = self._historical_cache_key(query_payload, symbol)
            cached_entry = self._historical_cache.get(cache_key)
            if cached_entry and (time.time() - cached_entry[0] < 10):
                cached_values.append(cached_entry[1])
            else:
                missing_symbols.append(symbol)

        fetched_values: list[dict[str, Any]] = []
        market_time: str | None = None
        message: str | None = None
        if missing_symbols:
            api_payload = {**query_payload, "values": missing_symbols}
            raw_payload = self._request(
                "POST",
                "charts/timeseries",
                json_body={"query": [api_payload]},
                headers=self._headers(use_session_token=True),
            )
            converted_payload = add_ist_time_fields(convert_paise_to_rupees(raw_payload))
            market_time = converted_payload.get("market_time")
            message = converted_payload.get("message")
            result_list = converted_payload.get("result") or []
            for result in result_list:
                for symbol_entry in result.get("values") or []:
                    if not isinstance(symbol_entry, dict):
                        continue
                    fetched_values.append(symbol_entry)
                    symbol_name = next(iter(symbol_entry.keys()))
                    self._historical_cache[self._historical_cache_key(query_payload, symbol_name)] = (
                        time.time(),
                        symbol_entry,
                    )

        all_values = cached_values + fetched_values
        return {
            "market_time": market_time,
            "message": message or "charts",
            "result": [
                {
                    "exchange": query_payload["exchange"],
                    "type": query_payload["type"],
                    "values": all_values,
                }
            ],
        }

    def _historical_cache_key(self, query_payload: dict[str, Any], symbol: str) -> str:
        parts = {
            "exchange": query_payload.get("exchange"),
            "type": query_payload.get("type"),
            "fields": tuple(query_payload.get("fields") or []),
            "interval": query_payload.get("interval"),
            "intraDay": query_payload.get("intraDay"),
            "startDate": query_payload.get("startDate"),
            "endDate": query_payload.get("endDate"),
            "symbol": symbol,
        }
        return json.dumps(parts, sort_keys=True)

    def place_order(self, order: OrderRequest) -> dict[str, Any]:
        raise NubraAPIError(READ_ONLY_TRADING_MESSAGE)

    def cancel_order(self, order_id: int) -> dict[str, Any]:
        raise NubraAPIError(READ_ONLY_TRADING_MESSAGE)

    def get_orders(
        self,
        *,
        live: bool = False,
        executed: bool = False,
        tag: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if live:
            params["live"] = 1
        if executed:
            params["executed"] = 1
        if tag:
            params["tag"] = tag
        payload = self._request(
            "GET",
            "orders/v2",
            params=params or None,
            headers=self._headers(use_session_token=True),
        )
        converted = add_ist_time_fields(convert_paise_to_rupees(payload))
        if not isinstance(converted, list):
            raise NubraAPIError("Unexpected orders response format", details=converted)
        return converted

    def get_positions(self) -> dict[str, Any]:
        payload = self._request(
            "GET",
            "portfolio/positions",
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_margin(self, payload_body: dict[str, Any]) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "orders/v2/margin_required",
            json_body=payload_body,
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_holdings(self) -> dict[str, Any]:
        payload = self._request(
            "GET",
            "portfolio/holdings",
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))

    def get_funds(self) -> dict[str, Any]:
        payload = self._request(
            "GET",
            "portfolio/user_funds_and_margin",
            headers=self._headers(use_session_token=True),
        )
        return add_ist_time_fields(convert_paise_to_rupees(payload))


class NubraService:
    """High-level service methods used by MCP tool handlers."""

    def __init__(self, client: NubraClient) -> None:
        self.client = client

    def _raise_read_only_trading(self) -> None:
        raise NubraAPIError(READ_ONLY_TRADING_MESSAGE)

    def auth_status(self) -> dict[str, Any]:
        return self.client.auth_status()

    def set_environment(self, environment: str) -> dict[str, Any]:
        return self.client.set_environment(environment)

    def send_otp(self, phone: str | None = None, environment: str | None = None) -> dict[str, Any]:
        return self.client.send_otp(phone=phone, environment=environment)

    def begin_auth_flow(self, phone: str, environment: str | None = None) -> dict[str, Any]:
        return self.client.begin_auth_flow(phone=phone, environment=environment)

    def verify_otp(self, otp: str, phone: str | None = None) -> dict[str, Any]:
        return self.client.verify_otp(otp=otp, phone=phone)

    def verify_mpin(self, mpin: str) -> dict[str, Any]:
        return self.client.verify_mpin(mpin)

    def logout(self) -> dict[str, Any]:
        return self.client.logout()

    def get_orders(
        self,
        *,
        live: bool = False,
        executed: bool = False,
        tag: str | None = None,
    ) -> dict[str, Any]:
        orders = self.client.get_orders(live=live, executed=executed, tag=tag)
        return {
            "count": len(orders),
            "live": live,
            "executed": executed,
            "tag": tag,
            "orders": orders,
        }

    def get_margin(
        self,
        *,
        exchange: str,
        orders: list[dict[str, Any]],
        with_portfolio: bool = True,
        with_legs: bool = False,
        is_basket: bool = False,
        basket_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "with_portfolio": with_portfolio,
            "with_legs": with_legs,
            "is_basket": is_basket,
            "order_req": {
                "exchange": exchange,
                "orders": orders,
            },
        }
        if basket_params:
            payload["order_req"]["basket_params"] = basket_params
        return self.client.get_margin(payload)

    def get_instrument_details(self, symbol: str, *, exchange: str = "NSE") -> dict[str, Any]:
        instrument = self.client.get_instrument_by_symbol(symbol, exchange=exchange)
        return {
            "symbol": symbol.strip().upper(),
            "exchange": exchange.upper(),
            "instrument": instrument,
            "ref_id": instrument.get("ref_id"),
            "tick_size": instrument.get("tick_size"),
            "lot_size": instrument.get("lot_size"),
            "nubra_name": instrument.get("nubra_name") or instrument.get("zanskar_name"),
            "derivative_type": instrument.get("derivative_type"),
            "asset_type": instrument.get("asset_type"),
        }

    def find_instruments(
        self,
        *,
        exchange: str = "NSE",
        symbol: str | None = None,
        asset: str | None = None,
        derivative_type: str | None = None,
        option_type: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        rows = self.client.get_instruments(exchange)
        target_symbol = symbol.strip().upper() if symbol else None
        target_asset = asset.strip().upper() if asset else None
        target_derivative_type = derivative_type.strip().upper() if derivative_type else None
        target_option_type = option_type.strip().upper() if option_type else None
        filtered: list[dict[str, Any]] = []

        for item in rows:
            stock_name = str(item.get("stock_name") or "").strip().upper()
            asset_name = str(item.get("asset") or "").strip().upper()
            item_derivative_type = str(item.get("derivative_type") or "").strip().upper()
            item_option_type = str(item.get("option_type") or "").strip().upper()

            if target_symbol and target_symbol not in {stock_name, asset_name} and target_symbol not in stock_name:
                continue
            if target_asset and target_asset != asset_name:
                continue
            if target_derivative_type and target_derivative_type != item_derivative_type:
                continue
            if target_option_type and target_option_type != item_option_type:
                continue
            filtered.append(item)

        return {
            "count": len(filtered),
            "exchange": exchange.upper(),
            "matches": filtered[:max(1, limit)],
        }

    def find_index_details(
        self,
        query: str,
        *,
        exchange: str = "NSE",
        limit: int = 10,
        instrument_limit: int = 10,
    ) -> dict[str, Any]:
        target = query.strip().upper()
        normalized_target = _normalize_lookup_text(query)
        target_tokens = _lookup_tokens(query)
        rows = self.client.get_index_master()

        def _row_strings(row: dict[str, Any]) -> list[str]:
            return _candidate_strings(row)

        def _score(row: dict[str, Any]) -> tuple[int, int, int, int, int]:
            values = _row_strings(row)
            uppercase_values = [value.upper() for value in values]
            normalized_values = [_normalize_lookup_text(value) for value in values]
            row_tokens: set[str] = set()
            for value in values:
                row_tokens.update(_lookup_tokens(value))

            exact = 0 if target in uppercase_values or normalized_target in normalized_values else 1
            prefix = 0 if any(
                value.startswith(target) or normalized.startswith(normalized_target)
                for value, normalized in zip(uppercase_values, normalized_values, strict=False)
            ) else 1
            contains = 0 if any(
                target in value or normalized_target in normalized
                for value, normalized in zip(uppercase_values, normalized_values, strict=False)
            ) else 1
            shared_tokens = len(target_tokens & row_tokens)
            token_penalty = 0 if shared_tokens > 0 else 1
            return (exact, prefix, contains, token_penalty, -shared_tokens)

        def _matches(row: dict[str, Any]) -> bool:
            values = _row_strings(row)
            uppercase_values = [value.upper() for value in values]
            normalized_values = [_normalize_lookup_text(value) for value in values]
            row_tokens: set[str] = set()
            for value in values:
                row_tokens.update(_lookup_tokens(value))
            if target in uppercase_values or normalized_target in normalized_values:
                return True
            if any(target in value or normalized_target in normalized for value, normalized in zip(uppercase_values, normalized_values, strict=False)):
                return True
            return bool(target_tokens & row_tokens)

        matches = [row for row in rows if _matches(row)]
        ranked = sorted(matches, key=_score)
        top_matches = ranked[:max(1, limit)]

        def _canonical_name(row: dict[str, Any]) -> str | None:
            for key in ("name", "index_name", "symbol", "trading_symbol", "asset", "display_name"):
                value = row.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            values = _candidate_strings(row)
            return values[0] if values else None

        related_instruments: list[dict[str, Any]] = []
        instrument_lookup_note: str | None = None
        alias_pool: set[str] = {query.strip()}
        if top_matches:
            for row in top_matches:
                alias_pool.update(_candidate_strings(row))
            best_name = _canonical_name(top_matches[0])
            if best_name:
                alias_pool.add(best_name)

        normalized_aliases = {_normalize_lookup_text(value) for value in alias_pool if value.strip()}
        alias_tokens: set[str] = set()
        for value in alias_pool:
            alias_tokens.update(_lookup_tokens(value))

        def _instrument_score(item: dict[str, Any]) -> tuple[int, int, int, int]:
            stock_name = str(item.get("stock_name") or "").strip()
            asset_name = str(item.get("asset") or "").strip()
            nubra_name = str(item.get("nubra_name") or item.get("zanskar_name") or "").strip()
            values = [value for value in (stock_name, asset_name, nubra_name) if value]
            normalized_values = [_normalize_lookup_text(value) for value in values]
            row_tokens: set[str] = set()
            for value in values:
                row_tokens.update(_lookup_tokens(value))

            exact = 0 if any(value in normalized_aliases for value in normalized_values) else 1
            prefix = 0 if any(
                any(value.startswith(alias) or alias.startswith(value) for alias in normalized_aliases)
                for value in normalized_values
            ) else 1
            token_overlap = len(alias_tokens & row_tokens)
            contains = 0 if token_overlap > 0 else 1
            shortest = min((len(value) for value in normalized_values if any(alias in value or value in alias for alias in normalized_aliases)), default=10_000)
            return (exact, prefix, contains, -token_overlap, shortest)

        try:
            instruments = self.client.get_instruments(exchange)
            candidates = []
            for item in instruments:
                stock_name = str(item.get("stock_name") or "").strip()
                asset_name = str(item.get("asset") or "").strip()
                nubra_name = str(item.get("nubra_name") or item.get("zanskar_name") or "").strip()
                values = [value for value in (stock_name, asset_name, nubra_name) if value]
                normalized_values = [_normalize_lookup_text(value) for value in values]
                item_tokens: set[str] = set()
                for value in values:
                    item_tokens.update(_lookup_tokens(value))
                if any(value in normalized_aliases for value in normalized_values):
                    candidates.append(item)
                    continue
                if any(any(alias in value or value in alias for alias in normalized_aliases) for value in normalized_values):
                    candidates.append(item)
                    continue
                if alias_tokens & item_tokens:
                    candidates.append(item)
            related_instruments = sorted(candidates, key=_instrument_score)[:max(1, instrument_limit)]
        except NubraAPIError as exc:
            instrument_lookup_note = f"Instrument master comparison unavailable: {exc}"

        return {
            "count": len(matches),
            "query": query,
            "exchange": exchange.upper(),
            "normalized_query": normalized_target,
            "best_match": top_matches[0] if top_matches else None,
            "best_match_name": _canonical_name(top_matches[0]) if top_matches else None,
            "matches": top_matches,
            "related_instruments": related_instruments,
            "instrument_lookup_note": instrument_lookup_note,
        }

    def quote_by_symbol(self, symbol: str, *, exchange: str = "NSE", levels: int = 5) -> dict[str, Any]:
        instrument = self.client.resolve_symbol(symbol, exchange=exchange)
        quote = self.client.get_quote(instrument.ref_id, levels=levels)
        return {
            "instrument": instrument.model_dump(),
            "quote": quote.get("orderBook", quote),
        }

    def current_price_by_symbol(self, symbol: str, *, exchange: str = "NSE") -> dict[str, Any]:
        normalized_symbol = symbol.strip().upper()
        instrument: dict[str, Any] | None = None
        try:
            instrument = self.client.resolve_symbol(normalized_symbol, exchange=exchange).model_dump()
        except NubraAPIError:
            instrument = None

        current_price = self.client.get_current_price(normalized_symbol, exchange=exchange)
        return {
            "symbol": normalized_symbol,
            "exchange": current_price.get("exchange", exchange.upper()),
            "instrument": instrument,
            "current_price": current_price.get("price"),
            "previous_close": current_price.get("prev_close"),
            "percent_change": current_price.get("change"),
            "raw": current_price,
        }

    def yesterday_change(self, symbol: str, *, exchange: str = "NSE") -> dict[str, Any]:
        payload = self.current_price_by_symbol(symbol, exchange=exchange)
        current_price = payload.get("current_price")
        previous_close = payload.get("previous_close")

        absolute_change: float | None = None
        if isinstance(current_price, (int, float)) and isinstance(previous_close, (int, float)):
            absolute_change = round(float(current_price) - float(previous_close), 4)

        if absolute_change is None:
            direction = "unknown"
        elif absolute_change > 0:
            direction = "up"
        elif absolute_change < 0:
            direction = "down"
        else:
            direction = "flat"

        payload.update(
            {
                "absolute_change": absolute_change,
                "direction": direction,
                "reference_note": "Change is measured against the previous trading session close returned by Nubra current price.",
            }
        )
        return payload

    def option_chain(self, symbol: str, *, exchange: str = "NSE", expiry: str | None = None) -> dict[str, Any]:
        chain = self.client.get_option_chain(symbol, exchange=exchange, expiry=expiry)
        payload = chain.get("chain", {})
        return {
            "asset": payload.get("asset", symbol.upper()),
            "exchange": payload.get("exchange", exchange),
            "expiry": payload.get("expiry"),
            "atm": payload.get("atm"),
            "current_price": payload.get("cp"),
            "available_expiries": payload.get("all_expiries", []),
            "calls": payload.get("ce", []),
            "puts": payload.get("pe", []),
        }

    def historical_data(
        self,
        symbol: str | list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "INDEX",
        fields: list[str] | None = None,
        intraday: bool | None = None,
    ) -> dict[str, Any]:
        values = [symbol] if isinstance(symbol, str) else symbol
        normalized_timeframe = timeframe.strip().lower()
        if normalized_timeframe not in ALLOWED_INTERVALS:
            allowed = ", ".join(sorted(ALLOWED_INTERVALS))
            raise ValueError(f"timeframe must be one of: {allowed}.")
        use_intraday = intraday if intraday is not None else timeframe not in {"1d", "1w", "1mt"}
        effective_start = _normalize_nubra_timestamp(start_date, is_end=False)
        effective_end = _normalize_nubra_timestamp(end_date, is_end=True)
        query = HistoricalQuery(
            exchange=exchange,
            type=_normalize_instrument_type(instrument_type),
            values=values,
            fields=fields or ["open", "high", "low", "close", "cumulative_volume"],
            startDate=effective_start,
            endDate=effective_end,
            interval=normalized_timeframe,
            intraDay=use_intraday,
            realTime=False,
        )
        return self.client.get_historical_data(query)

    def calculate_option_greeks(
        self,
        symbol: str,
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
    ) -> dict[str, Any]:
        chain = self.option_chain(symbol, exchange=exchange, expiry=expiry)
        calls = chain["calls"]
        puts = chain["puts"]

        def _greek_snapshot(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            for leg in legs:
                rows.append(
                    {
                        "ref_id": leg.get("ref_id"),
                        "strike_price": leg.get("sp"),
                        "last_traded_price": leg.get("ltp"),
                        "iv": leg.get("iv"),
                        "delta": leg.get("delta"),
                        "gamma": leg.get("gamma"),
                        "theta": leg.get("theta"),
                        "vega": leg.get("vega"),
                        "open_interest": leg.get("oi"),
                        "volume": leg.get("volume"),
                    }
                )
            return rows

        return {
            "asset": chain["asset"],
            "exchange": chain["exchange"],
            "expiry": chain["expiry"],
            "atm": chain["atm"],
            "current_price": chain["current_price"],
            "calls": _greek_snapshot(calls),
            "puts": _greek_snapshot(puts),
        }

    def _resolve_atm_option_symbols(
        self,
        symbols: list[str],
        *,
        exchange: str = "NSE",
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        option_symbols: list[str] = []
        instrument_to_underlying: dict[str, dict[str, Any]] = {}
        for underlying in symbols:
            chain = self.option_chain(underlying, exchange=exchange, expiry=None)
            atm_strike = chain.get("atm")
            calls = chain.get("calls") or []
            puts = chain.get("puts") or []
            atm_call = next((leg for leg in calls if leg.get("sp") == atm_strike), None)
            atm_put = next((leg for leg in puts if leg.get("sp") == atm_strike), None)
            for option_type, leg in (("CE", atm_call), ("PE", atm_put)):
                if not leg:
                    continue
                ref_id = leg.get("ref_id")
                if ref_id is None:
                    continue
                instrument = self.client.get_instrument_by_ref_id(int(ref_id), exchange=exchange)
                option_symbol = str(instrument.get("stock_name"))
                option_symbols.append(option_symbol)
                instrument_to_underlying[option_symbol] = {
                    "underlying": underlying,
                    "option_type": option_type,
                    "expiry": chain.get("expiry"),
                    "atm_strike": atm_strike,
                    "ref_id": ref_id,
                }
        return option_symbols, instrument_to_underlying

    def _resolve_atm_straddle_legs(
        self,
        symbol: str,
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
    ) -> dict[str, Any]:
        normalized_symbol = symbol.strip().upper()
        chain = self.option_chain(normalized_symbol, exchange=exchange, expiry=expiry)
        atm_strike = chain.get("atm")
        calls = chain.get("calls") or []
        puts = chain.get("puts") or []

        atm_call = next((leg for leg in calls if leg.get("sp") == atm_strike), None)
        atm_put = next((leg for leg in puts if leg.get("sp") == atm_strike), None)
        if not atm_call or not atm_put:
            raise NubraAPIError(f"Unable to resolve both ATM call and put legs for '{normalized_symbol}' on {exchange}.")

        call_ref_id = atm_call.get("ref_id")
        put_ref_id = atm_put.get("ref_id")
        if call_ref_id is None or put_ref_id is None:
            raise NubraAPIError(f"ATM straddle legs for '{normalized_symbol}' are missing ref_id values.")

        call_instrument = self.client.get_instrument_by_ref_id(int(call_ref_id), exchange=exchange)
        put_instrument = self.client.get_instrument_by_ref_id(int(put_ref_id), exchange=exchange)
        lot_size = int(call_instrument.get("lot_size") or put_instrument.get("lot_size") or 0)
        if lot_size <= 0:
            raise NubraAPIError(f"Unable to determine lot size for ATM straddle on '{normalized_symbol}'.")

        return {
            "underlying": normalized_symbol,
            "exchange": chain.get("exchange", exchange),
            "expiry": chain.get("expiry"),
            "atm_strike": atm_strike,
            "current_price": chain.get("current_price"),
            "lot_size": lot_size,
            "call_leg": {
                "ref_id": int(call_ref_id),
                "symbol": call_instrument.get("stock_name"),
                "option_type": "CE",
                "strike_price": atm_call.get("sp"),
                "last_traded_price": atm_call.get("ltp"),
            },
            "put_leg": {
                "ref_id": int(put_ref_id),
                "symbol": put_instrument.get("stock_name"),
                "option_type": "PE",
                "strike_price": atm_put.get("sp"),
                "last_traded_price": atm_put.get("ltp"),
            },
        }

    def estimate_atm_straddle_margin(
        self,
        symbols: list[str],
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
        lots: int = 1,
        order_side: str = "ORDER_SIDE_SELL",
        order_delivery_type: str = "ORDER_DELIVERY_TYPE_CNC",
        with_portfolio: bool = True,
        with_legs: bool = False,
    ) -> dict[str, Any]:
        if lots <= 0:
            raise ValueError("lots must be greater than zero.")

        normalized_side = order_side.strip().upper()
        if normalized_side not in {"ORDER_SIDE_SELL", "ORDER_SIDE_BUY"}:
            raise ValueError("order_side must be ORDER_SIDE_SELL or ORDER_SIDE_BUY.")

        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            legs = self._resolve_atm_straddle_legs(symbol, exchange=exchange, expiry=expiry)
            order_qty = int(legs["lot_size"]) * int(lots)
            orders = [
                {
                    "ref_id": legs["call_leg"]["ref_id"],
                    "order_qty": order_qty,
                    "order_side": normalized_side,
                    "order_delivery_type": order_delivery_type,
                },
                {
                    "ref_id": legs["put_leg"]["ref_id"],
                    "order_qty": order_qty,
                    "order_side": normalized_side,
                    "order_delivery_type": order_delivery_type,
                },
            ]
            margin = self.get_margin(
                exchange=exchange,
                orders=orders,
                with_portfolio=with_portfolio,
                with_legs=with_legs,
                is_basket=True,
                basket_params={
                    "order_side": "ORDER_SIDE_BUY",
                    "order_delivery_type": order_delivery_type,
                    "price_type": "MARKET",
                    "multiplier": 1,
                },
            )
            rows.append(
                {
                    "underlying": legs["underlying"],
                    "exchange": legs["exchange"],
                    "expiry": legs["expiry"],
                    "atm_strike": legs["atm_strike"],
                    "current_price": legs["current_price"],
                    "lots": lots,
                    "lot_size": legs["lot_size"],
                    "per_leg_order_qty": order_qty,
                    "strategy": "ATM straddle",
                    "order_side": normalized_side,
                    "order_delivery_type": order_delivery_type,
                    "call_leg": legs["call_leg"],
                    "put_leg": legs["put_leg"],
                    "total_margin": margin.get("total_margin"),
                    "margin_benefit": margin.get("margin_benefit"),
                    "max_quantity": margin.get("max_quantity"),
                    "message": margin.get("message"),
                    "code": margin.get("code"),
                    "raw_margin": margin,
                }
            )

        ranked = sorted(rows, key=lambda row: (row.get("total_margin") is None, -(row.get("total_margin") or 0)))
        return {
            "count": len(ranked),
            "strategy": "ATM straddle",
            "order_side": normalized_side,
            "order_delivery_type": order_delivery_type,
            "with_portfolio": with_portfolio,
            "with_legs": with_legs,
            "margin_basis_note": "Use total_margin as the authoritative required margin from Nubra.",
            "rows": ranked,
        }

    def analyze_option_greek_changes(
        self,
        *,
        symbols: list[str],
        greek: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        baseline: str = "open",
        compare_to: str = "latest",
        exchange: str = "NSE",
        intraday: bool = True,
    ) -> dict[str, Any]:
        greek_name = greek.strip().lower()
        if greek_name not in {"delta", "gamma", "theta", "vega", "iv_mid"}:
            raise ValueError("greek must be one of delta, gamma, theta, vega, iv_mid.")
        baseline_name = baseline.strip().lower()
        compare_name = compare_to.strip().lower()
        if baseline_name != "open":
            raise ValueError("baseline currently supports only 'open'.")
        if compare_name not in {"latest", "high", "low"}:
            raise ValueError("compare_to must be one of latest, high, low.")

        option_symbols, instrument_to_underlying = self._resolve_atm_option_symbols(symbols, exchange=exchange)
        payload = self.historical_data(
            option_symbols,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            instrument_type="OPT",
            fields=[greek_name, "close"],
            intraday=intraday,
        )

        option_rows: list[dict[str, Any]] = []
        for result in payload.get("result") or []:
            for symbol_entry in result.get("values") or []:
                if not isinstance(symbol_entry, dict):
                    continue
                symbol_name, series = next(iter(symbol_entry.items()))
                meta = instrument_to_underlying.get(symbol_name, {})
                greek_series = series.get(greek_name) or []
                numeric_values = [point.get("v") for point in greek_series if isinstance(point.get("v"), (int, float))]
                if not numeric_values:
                    option_rows.append(
                        {
                            "symbol": symbol_name,
                            "underlying": meta.get("underlying"),
                            "option_type": meta.get("option_type"),
                            "insufficient_data": True,
                        }
                    )
                    continue
                open_value = numeric_values[0]
                latest_value = numeric_values[-1]
                high_value = max(numeric_values)
                low_value = min(numeric_values)
                compare_value = latest_value if compare_name == "latest" else high_value if compare_name == "high" else low_value
                option_rows.append(
                    {
                        "symbol": symbol_name,
                        "underlying": meta.get("underlying"),
                        "option_type": meta.get("option_type"),
                        "expiry": meta.get("expiry"),
                        "atm_strike": meta.get("atm_strike"),
                        "ref_id": meta.get("ref_id"),
                        f"{greek_name}_open": open_value,
                        f"{greek_name}_latest": latest_value,
                        f"{greek_name}_high": high_value,
                        f"{greek_name}_low": low_value,
                        f"{greek_name}_change": compare_value - open_value,
                        "insufficient_data": False,
                    }
                )

        summaries: list[dict[str, Any]] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in option_rows:
            grouped.setdefault(str(row.get("underlying") or row.get("symbol")), []).append(row)
        for underlying, rows in grouped.items():
            valid_rows = [row for row in rows if not row.get("insufficient_data")]
            ranking_score = max((row.get(f"{greek_name}_change") or float("-inf")) for row in valid_rows) if valid_rows else None
            summaries.append(
                {
                    "underlying": underlying,
                    "atm_legs": rows,
                    "ranking_score": ranking_score,
                }
            )
        ranked = sorted(
            summaries,
            key=lambda row: (row.get("ranking_score") is None, -(row.get("ranking_score") or float("-inf"))),
        )
        return {
            "count": len(ranked),
            "greek": greek_name,
            "baseline": baseline_name,
            "compare_to": compare_name,
            "ranking_basis": f"ATM option {greek_name} {compare_name} minus open, using intraday historical data with intraday=True.",
            "underlyings_ranked": ranked,
            "batching_note": "Historical data requests are sent to Nubra in batches of at most 5 instruments.",
            "time_note": "Use Nubra SDK-style UTC timestamps like 2026-03-10T03:45:00.000Z.",
        }

    def find_delta_neutral_pairs(
        self,
        symbol: str,
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        chain = self.option_chain(symbol, exchange=exchange, expiry=expiry)
        calls = chain["calls"]
        puts = chain["puts"]
        pairs: list[dict[str, Any]] = []
        for call in calls:
            call_delta = float(call.get("delta") or 0.0)
            for put in puts:
                put_delta = -abs(float(put.get("delta") or 0.0))
                net_delta = call_delta + put_delta
                pairs.append(
                    {
                        "call_ref_id": call.get("ref_id"),
                        "call_strike": call.get("sp"),
                        "call_delta": call_delta,
                        "put_ref_id": put.get("ref_id"),
                        "put_strike": put.get("sp"),
                        "put_delta": put_delta,
                        "net_delta": net_delta,
                        "score": abs(net_delta),
                        "combined_premium": (call.get("ltp") or 0) + (put.get("ltp") or 0),
                        "combined_open_interest": (call.get("oi") or 0) + (put.get("oi") or 0),
                    }
                )

        ranked = sorted(pairs, key=lambda row: (row["score"], -row["combined_open_interest"]))[:top_k]
        return {
            "asset": chain["asset"],
            "exchange": chain["exchange"],
            "expiry": chain["expiry"],
            "atm": chain["atm"],
            "current_price": chain["current_price"],
            "pairs": ranked,
        }

    def _derive_limit_price_from_quote(
        self,
        *,
        ref_id: int,
        order_side: str,
        exchange: str,
    ) -> float:
        quote = self.client.get_quote(ref_id, levels=1).get("orderBook", {})
        ltp = quote.get("last_traded_price")
        if ltp is None:
            raise NubraAPIError(
                f"Unable to derive a limit price for ref_id {ref_id} on {exchange}. Quote data missing LTP."
            )
        normalized_side = order_side.strip().upper()
        ltp_value = float(ltp)

        if normalized_side == "ORDER_SIDE_BUY":
            return max(0.05, round(ltp_value - 10, 2))
        if normalized_side == "ORDER_SIDE_SELL":
            return round(ltp_value + 10, 2)
        raise NubraAPIError(
            f"Unable to derive a limit price for ref_id {ref_id} on {exchange}. Unsupported order side '{order_side}'."
        )

    def _resolve_sdk_environment(self, environment: str | None = None) -> str:
        effective = (environment or self.client.state.environment or self.client.settings.environment).strip().upper()
        if effective not in {"PROD", "UAT"}:
            raise NubraAPIError("Environment must be PROD or UAT for SDK order placement.")
        return effective

    def _get_sdk_clients(self, environment: str | None = None) -> tuple[Any, Any, Any, dict[str, Any]]:
        try:
            from nubra_python_sdk.marketdata.market_data import MarketData
            from nubra_python_sdk.refdata.instruments import InstrumentData
            from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
            from nubra_python_sdk.trading.trading_data import NubraTrader
            from nubra_python_sdk.trading.trading_enum import DeliveryTypeEnum, ExchangeEnum, OrderSideEnum, PriceTypeEnumV2
        except Exception as exc:
            raise NubraAPIError(f"nubra-python-sdk import failed: {exc}") from exc

        effective_environment = self._resolve_sdk_environment(environment)
        sdk_env = NubraEnv.PROD if effective_environment == "PROD" else NubraEnv.UAT
        nubra = InitNubraSdk(sdk_env, env_creds=True)
        instruments = InstrumentData(nubra)
        trade = NubraTrader(nubra, version="V2")
        market_data = MarketData(nubra)
        enums = {
            "OrderSideEnum": OrderSideEnum,
            "DeliveryTypeEnum": DeliveryTypeEnum,
            "PriceTypeEnumV2": PriceTypeEnumV2,
            "ExchangeEnum": ExchangeEnum,
        }
        return instruments, trade, market_data, enums

    def _parse_expiry_date(self, expiry_date: str) -> tuple[int, int, int]:
        try:
            parsed = datetime.strptime(expiry_date.strip(), "%d-%m-%y")
        except ValueError as exc:
            raise ValueError("expiry_date must be in dd-mm-yy format.") from exc
        return parsed.day, parsed.month, parsed.year

    def _is_index_underlying(self, underlying: str) -> bool:
        target = underlying.strip().upper()
        try:
            index_details = self.find_index_details(target, exchange="NSE", limit=3, instrument_limit=0)
        except Exception:
            index_details = {"count": 0}
        if int(index_details.get("count") or 0) > 0:
            return True
        try:
            chain = self.option_chain(target, exchange="NSE", expiry=None)
            calls = chain.get("calls") or []
            puts = chain.get("puts") or []
            sample = calls[0] if calls else puts[0] if puts else None
            if sample:
                ref_id = sample.get("ref_id")
                if ref_id is not None:
                    instrument = self.client.get_instrument_by_ref_id(int(ref_id), exchange="NSE")
                    return str(instrument.get("asset_type") or "").strip().upper() == "INDEX_FO"
        except Exception:
            return False
        return False

    def _default_expiry_type_for_underlying(self, underlying: str) -> str:
        return "weekly" if self._is_index_underlying(underlying) else "monthly"

    def _option_lot_size_for_underlying(
        self,
        underlying: str,
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
    ) -> int:
        legs = self._resolve_atm_straddle_legs(underlying, exchange=exchange, expiry=expiry)
        lot_size = int(legs.get("lot_size") or 0)
        if lot_size <= 0:
            raise NubraAPIError(f"Unable to determine lot size for '{underlying}'.")
        return lot_size

    def _strategy_legs_from_template(
        self,
        *,
        strategy: str,
        underlying: str,
        expiry_date: str,
        expiry_type: str | None,
        side: str,
        center_strike: int | float | None = None,
        call_strike: int | float | None = None,
        put_strike: int | float | None = None,
        lower_put_strike: int | float | None = None,
        upper_call_strike: int | float | None = None,
    ) -> list[dict[str, Any]]:
        normalized_strategy = strategy.strip().lower().replace("-", "_").replace(" ", "_")
        normalized_side = side.strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell.")

        effective_expiry_type = (expiry_type or self._default_expiry_type_for_underlying(underlying)).strip().lower()
        if effective_expiry_type not in {"weekly", "monthly"}:
            raise ValueError("expiry_type must be weekly or monthly.")

        if center_strike is None and normalized_strategy in {"straddle", "iron_butterfly"}:
            chain = self.option_chain(underlying, exchange="NSE", expiry=None)
            center_strike = int(chain.get("atm") or 0)
            if center_strike <= 0:
                raise NubraAPIError(f"Unable to infer ATM strike for '{underlying}'.")

        buy_side = "BUY"
        sell_side = "SELL"

        def _leg(strike: int | float, option_type: str, leg_side: str) -> dict[str, Any]:
            return {
                "underlying": underlying,
                "strike": float(strike),
                "option_type": option_type,
                "expiry_type": effective_expiry_type,
                "expiry_date": expiry_date,
                "side": leg_side,
            }

        if normalized_strategy == "straddle":
            if center_strike is None:
                raise ValueError("center_strike is required for straddle when ATM cannot be inferred.")
            leg_side = sell_side if normalized_side == "sell" else buy_side
            return [
                _leg(center_strike, "CE", leg_side),
                _leg(center_strike, "PE", leg_side),
            ]

        if normalized_strategy == "strangle":
            if put_strike is None or call_strike is None:
                raise ValueError("put_strike and call_strike are required for strangle.")
            leg_side = sell_side if normalized_side == "sell" else buy_side
            return [
                _leg(put_strike, "PE", leg_side),
                _leg(call_strike, "CE", leg_side),
            ]

        if normalized_strategy == "iron_condor":
            if lower_put_strike is None or put_strike is None or call_strike is None or upper_call_strike is None:
                raise ValueError("iron_condor requires lower_put_strike, put_strike, call_strike, and upper_call_strike.")
            if normalized_side == "sell":
                return [
                    _leg(lower_put_strike, "PE", buy_side),
                    _leg(put_strike, "PE", sell_side),
                    _leg(call_strike, "CE", sell_side),
                    _leg(upper_call_strike, "CE", buy_side),
                ]
            return [
                _leg(lower_put_strike, "PE", sell_side),
                _leg(put_strike, "PE", buy_side),
                _leg(call_strike, "CE", buy_side),
                _leg(upper_call_strike, "CE", sell_side),
            ]

        if normalized_strategy == "iron_butterfly":
            if center_strike is None or lower_put_strike is None or upper_call_strike is None:
                raise ValueError("iron_butterfly requires center_strike, lower_put_strike, and upper_call_strike.")
            if normalized_side == "sell":
                return [
                    _leg(lower_put_strike, "PE", buy_side),
                    _leg(center_strike, "PE", sell_side),
                    _leg(center_strike, "CE", sell_side),
                    _leg(upper_call_strike, "CE", buy_side),
                ]
            return [
                _leg(lower_put_strike, "PE", sell_side),
                _leg(center_strike, "PE", buy_side),
                _leg(center_strike, "CE", buy_side),
                _leg(upper_call_strike, "CE", sell_side),
            ]

        raise ValueError("strategy must be one of straddle, strangle, iron_condor, iron_butterfly.")

    def _build_option_symbol_from_leg(self, leg: dict[str, Any]) -> dict[str, str]:
        if leg.get("symbol"):
            side = str(leg.get("side") or "").strip().upper()
            if side not in {"BUY", "SELL"}:
                raise ValueError("Each leg side must be BUY or SELL.")
            return {
                "symbol": str(leg["symbol"]).strip().upper(),
                "side": side,
            }

        required = ("underlying", "strike", "option_type", "expiry_type", "side", "expiry_date")
        missing = [field for field in required if field not in leg or leg.get(field) in {None, ""}]
        if missing:
            raise ValueError(f"Missing required leg fields: {', '.join(missing)}")

        underlying = str(leg["underlying"]).strip().upper()
        strike_text = _format_option_strike(leg["strike"])
        option_type = str(leg["option_type"]).strip().upper()
        expiry_type = str(leg["expiry_type"]).strip().lower()
        side = str(leg["side"]).strip().upper()
        day, month, year = self._parse_expiry_date(str(leg["expiry_date"]))

        if option_type not in {"CE", "PE"}:
            raise ValueError("option_type must be CE or PE.")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL.")
        if expiry_type not in {"weekly", "monthly"}:
            raise ValueError("expiry_type must be weekly or monthly.")

        year_short = str(year)[-2:]
        if expiry_type == "weekly":
            symbol = f"{underlying}{year_short}{month}{day:02d}{strike_text}{option_type}"
        else:
            symbol = f"{underlying}{year_short}{MONTH_CODE_MONTHLY[month]}{strike_text}{option_type}"

        return {
            "symbol": symbol,
            "side": side,
        }

    def _sdk_get_ltp(self, market_data: Any, ref_id: int) -> float:
        quote = market_data.quote(ref_id=ref_id, levels=1)
        order_book = getattr(quote, "orderBook", None) or getattr(quote, "order_book", None) or quote
        ltp = getattr(order_book, "last_traded_price", None)
        if ltp is None:
            ltp = getattr(order_book, "ltp", None)
        if ltp is not None:
            return float(ltp)

        bid = getattr(order_book, "bid", None) or []
        ask = getattr(order_book, "ask", None) or []
        if bid:
            return float(getattr(bid[0], "price"))
        if ask:
            return float(getattr(ask[0], "price"))
        raise NubraAPIError(f"Unable to determine LTP for ref_id {ref_id}.")

    def _sdk_signed_entry_price(self, orders: list[dict[str, Any]], market_data: Any, sign_style: str) -> float:
        normalized_style = sign_style.strip().lower()
        if normalized_style not in {"buy_positive", "sell_positive"}:
            raise ValueError("sign_style must be 'buy_positive' or 'sell_positive'.")

        total = 0.0
        for order in orders:
            ltp = self._sdk_get_ltp(market_data, int(order["ref_id"]))
            is_buy = str(order["order_side"]).strip().upper().endswith("BUY")
            sign = 1 if (normalized_style == "buy_positive" and is_buy) or (normalized_style == "sell_positive" and not is_buy) else -1
            total += sign * ltp
        return total

    def _sdk_to_plain(self, value: Any) -> Any:
        if hasattr(value, "model_dump"):
            return self._sdk_to_plain(value.model_dump())
        if isinstance(value, dict):
            return {key: self._sdk_to_plain(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sdk_to_plain(item) for item in value]
        if hasattr(value, "__dict__") and not isinstance(value, (str, bytes, int, float, bool)):
            return self._sdk_to_plain(vars(value))
        return value

    def _sdk_place_option_strategy(
        self,
        *,
        legs: list[dict[str, Any]],
        basket_name: str,
        tag: str | None,
        exchange: str,
        multiplier: int,
        sign_style: str,
        lots: int,
        default_order_qty: int | None,
        order_delivery_type: str,
        environment: str | None,
    ) -> dict[str, Any]:
        self._raise_read_only_trading()
        if not legs:
            raise ValueError("legs must contain at least one options leg.")
        if multiplier <= 0:
            raise ValueError("multiplier must be greater than zero.")
        if lots <= 0:
            raise ValueError("lots must be greater than zero.")

        instruments, trade, market_data, enums = self._get_sdk_clients(environment)
        order_side_enum = enums["OrderSideEnum"]
        delivery_type_enum = enums["DeliveryTypeEnum"]
        price_type_enum = enums["PriceTypeEnumV2"]
        exchange_enum = enums["ExchangeEnum"]
        side_map = {
            "BUY": order_side_enum.ORDER_SIDE_BUY,
            "SELL": order_side_enum.ORDER_SIDE_SELL,
        }

        try:
            delivery_type_value = getattr(delivery_type_enum, order_delivery_type.strip().upper())
        except AttributeError as exc:
            raise ValueError(f"Unsupported order_delivery_type '{order_delivery_type}'.") from exc
        try:
            exchange_value = getattr(exchange_enum, exchange.strip().upper())
        except AttributeError:
            exchange_value = exchange.strip().upper()

        built_symbols: list[dict[str, str]] = [self._build_option_symbol_from_leg(leg) for leg in legs]
        orders: list[dict[str, Any]] = []
        resolved_legs: list[dict[str, Any]] = []
        shared_lot_size: int | None = None

        for leg_input, built in zip(legs, built_symbols, strict=False):
            instrument = instruments.get_instrument_by_symbol(built["symbol"], exchange=exchange)
            if instrument is None:
                raise NubraAPIError(f"Instrument not found for symbol '{built['symbol']}' on {exchange}.")

            ref_id = getattr(instrument, "ref_id", None)
            if ref_id is None and isinstance(instrument, dict):
                ref_id = instrument.get("ref_id")
            if ref_id is None:
                raise NubraAPIError(f"Resolved instrument for '{built['symbol']}' is missing ref_id.")

            lot_size = getattr(instrument, "lot_size", None)
            if lot_size is None and isinstance(instrument, dict):
                lot_size = instrument.get("lot_size")
            lot_size = int(lot_size or 0)
            if lot_size <= 0:
                raise NubraAPIError(f"Resolved instrument for '{built['symbol']}' is missing lot_size.")

            if shared_lot_size is None:
                shared_lot_size = lot_size
            elif shared_lot_size != lot_size:
                raise NubraAPIError("All legs in an options strategy must have the same lot_size.")

            order_qty = leg_input.get("order_qty")
            if order_qty is None:
                order_qty = default_order_qty
            if order_qty is None:
                order_qty = lot_size * int(lots)

            orders.append(
                {
                    "ref_id": int(ref_id),
                    "order_qty": int(order_qty),
                    "order_side": side_map[built["side"]],
                }
            )
            resolved_legs.append(
                {
                    "input": leg_input,
                    "built_symbol": built["symbol"],
                    "side": built["side"],
                    "lot_size": lot_size,
                    "instrument": self._sdk_to_plain(instrument),
                }
            )

        entry_price = self._sdk_signed_entry_price(orders, market_data, sign_style=sign_style)
        basket_payload = {
            "exchange": exchange_value,
            "basket_name": basket_name,
            "tag": tag,
            "orders": orders,
            "basket_params": {
                "order_side": order_side_enum.ORDER_SIDE_BUY,
                "order_delivery_type": delivery_type_value,
                "price_type": price_type_enum.LIMIT,
                "entry_price": entry_price,
                "multiplier": multiplier,
            },
        }
        basket = trade.flexi_order(basket_payload)

        return {
            "placement_engine": "sdk_flexi_order",
            "environment": self._resolve_sdk_environment(environment),
            "exchange": exchange,
            "basket_name": basket_name,
            "tag": tag,
            "sign_style": sign_style,
            "lots": lots,
            "lot_size": shared_lot_size,
            "entry_price": entry_price,
            "multiplier": multiplier,
            "resolved_legs": resolved_legs,
            "submitted_orders": orders,
            "basket_payload": basket_payload,
            "basket_response": self._sdk_to_plain(basket),
            "prompting_note": "If the user wants NIFTY options and expiry is unclear, ask whether it is weekly or monthly and ask for expiry date in dd-mm-yy.",
        }

    def place_options_strategy(
        self,
        *,
        legs: list[dict[str, Any]],
        basket_name: str,
        tag: str | None = None,
        exchange: str = "NSE",
        multiplier: int = 1,
        sign_style: str = "buy_positive",
        lots: int = 1,
        default_order_qty: int | None = None,
        order_delivery_type: str = "ORDER_DELIVERY_TYPE_CNC",
        environment: str | None = None,
    ) -> dict[str, Any]:
        return self._sdk_place_option_strategy(
            legs=legs,
            basket_name=basket_name,
            tag=tag,
            exchange=exchange,
            multiplier=multiplier,
            sign_style=sign_style,
            lots=lots,
            default_order_qty=default_order_qty,
            order_delivery_type=order_delivery_type,
            environment=environment,
        )

    def place_named_option_strategy(
        self,
        *,
        strategy: str,
        underlying: str,
        expiry_date: str,
        exchange: str = "NSE",
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
        environment: str | None = None,
    ) -> dict[str, Any]:
        normalized_side = side.strip().lower()
        effective_expiry_type = (expiry_type or self._default_expiry_type_for_underlying(underlying)).strip().lower()
        effective_sign_style = sign_style or ("sell_positive" if normalized_side == "sell" else "buy_positive")
        strategy_legs = self._strategy_legs_from_template(
            strategy=strategy,
            underlying=underlying,
            expiry_date=expiry_date,
            expiry_type=effective_expiry_type,
            side=normalized_side,
            center_strike=center_strike,
            call_strike=call_strike,
            put_strike=put_strike,
            lower_put_strike=lower_put_strike,
            upper_call_strike=upper_call_strike,
        )
        result = self._sdk_place_option_strategy(
            legs=strategy_legs,
            basket_name=basket_name or f"{underlying.strip().upper()}_{strategy.strip().lower()}",
            tag=tag,
            exchange=exchange,
            multiplier=multiplier,
            sign_style=effective_sign_style,
            lots=lots,
            default_order_qty=order_qty,
            order_delivery_type=order_delivery_type,
            environment=environment,
        )
        result.update(
            {
                "strategy": strategy,
                "underlying": underlying.strip().upper(),
                "side": normalized_side,
                "expiry_type": effective_expiry_type,
                "expiry_date": expiry_date,
                "lots": lots,
                "effective_order_qty": result.get("submitted_orders", [{}])[0].get("order_qty") if result.get("submitted_orders") else None,
                "strategy_prompting_note": "Stock options default to monthly expiry. Index options default to weekly expiry unless explicitly overridden.",
            }
        )
        return result

    def place_order(self, order_input: dict[str, Any]) -> dict[str, Any]:
        self._raise_read_only_trading()
        order = OrderRequest(**order_input)
        resolved_symbol: str | None = order.symbol
        if order.ref_id is None:
            if not order.symbol:
                raise NubraAPIError("Either ref_id or symbol is required to place an order")
            instrument = self.client.resolve_symbol(order.symbol, exchange=order.exchange)
            order.ref_id = instrument.ref_id
            resolved_symbol = instrument.symbol

        original_price_type = order.price_type
        converted_market_to_limit = False
        if order.price_type.strip().upper() == "MARKET":
            derived_limit_price = self._derive_limit_price_from_quote(
                ref_id=order.ref_id,
                order_side=order.order_side,
                exchange=order.exchange,
            )
            order.price_type = "LIMIT"
            order.order_price = derived_limit_price
            converted_market_to_limit = True

        response = self.client.place_order(order)
        return {
            "order": response,
            "effective_price_type": order.price_type,
            "effective_order_price": order.order_price,
            "converted_market_to_limit": converted_market_to_limit,
            "requested_price_type": original_price_type,
            "resolved_symbol": resolved_symbol,
        }

    def square_off_position(
        self,
        *,
        symbol: str | None = None,
        ref_id: int | None = None,
        exchange: str = "NSE",
        quantity: int | None = None,
    ) -> dict[str, Any]:
        self._raise_read_only_trading()
        positions_payload = self.client.get_positions()
        portfolio = positions_payload.get("portfolio", {})
        candidate_lists = [
            portfolio.get("stock_positions") or [],
            portfolio.get("fut_positions") or [],
            portfolio.get("opt_positions") or [],
        ]
        all_positions: list[dict[str, Any]] = [item for group in candidate_lists for item in group]

        match: dict[str, Any] | None = None
        for position in all_positions:
            pos_ref_id = position.get("ref_id")
            pos_symbol = str(position.get("symbol") or "").strip().upper()
            if ref_id is not None and pos_ref_id == ref_id:
                match = position
                break
            if symbol and pos_symbol == symbol.strip().upper():
                match = position
                break

        if not match:
            target = f"ref_id={ref_id}" if ref_id is not None else f"symbol={symbol}"
            raise NubraAPIError(f"No open position found for {target}")

        net_qty = match.get("quantity", match.get("qty"))
        if net_qty is None:
            raise NubraAPIError("Matched position is missing quantity")
        exit_qty = quantity or abs(int(net_qty))
        if exit_qty <= 0:
            raise NubraAPIError("Square off quantity must be greater than zero")

        current_side = str(match.get("order_side") or "").upper()
        if current_side in {"BUY", "ORDER_SIDE_BUY"}:
            exit_side = "ORDER_SIDE_SELL"
        elif current_side in {"SELL", "ORDER_SIDE_SELL"}:
            exit_side = "ORDER_SIDE_BUY"
        else:
            raise NubraAPIError(f"Unable to infer square-off side from position side '{current_side}'")

        instrument_ref_id = int(match["ref_id"])
        delivery_type = str(match.get("product") or "ORDER_DELIVERY_TYPE_IDAY")
        derived_limit_price = self._derive_limit_price_from_quote(
            ref_id=instrument_ref_id,
            order_side=exit_side,
            exchange=str(match.get("exchange") or exchange),
        )
        order = OrderRequest(
            ref_id=instrument_ref_id,
            symbol=str(match.get("symbol") or symbol or ""),
            exchange=str(match.get("exchange") or exchange),
            order_type="ORDER_TYPE_REGULAR",
            order_qty=exit_qty,
            order_side=exit_side,
            order_delivery_type=delivery_type,
            validity_type="DAY",
            price_type="LIMIT",
            order_price=derived_limit_price,
            tag="square_off_via_mcp",
        )
        response = self.client.place_order(order)
        return {
            "position": match,
            "square_off_order": response,
            "effective_price_type": "LIMIT",
            "effective_order_price": derived_limit_price,
            "converted_market_to_limit": True,
        }

    def cancel_order(self, order_id: int) -> dict[str, Any]:
        self._raise_read_only_trading()

    def get_positions(self) -> dict[str, Any]:
        positions = self.client.get_positions()
        portfolio = positions.get("portfolio", {})
        return {
            "message": positions.get("message", "positions"),
            "client_code": portfolio.get("client_code"),
            "position_stats": portfolio.get("position_stats", {}),
            "stock_positions": portfolio.get("stock_positions") or [],
            "fut_positions": portfolio.get("fut_positions") or [],
            "opt_positions": portfolio.get("opt_positions") or [],
            "close_positions": portfolio.get("close_positions") or [],
        }

    def get_holdings(self) -> dict[str, Any]:
        payload = self.client.get_holdings()
        portfolio = payload.get("portfolio", {})
        return {
            "message": payload.get("message", "holdings"),
            "client_code": portfolio.get("client_code"),
            "holding_stats": portfolio.get("holding_stats", {}),
            "holdings": portfolio.get("holdings") or [],
        }

    def get_funds(self) -> dict[str, Any]:
        payload = self.client.get_funds()
        return {
            "message": payload.get("message", "funds"),
            "funds": payload.get("funds", payload),
        }

    def strategy_pnl_summary(self) -> dict[str, Any]:
        orders = self.client.get_orders(executed=True)
        positions_payload = self.client.get_positions()
        portfolio = positions_payload.get("portfolio", {})
        position_groups = [
            portfolio.get("stock_positions") or [],
            portfolio.get("fut_positions") or [],
            portfolio.get("opt_positions") or [],
            portfolio.get("close_positions") or [],
        ]
        all_positions: list[dict[str, Any]] = [item for group in position_groups for item in group]

        positions_by_ref: dict[int, list[dict[str, Any]]] = {}
        for position in all_positions:
            ref_id = position.get("ref_id")
            if ref_id is None:
                continue
            positions_by_ref.setdefault(int(ref_id), []).append(position)

        tags_by_ref: dict[int, set[str]] = {}
        for order in orders:
            ref_id = order.get("ref_id")
            if ref_id is None:
                continue
            tag_value = str(order.get("tag") or "").strip() or "__UNTAGGED__"
            tags_by_ref.setdefault(int(ref_id), set()).add(tag_value)

        grouped: dict[str, dict[str, Any]] = {}
        for order in orders:
            tag_value = str(order.get("tag") or "").strip() or "__UNTAGGED__"
            entry = grouped.setdefault(
                tag_value,
                {
                    "tag": None if tag_value == "__UNTAGGED__" else tag_value,
                    "orders": [],
                    "position_pnl": 0.0,
                    "matched_positions": [],
                    "unmatched_ref_ids": [],
                    "ambiguous_ref_ids": [],
                },
            )
            entry["orders"].append(order)

        for tag_value, entry in grouped.items():
            seen_refs: set[int] = set()
            for order in entry["orders"]:
                ref_id = order.get("ref_id")
                if ref_id is None:
                    continue
                ref_id = int(ref_id)
                if ref_id in seen_refs:
                    continue
                seen_refs.add(ref_id)
                position_matches = positions_by_ref.get(ref_id) or []
                owner_tags = tags_by_ref.get(ref_id) or set()
                if len(owner_tags) > 1:
                    entry["ambiguous_ref_ids"].append(ref_id)
                    continue
                if not position_matches:
                    entry["unmatched_ref_ids"].append(ref_id)
                    continue
                for position in position_matches:
                    entry["matched_positions"].append(position)
                    pnl_value = position.get("pnl")
                    if isinstance(pnl_value, (int, float)):
                        entry["position_pnl"] += float(pnl_value)

        strategies = [grouped[tag] for tag in grouped if tag != "__UNTAGGED__"]
        untagged = grouped.get("__UNTAGGED__", {"orders": []})
        untagged_individual = []
        for order in untagged.get("orders", []):
            ref_id = order.get("ref_id")
            matches = positions_by_ref.get(int(ref_id), []) if ref_id is not None else []
            order_pnl = matches[0].get("pnl") if len(matches) == 1 else None
            untagged_individual.append(
                {
                    "order_id": order.get("order_id"),
                    "tag": order.get("tag"),
                    "ref_id": ref_id,
                    "symbol": order.get("display_name") or (order.get("ref_data") or {}).get("stock_name"),
                    "pnl": order_pnl,
                }
            )

        return {
            "strategy_count": len(strategies),
            "strategies": strategies,
            "untagged_orders": untagged_individual,
            "note": "Strategy P&L is inferred by grouping executed orders by tag and matching positions by ref_id. Shared ref_ids across multiple tags are marked ambiguous.",
        }

    def _load_talib_helpers(self) -> tuple[Any, Any]:
        try:
            from nubra_talib import add_talib, to_ohlcv_df
            return to_ohlcv_df, add_talib
        except Exception:
            local_src = Path(__file__).resolve().parents[1] / "nubra_talib" / "src"
            if local_src.exists():
                src_str = str(local_src)
                if src_str not in sys.path:
                    sys.path.insert(0, src_str)
                from nubra_talib import add_talib, to_ohlcv_df
                return to_ohlcv_df, add_talib
            raise

    def _historical_to_df(
        self,
        symbol: str,
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> Any:
        to_ohlcv_df, _ = self._load_talib_helpers()
        payload = self.historical_data(
            symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            instrument_type=instrument_type,
            fields=["open", "high", "low", "close", "cumulative_volume"],
        )
        df = to_ohlcv_df(payload, symbol=symbol.strip().upper(), interval=timeframe)
        if df is None or getattr(df, "empty", True):
            raise NubraAPIError(
                f"No historical OHLCV data returned for '{symbol}'.",
                details={"symbol": symbol, "timeframe": timeframe},
            )
        return df

    def compare_symbols_performance(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            df = self._historical_to_df(
                symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                instrument_type=instrument_type,
            )
            closes = df["close"].dropna()
            first_close = float(closes.iloc[0])
            last_close = float(closes.iloc[-1])
            rows.append(
                {
                    "symbol": symbol.strip().upper(),
                    "start_close": round(first_close, 2),
                    "end_close": round(last_close, 2),
                    "return_pct": _pct_change(last_close, first_close),
                    "bars": int(len(df)),
                    "timeframe": timeframe,
                }
            )
        ranked = sorted(rows, key=lambda row: row.get("return_pct") or float("-inf"), reverse=True)
        return {"timeframe": timeframe, "start_date": start_date, "end_date": end_date, "rows": ranked}

    def rank_symbols_by_return(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        payload = self.compare_symbols_performance(
            symbols,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            instrument_type=instrument_type,
        )
        return {
            "ranking_metric": "return_pct",
            "timeframe": payload["timeframe"],
            "start_date": payload["start_date"],
            "end_date": payload["end_date"],
            "ranked_symbols": payload["rows"],
        }

    def find_volume_spikes(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
        lookback_bars: int = 20,
        min_spike_ratio: float = 1.5,
    ) -> dict[str, Any]:
        matches: list[dict[str, Any]] = []
        for symbol in symbols:
            df = self._historical_to_df(
                symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                instrument_type=instrument_type,
            )
            if len(df) <= lookback_bars:
                continue
            latest_volume = float(df["volume"].fillna(0).iloc[-1])
            baseline = float(df["volume"].fillna(0).iloc[-(lookback_bars + 1):-1].mean())
            ratio = None if baseline == 0 else round(latest_volume / baseline, 4)
            if ratio is not None and ratio >= min_spike_ratio:
                matches.append(
                    {
                        "symbol": symbol.strip().upper(),
                        "latest_volume": latest_volume,
                        "average_volume": round(baseline, 2),
                        "spike_ratio": ratio,
                        "timestamp": str(df["timestamp"].iloc[-1]),
                    }
                )
        matches.sort(key=lambda row: row["spike_ratio"], reverse=True)
        return {
            "timeframe": timeframe,
            "lookback_bars": lookback_bars,
            "min_spike_ratio": min_spike_ratio,
            "matches": matches,
        }

    def summarize_option_chain(
        self,
        symbol: str,
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        chain = self.option_chain(symbol, exchange=exchange, expiry=expiry)
        calls = chain.get("calls") or []
        puts = chain.get("puts") or []

        def _top_oi(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
            ranked = sorted(legs, key=lambda leg: float(leg.get("oi") or 0), reverse=True)[:top_k]
            return [
                {
                    "strike_price": leg.get("sp"),
                    "open_interest": leg.get("oi"),
                    "volume": leg.get("volume"),
                    "iv": leg.get("iv"),
                    "last_traded_price": leg.get("ltp"),
                }
                for leg in ranked
            ]

        return {
            "asset": chain.get("asset"),
            "exchange": chain.get("exchange"),
            "expiry": chain.get("expiry"),
            "atm": chain.get("atm"),
            "current_price": chain.get("current_price"),
            "top_call_oi": _top_oi(calls),
            "top_put_oi": _top_oi(puts),
        }

    def find_oi_walls(
        self,
        symbols: list[str],
        *,
        exchange: str = "NSE",
        expiry: str | None = None,
        top_k: int = 3,
        max_distance_pct: float = 2.5,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            chain = self.option_chain(symbol, exchange=exchange, expiry=expiry)
            current_price = chain.get("current_price")
            if current_price in (None, 0):
                continue
            calls = sorted(chain.get("calls") or [], key=lambda leg: float(leg.get("oi") or 0), reverse=True)[:top_k]
            puts = sorted(chain.get("puts") or [], key=lambda leg: float(leg.get("oi") or 0), reverse=True)[:top_k]
            strike_candidates: list[dict[str, Any]] = []
            for side, legs in (("call_resistance", calls), ("put_support", puts)):
                for leg in legs:
                    strike = leg.get("sp")
                    if strike in (None, 0):
                        continue
                    distance_pct = round(abs((float(strike) - float(current_price)) / float(current_price)) * 100.0, 4)
                    if distance_pct <= max_distance_pct:
                        strike_candidates.append(
                            {
                                "wall_type": side,
                                "strike_price": strike,
                                "open_interest": leg.get("oi"),
                                "distance_pct": distance_pct,
                                "last_traded_price": leg.get("ltp"),
                                "iv": leg.get("iv"),
                            }
                        )
            strike_candidates.sort(key=lambda row: (row["distance_pct"], -(row.get("open_interest") or 0)))
            rows.append(
                {
                    "symbol": symbol.strip().upper(),
                    "expiry": chain.get("expiry"),
                    "current_price": current_price,
                    "nearby_walls": strike_candidates[:top_k],
                }
            )
        ranked = sorted(
            rows,
            key=lambda row: row["nearby_walls"][0]["distance_pct"] if row.get("nearby_walls") else float("inf"),
        )
        return {
            "max_distance_pct": max_distance_pct,
            "top_k": top_k,
            "rows": ranked,
        }

    def summarize_symbol_indicators(
        self,
        symbol: str,
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        indicators: dict[str, dict[str, Any]] | None = None,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        _, add_talib = self._load_talib_helpers()
        df = self._historical_to_df(
            symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            instrument_type=instrument_type,
        )
        indicator_map = indicators or {"RSI": {"timeperiod": 14}, "EMA": {"timeperiod": 21}, "SMA": {"timeperiod": 50}}
        enriched = add_talib(df, funcs=indicator_map)
        latest = enriched.iloc[-1].to_dict()
        latest["timestamp"] = str(latest.get("timestamp"))
        return {
            "symbol": symbol.strip().upper(),
            "timeframe": timeframe,
            "start_date": start_date,
            "end_date": end_date,
            "indicators": indicator_map,
            "latest": latest,
        }

    def scan_indicator_threshold(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        indicator: str,
        params: dict[str, Any] | None = None,
        operator: str = ">=",
        value: float = 0.0,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        _, add_talib = self._load_talib_helpers()
        matches: list[dict[str, Any]] = []
        ops = {
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
        }
        normalized_operator = operator.strip()
        if normalized_operator not in ops:
            raise ValueError("operator must be one of >, >=, <, <=, ==")

        for symbol in symbols:
            df = self._historical_to_df(
                symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                instrument_type=instrument_type,
            )
            enriched = add_talib(df, funcs={indicator.upper(): params or {}})
            column_candidates = [col for col in enriched.columns if col.lower().startswith(indicator.lower())]
            if not column_candidates:
                continue
            col = column_candidates[0]
            latest_value = enriched[col].iloc[-1]
            try:
                latest_numeric = float(latest_value)
            except Exception:
                continue
            if ops[normalized_operator](latest_numeric, float(value)):
                matches.append(
                    {
                        "symbol": symbol.strip().upper(),
                        "indicator": indicator.upper(),
                        "column": col,
                        "latest_value": round(latest_numeric, 4),
                        "timestamp": str(enriched["timestamp"].iloc[-1]),
                    }
                )
        return {
            "indicator": indicator.upper(),
            "operator": normalized_operator,
            "value": value,
            "matches": matches,
        }

    def scan_indicator_crossover(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        start_date: str,
        end_date: str,
        fast_indicator: str,
        fast_params: dict[str, Any] | None = None,
        slow_indicator: str,
        slow_params: dict[str, Any] | None = None,
        direction: str = "bullish",
        lookback_bars: int = 5,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
    ) -> dict[str, Any]:
        _, add_talib = self._load_talib_helpers()
        normalized_direction = direction.strip().lower()
        if normalized_direction not in {"bullish", "bearish", "any"}:
            raise ValueError("direction must be bullish, bearish, or any.")

        matches: list[dict[str, Any]] = []
        for symbol in symbols:
            df = self._historical_to_df(
                symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                instrument_type=instrument_type,
            )
            enriched = add_talib(
                df,
                funcs={
                    fast_indicator.upper(): fast_params or {},
                    slow_indicator.upper(): slow_params or {},
                },
            )
            fast_cols = [col for col in enriched.columns if col.lower().startswith(fast_indicator.lower())]
            slow_cols = [col for col in enriched.columns if col.lower().startswith(slow_indicator.lower())]
            if not fast_cols or not slow_cols:
                continue
            fast_col = fast_cols[0]
            slow_col = slow_cols[0]
            subset = enriched[["timestamp", "close", fast_col, slow_col]].dropna().reset_index(drop=True)
            if len(subset) < 2:
                continue
            recent = subset.tail(max(lookback_bars + 1, 2)).reset_index(drop=True)
            signal_row: dict[str, Any] | None = None
            for index in range(1, len(recent)):
                prev_fast = float(recent.iloc[index - 1][fast_col])
                prev_slow = float(recent.iloc[index - 1][slow_col])
                curr_fast = float(recent.iloc[index][fast_col])
                curr_slow = float(recent.iloc[index][slow_col])
                crossed_up = prev_fast <= prev_slow and curr_fast > curr_slow
                crossed_down = prev_fast >= prev_slow and curr_fast < curr_slow
                if crossed_up and normalized_direction in {"bullish", "any"}:
                    signal_row = {
                        "symbol": symbol.strip().upper(),
                        "direction": "bullish",
                        "timestamp": str(recent.iloc[index]["timestamp"]),
                        "close": round(float(recent.iloc[index]["close"]), 2),
                        "fast_value": round(curr_fast, 4),
                        "slow_value": round(curr_slow, 4),
                    }
                if crossed_down and normalized_direction in {"bearish", "any"}:
                    signal_row = {
                        "symbol": symbol.strip().upper(),
                        "direction": "bearish",
                        "timestamp": str(recent.iloc[index]["timestamp"]),
                        "close": round(float(recent.iloc[index]["close"]), 2),
                        "fast_value": round(curr_fast, 4),
                        "slow_value": round(curr_slow, 4),
                    }
            if signal_row:
                signal_row.update(
                    {
                        "fast_indicator": fast_indicator.upper(),
                        "fast_params": fast_params or {},
                        "slow_indicator": slow_indicator.upper(),
                        "slow_params": slow_params or {},
                    }
                )
                matches.append(signal_row)

        return {
            "timeframe": timeframe,
            "lookback_bars": lookback_bars,
            "direction": normalized_direction,
            "matches": matches,
        }

    def find_symbols_with_rising_greeks(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        start_date: str,
        end_date: str,
        exchange: str = "NSE",
        instrument_type: str = "STOCK",
        intraday: bool = True,
    ) -> dict[str, Any]:
        option_symbols, instrument_to_underlying = self._resolve_atm_option_symbols(symbols, exchange=exchange)

        payload = self.historical_data(
            option_symbols,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
            instrument_type="OPT",
            fields=["delta", "vega", "theta", "gamma", "iv_mid", "close"],
            intraday=intraday,
        )
        option_rows: list[dict[str, Any]] = []
        for result in payload.get("result") or []:
            for symbol_entry in result.get("values") or []:
                if not isinstance(symbol_entry, dict):
                    continue
                symbol_name, series = next(iter(symbol_entry.items()))
                meta = instrument_to_underlying.get(symbol_name, {})
                delta_series = series.get("delta") or []
                vega_series = series.get("vega") or []
                if len(delta_series) < 2 or len(vega_series) < 2:
                    option_rows.append(
                        {
                            "symbol": symbol_name,
                            "underlying": meta.get("underlying"),
                            "option_type": meta.get("option_type"),
                            "delta_rising": False,
                            "vega_rising": False,
                            "insufficient_data": True,
                        }
                    )
                    continue
                delta_first = delta_series[0].get("v")
                delta_last = delta_series[-1].get("v")
                vega_first = vega_series[0].get("v")
                vega_last = vega_series[-1].get("v")
                delta_valid = isinstance(delta_first, (int, float)) and isinstance(delta_last, (int, float))
                vega_valid = isinstance(vega_first, (int, float)) and isinstance(vega_last, (int, float))
                delta_rising = delta_valid and delta_last > delta_first
                vega_rising = vega_valid and vega_last > vega_first
                option_rows.append(
                    {
                        "symbol": symbol_name,
                        "underlying": meta.get("underlying"),
                        "option_type": meta.get("option_type"),
                        "expiry": meta.get("expiry"),
                        "atm_strike": meta.get("atm_strike"),
                        "ref_id": meta.get("ref_id"),
                        "delta_first": delta_first,
                        "delta_last": delta_last,
                        "delta_change": (delta_last - delta_first) if delta_valid else None,
                        "vega_first": vega_first,
                        "vega_last": vega_last,
                        "vega_change": (vega_last - vega_first) if vega_valid else None,
                        "delta_rising": bool(delta_rising),
                        "vega_rising": bool(vega_rising),
                        "both_rising": bool(delta_rising and vega_rising),
                        "insufficient_data": False,
                    }
                )
        underlying_summary: dict[str, dict[str, Any]] = {}
        for row in option_rows:
            underlying = str(row.get("underlying") or row.get("symbol"))
            entry = underlying_summary.setdefault(
                underlying,
                {
                    "underlying": underlying,
                    "atm_legs": [],
                    "any_leg_both_rising": False,
                    "all_available_legs_both_rising": True,
                },
            )
            entry["atm_legs"].append(row)
            both_rising = bool(row.get("both_rising"))
            entry["any_leg_both_rising"] = entry["any_leg_both_rising"] or both_rising
            if not both_rising:
                entry["all_available_legs_both_rising"] = False

        summaries = list(underlying_summary.values())
        both_rising = [row for row in summaries if row.get("any_leg_both_rising")]
        return {
            "count": len(summaries),
            "underlyings_with_any_atm_leg_rising": both_rising,
            "underlyings": summaries,
            "rate_limit_note": "Historical data for the same symbol/query is cached for 10 seconds inside the MCP server.",
            "batching_note": "Historical data requests are sent to Nubra in batches of at most 5 instruments.",
            "intraday_note": "Historical Greek queries use the supplied UTC window. Use Nubra SDK-style timestamps like 2026-03-10T03:45:00.000Z.",
            "time_note": "Human-readable time fields are returned in IST alongside raw UTC/nanosecond timestamps.",
        }
