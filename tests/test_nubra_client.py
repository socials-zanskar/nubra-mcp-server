from __future__ import annotations

import json
import os
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from config import Settings
from nubra_client import (
    AuthState,
    HistoricalQuery,
    ModifyOrderRequest,
    NubraAPIError,
    NubraClient,
    NubraService,
    OrderRequest,
    normalize_nubra_payload,
)


class DummyHistoricalClient:
    def __init__(self) -> None:
        self.last_query: HistoricalQuery | None = None

    def get_historical_data(self, query: HistoricalQuery) -> dict:
        self.last_query = query
        return {"query": query.model_dump()}


class DummyPortfolioClient:
    def get_holdings(self) -> dict:
        return {
            "message": "holdings",
            "portfolio": {
                "client_code": "I0001",
                "holding_stats": {"invested_amount": 1000},
                "holdings": [
                    {
                        "symbol": "RELIANCE",
                        "displayName": "RELIANCE",
                        "qty": 2,
                        "ltp": 1500.0,
                        "invested_value": 2800.0,
                        "current_value": 3000.0,
                        "net_pnl": 200.0,
                        "is_pledgeable": True,
                        "available_to_pledge": 2,
                        "margin_benefit": 1000.0,
                        "haircut": 15.0,
                    }
                ],
            },
        }

    def get_positions(self) -> dict:
        return {
            "message": "positions",
            "portfolio": {
                "client_code": "I0001",
                "position_stats": {"total_pnl": -50.0},
                "stock_positions": [
                    {
                        "symbol": "TCS",
                        "display_name": "TCS",
                        "qty": 1,
                        "ltp": 4000.0,
                        "avg_price": 4050.0,
                        "pnl": -50.0,
                        "order_side": "BUY",
                        "product": "ORDER_DELIVERY_TYPE_IDAY",
                        "asset_type": "STOCKS",
                        "derivative_type": "STOCK",
                        "ref_id": 123,
                        "exchange": "NSE",
                    }
                ],
                "fut_positions": [],
                "opt_positions": [],
                "close_positions": [],
            },
        }

    def get_funds(self) -> dict:
        return {
            "message": "funds",
            "funds": {
                "port_funds_and_margin": {
                    "start_of_day_funds": 100000.0,
                    "net_margin_available": -5000.0,
                    "total_margin_blocked": 12000.0,
                    "mtm_deriv": -100.0,
                    "mtm_eq_iday_cnc": 50.0,
                    "mtm_eq_delivery": -10.0,
                }
            },
        }

    def get_instruments(self, exchange: str) -> list[dict]:
        return [
            {
                "stock_name": "RELIANCE",
                "asset": "RELIANCE",
                "derivative_type": "STOCK",
                "option_type": "",
                "expiry": None,
                "strike_price": None,
                "ref_id": 71878,
            },
            {
                "stock_name": "NIFTY2632722500CE",
                "asset": "NIFTY",
                "derivative_type": "OPT",
                "option_type": "CE",
                "expiry": "2026-03-27",
                "strike_price": 22500,
                "ref_id": 2001,
            },
        ]


class NubraClientTests(unittest.TestCase):
    def test_normalize_nubra_payload_converts_portfolio_money_fields_to_rupees(self) -> None:
        payload = normalize_nubra_payload(
            {
                "funds": {
                    "port_funds_and_margin": {
                        "start_of_day_funds": 10000000,
                        "net_margin_available": -500000,
                        "total_margin_blocked": 1200000,
                        "mtm_deriv": -10000,
                        "mtm_eq_iday_cnc": 5000,
                        "mtm_eq_delivery": -1000,
                    }
                },
                "portfolio": {
                    "holding_stats": {"invested_amount": 280000},
                    "holdings": [
                        {
                            "symbol": "RELIANCE",
                            "invested_value": 280000,
                            "current_value": 300000,
                            "net_pnl": 20000,
                            "margin_benefit": 100000,
                        }
                    ],
                },
            }
        )

        funds = payload["funds"]["port_funds_and_margin"]
        holding = payload["portfolio"]["holdings"][0]
        self.assertEqual(funds["start_of_day_funds"], 100000.0)
        self.assertEqual(funds["start_of_day_funds_display"], "Rs. 100,000.00")
        self.assertEqual(funds["net_margin_available"], -5000.0)
        self.assertEqual(holding["invested_value"], 2800.0)
        self.assertEqual(holding["current_value"], 3000.0)
        self.assertEqual(holding["net_pnl"], 200.0)
        self.assertEqual(holding["margin_benefit_display"], "Rs. 1,000.00")

    def test_auth_state_path_resolves_relative_to_repo(self) -> None:
        settings = Settings(auth_state_file="tmp/auth_state.json")
        self.assertTrue(settings.auth_state_path.is_absolute())
        self.assertTrue(str(settings.auth_state_path).endswith("tmp\\auth_state.json"))

    def test_settings_from_env_accepts_phone_no_alias(self) -> None:
        previous_phone = os.environ.get("PHONE")
        previous_phone_no = os.environ.get("PHONE_NO")
        try:
            os.environ.pop("PHONE", None)
            os.environ["PHONE_NO"] = "9999999999"
            settings = Settings.from_env()
            self.assertEqual(settings.phone, "9999999999")
        finally:
            if previous_phone is None:
                os.environ.pop("PHONE", None)
            else:
                os.environ["PHONE"] = previous_phone
            if previous_phone_no is None:
                os.environ.pop("PHONE_NO", None)
            else:
                os.environ["PHONE_NO"] = previous_phone_no

    def test_settings_default_collector_root_points_to_sibling_repo(self) -> None:
        settings = Settings()
        self.assertTrue(str(settings.collector_root_path).endswith("Nubra_API_Full_context\\nubracollector"))

    def test_load_state_resets_environment_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "environment": "PROD",
                        "phone": "9999999999",
                        "device_id": "device-1",
                        "session_token": "secret",
                        "authenticated": True,
                    }
                ),
                encoding="utf-8",
            )
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(state_path)))
            self.assertEqual(client.state.environment, "UAT")
            self.assertEqual(client.state.phone, "9999999999")
            self.assertEqual(client.state.device_id, "device-1")
            self.assertFalse(client.state.authenticated)
            self.assertIsNone(client.state.session_token)

    def test_auth_status_probes_once_with_recent_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state = AuthState(
                environment="UAT",
                phone="9999999999",
                device_id="device-1",
                session_token="session",
                authenticated=True,
            )
            state_path.write_text(json.dumps(asdict(state)), encoding="utf-8")
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(state_path)))

            calls = {"count": 0}

            def fake_request(*args, **kwargs):
                calls["count"] += 1
                return {"message": "ok"}

            client._request = fake_request  # type: ignore[method-assign]

            first = client.auth_status()
            second = client.auth_status()

            self.assertTrue(first["session_active"])
            self.assertTrue(second["session_active"])
            self.assertEqual(calls["count"], 1)

    def test_open_nubracollector_ui_reuses_active_session(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state = AuthState(
                environment="PROD",
                phone="9999999999",
                device_id="device-1",
                auth_token="auth-token",
                session_token="session-token",
                authenticated=True,
            )
            state_path.write_text(json.dumps(asdict(state)), encoding="utf-8")
            client = NubraClient(Settings(environment="PROD", auth_state_file=str(state_path)))

            seeded: dict[str, str] = {}

            class DummyAdapter:
                AUTO_CONNECT_ENV_VAR = "NUBRA_COLLECTOR_AUTO_CONNECT"
                AUTO_CONNECT_ENV_NAME = "NUBRA_COLLECTOR_AUTO_ENV"
                AUTO_CONNECT_PHONE_ENV_NAME = "NUBRA_COLLECTOR_AUTO_PHONE"

                @staticmethod
                def seed_shared_session(*, auth_token: str, session_token: str, device_id: str) -> str:
                    seeded["auth_token"] = auth_token
                    seeded["session_token"] = session_token
                    seeded["device_id"] = device_id
                    return "C:\\temp\\collector.db"

            popen_kwargs: dict[str, object] = {}

            class DummyPopen:
                def __init__(self, args: list[str], **kwargs: object) -> None:
                    popen_kwargs["args"] = args
                    popen_kwargs.update(kwargs)
                    self.pid = 4321

            with (
                patch.object(client, "_has_reusable_local_session", return_value=True),
                patch.object(client, "_load_collector_session_adapter", return_value=DummyAdapter),
                patch.object(client, "_resolve_collector_python", return_value="pythonw.exe"),
                patch("nubra_client.subprocess.Popen", DummyPopen),
            ):
                payload = client.open_nubracollector_ui()

            self.assertTrue(payload["session_reused"])
            self.assertEqual(payload["pid"], 4321)
            self.assertEqual(seeded["auth_token"], "auth-token")
            self.assertEqual(seeded["session_token"], "session-token")
            self.assertEqual(seeded["device_id"], "device-1")
            self.assertEqual(popen_kwargs["args"], ["pythonw.exe", "-m", "nubracollector"])

    def test_open_nubracollector_ui_falls_back_to_login_when_session_is_inactive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state_path.write_text(json.dumps(asdict(AuthState(environment="UAT", phone="9999999999"))), encoding="utf-8")
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(state_path)))

            class DummyAdapter:
                AUTO_CONNECT_ENV_VAR = "NUBRA_COLLECTOR_AUTO_CONNECT"
                AUTO_CONNECT_ENV_NAME = "NUBRA_COLLECTOR_AUTO_ENV"
                AUTO_CONNECT_PHONE_ENV_NAME = "NUBRA_COLLECTOR_AUTO_PHONE"

                @staticmethod
                def seed_shared_session(**_: object) -> str:
                    raise AssertionError("seed_shared_session should not be called without an active session")

            class DummyPopen:
                def __init__(self, *_: object, **__: object) -> None:
                    self.pid = 9876

            with (
                patch.object(client, "_has_reusable_local_session", return_value=False),
                patch.object(client, "_load_collector_session_adapter", return_value=DummyAdapter),
                patch.object(client, "_resolve_collector_python", return_value="python.exe"),
                patch("nubra_client.subprocess.Popen", DummyPopen),
            ):
                payload = client.open_nubracollector_ui()

            self.assertFalse(payload["session_reused"])
            self.assertTrue(payload["requires_login"])

    def test_open_backtest_ui_launches_in_prod_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state_path.write_text(json.dumps(asdict(AuthState(environment="UAT", phone="9999999999"))), encoding="utf-8")
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(state_path)))

            popen_kwargs: dict[str, object] = {}

            class DummyPopen:
                def __init__(self, args: list[str], **kwargs: object) -> None:
                    popen_kwargs["args"] = args
                    popen_kwargs.update(kwargs)
                    self.pid = 2468

            with patch("nubra_client.subprocess.Popen", DummyPopen):
                payload = client.open_backtest_ui()

            self.assertEqual(payload["environment"], "PROD")
            self.assertEqual(payload["pid"], 2468)
            self.assertIn("PROD mode", payload["message"])
            self.assertEqual((popen_kwargs["env"] or {}).get("NUBRA_BACKTEST_UI_ENV"), "PROD")

    def test_detached_popen_kwargs_use_start_new_session_on_posix(self) -> None:
        client = NubraClient(Settings())
        with patch("nubra_client.sys.platform", "darwin"):
            payload = client._build_detached_popen_kwargs()
        self.assertEqual(payload, {"start_new_session": True})

    def test_set_environment_clears_existing_session_and_reports_saved_mpin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "auth_state.json"
            state = AuthState(
                environment="PROD",
                phone="9999999999",
                device_id="device-1",
                auth_token="auth",
                session_token="session",
                authenticated=True,
            )
            state_path.write_text(json.dumps(asdict(state)), encoding="utf-8")
            client = NubraClient(Settings(environment="PROD", auth_state_file=str(state_path), mpin="4321"))

            client.logout = lambda: {"message": "Logged out.", "environment": "PROD"}  # type: ignore[method-assign]

            payload = client.set_environment("UAT")

            self.assertEqual(payload["environment"], "UAT")
            self.assertEqual(payload["previous_environment"], "PROD")
            self.assertTrue(payload["session_reset"])
            self.assertTrue(payload["saved_mpin_available"])
            self.assertEqual(payload["next_step"], "send_otp")

    def test_verify_otp_with_saved_mpin_reuses_configured_mpin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json"), mpin="9876"))

            calls: dict[str, str] = {}

            def fake_verify_otp(otp: str, phone: str | None = None) -> dict[str, object]:
                calls["otp"] = otp
                return {"environment": "UAT", "phone": phone or "9999999999"}

            def fake_verify_mpin(mpin: str) -> dict[str, object]:
                calls["mpin"] = mpin
                return {"environment": "UAT", "phone": "9999999999", "authenticated": True}

            client.verify_otp = fake_verify_otp  # type: ignore[method-assign]
            client.verify_mpin = fake_verify_mpin  # type: ignore[method-assign]

            payload = client.verify_otp_with_saved_mpin("123456", phone="9999999999")

            self.assertEqual(calls["otp"], "123456")
            self.assertEqual(calls["mpin"], "9876")
            self.assertTrue(payload["authenticated"])
            self.assertTrue(payload["used_saved_mpin"])

    def test_connect_nubra_mcp_stores_session_mpin_and_sends_otp(self) -> None:
        client = NubraClient(Settings(environment="PROD", auth_state_file="auth_state_test.json"))
        client.switch_environment_and_send_otp = lambda environment, phone=None: {  # type: ignore[method-assign]
            "environment": environment,
            "phone": phone or "9999999999",
            "device_id": "device-1",
        }

        payload = client.connect_nubra_mcp(phone="9999999999", mpin="2468", environment="PROD")

        self.assertEqual(payload["intro_message"], "Connected successfully with nubra mcp.")
        self.assertEqual(payload["next_step"], "complete_connect_with_otp")
        self.assertEqual(client._session_mpin, "2468")

    def test_complete_connect_with_otp_uses_session_mpin(self) -> None:
        client = NubraClient(Settings(environment="PROD", auth_state_file="auth_state_test.json"))
        client._session_mpin = "2468"

        calls: dict[str, str] = {}

        def fake_verify_otp(otp: str, phone: str | None = None) -> dict[str, object]:
            calls["otp"] = otp
            return {"environment": "PROD", "phone": phone or "9999999999"}

        def fake_verify_mpin(mpin: str) -> dict[str, object]:
            calls["mpin"] = mpin
            return {"environment": "PROD", "phone": "9999999999", "authenticated": True}

        client.verify_otp = fake_verify_otp  # type: ignore[method-assign]
        client.verify_mpin = fake_verify_mpin  # type: ignore[method-assign]

        payload = client.complete_connect_with_otp("123456", phone="9999999999")

        self.assertEqual(calls["otp"], "123456")
        self.assertEqual(calls["mpin"], "2468")
        self.assertTrue(payload["authenticated"])
        self.assertIsNone(client._session_mpin)

    def test_client_place_order_uses_current_session_rest_path_in_uat(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            client.state.session_token = "session-token"
            client.state.authenticated = True
            captured: dict[str, Any] = {}

            def fake_request(method, path, *, params=None, json_body=None, headers=None):
                captured["method"] = method
                captured["path"] = path
                captured["json_body"] = json_body
                captured["headers"] = headers
                return {"order_id": 777, "exchange": json_body["exchange"], "ref_id": json_body["ref_id"]}

            client._request = fake_request  # type: ignore[method-assign]

            payload = client.place_order(
                OrderRequest(
                    ref_id=71335,
                    exchange="NSE",
                    order_type="ORDER_TYPE_REGULAR",
                    order_qty=1,
                    order_side="ORDER_SIDE_BUY",
                    order_delivery_type="ORDER_DELIVERY_TYPE_CNC",
                    validity_type="DAY",
                    price_type="LIMIT",
                    order_price=1220,
                )
            )

            self.assertEqual(captured["method"], "POST")
            self.assertEqual(captured["path"], "orders/v2/single")
            self.assertEqual(captured["json_body"]["ref_id"], 71335)
            self.assertEqual(captured["headers"]["Authorization"], "Bearer session-token")
            self.assertEqual(payload["order_id"], 777)
            self.assertEqual(payload["ref_id"], 71335)

    def test_client_modify_order_includes_price_type_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            client.state.session_token = "session-token"
            client.state.authenticated = True
            captured: dict[str, Any] = {}

            def fake_request(method, path, *, params=None, json_body=None, headers=None):
                captured["method"] = method
                captured["path"] = path
                captured["json_body"] = json_body
                captured["headers"] = headers
                return {"message": "update request pushed"}

            client._request = fake_request  # type: ignore[method-assign]

            payload = client.modify_order(
                ModifyOrderRequest(
                    order_id=12345,
                    exchange="NSE",
                    order_type="ORDER_TYPE_REGULAR",
                    order_qty=10,
                    price_type="LIMIT",
                    order_price=1030,
                )
            )

            self.assertEqual(captured["method"], "POST")
            self.assertEqual(captured["path"], "orders/v2/modify/12345")
            self.assertEqual(captured["json_body"]["order_qty"], 10)
            self.assertEqual(captured["json_body"]["order_price"], 1030)
            self.assertEqual(captured["json_body"]["price_type"], "LIMIT")
            self.assertEqual(captured["headers"]["Authorization"], "Bearer session-token")
            self.assertEqual(payload["message"], "update request pushed")

    def test_prime_sdk_environment_sets_phone_no_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(
                Settings(
                    environment="UAT",
                    auth_state_file=str(Path(temp_dir) / "auth.json"),
                    phone="9999999999",
                    mpin="2468",
                )
            )
            previous_phone = os.environ.get("PHONE")
            previous_phone_no = os.environ.get("PHONE_NO")
            previous_mpin = os.environ.get("MPIN")
            try:
                os.environ.pop("PHONE", None)
                os.environ.pop("PHONE_NO", None)
                os.environ.pop("MPIN", None)
                client._prime_sdk_environment()
                self.assertEqual(os.environ["PHONE"], "9999999999")
                self.assertEqual(os.environ["PHONE_NO"], "9999999999")
                self.assertEqual(os.environ["MPIN"], "2468")
            finally:
                if previous_phone is None:
                    os.environ.pop("PHONE", None)
                else:
                    os.environ["PHONE"] = previous_phone
                if previous_phone_no is None:
                    os.environ.pop("PHONE_NO", None)
                else:
                    os.environ["PHONE_NO"] = previous_phone_no
                if previous_mpin is None:
                    os.environ.pop("MPIN", None)
                else:
                    os.environ["MPIN"] = previous_mpin


class HistoricalWindowTests(unittest.TestCase):
    def test_daily_history_defaults_to_recent_month(self) -> None:
        service = NubraService(DummyHistoricalClient())
        payload = service.historical_data(
            "RELIANCE",
            timeframe="1d",
            start_date="",
            end_date="",
            instrument_type="STOCK",
        )

        query = payload["query"]
        start_dt = datetime.fromisoformat(query["startDate"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(query["endDate"].replace("Z", "+00:00"))
        self.assertEqual(query["type"], "STOCK")
        self.assertGreaterEqual((end_dt - start_dt).days, 29)
        self.assertLessEqual((end_dt - start_dt).days, 31)

    def test_intraday_history_defaults_to_recent_two_days(self) -> None:
        service = NubraService(DummyHistoricalClient())
        payload = service.historical_data(
            "RELIANCE",
            timeframe="5m",
            start_date="",
            end_date="",
            instrument_type="STOCK",
        )

        query = payload["query"]
        start_dt = datetime.fromisoformat(query["startDate"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(query["endDate"].replace("Z", "+00:00"))
        self.assertGreaterEqual((end_dt - start_dt).days, 1)
        self.assertLessEqual((end_dt - start_dt).days, 2)
        self.assertFalse(query["intraDay"])

    def test_historical_data_respects_explicit_intraday_override(self) -> None:
        service = NubraService(DummyHistoricalClient())
        payload = service.historical_data(
            "RELIANCE",
            timeframe="1h",
            start_date="2026-02-01T00:00:00.000Z",
            end_date="2026-05-05T23:59:59.000Z",
            instrument_type="STOCK",
            intraday=True,
        )

        query = payload["query"]
        self.assertTrue(query["intraDay"])

    def test_volume_breakout_finds_confirmed_match(self) -> None:
        service = NubraService(DummyHistoricalClient())
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=25, freq="D"),
                "open": [100.0] * 25,
                "high": [101.0] * 24 + [110.0],
                "low": [99.0] * 25,
                "close": [100.5] * 24 + [109.0],
                "volume": [1000.0] * 24 + [3000.0],
            }
        )
        service._historical_to_df = lambda *args, **kwargs: df  # type: ignore[method-assign]

        payload = service.find_volume_breakouts(
            ["RELIANCE"],
            timeframe="1d",
            start_date="2026-01-01",
            end_date="2026-01-25",
            breakout_lookback_bars=20,
            volume_lookback_bars=20,
            min_volume_spike_ratio=1.5,
            min_breakout_pct=0.0,
            require_close_breakout=True,
        )

        self.assertEqual(payload["strategy"], "volume_breakout")
        self.assertEqual(payload["summary"]["match_count"], 1)
        self.assertEqual(payload["matches"][0]["symbol"], "RELIANCE")
        self.assertEqual(payload["matches"][0]["latest_close_display"], "Rs. 109.00")

    def test_volume_breakout_deduplicates_symbols_before_fetching(self) -> None:
        service = NubraService(DummyHistoricalClient())
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=25, freq="D"),
                "open": [100.0] * 25,
                "high": [101.0] * 24 + [110.0],
                "low": [99.0] * 25,
                "close": [100.5] * 24 + [109.0],
                "volume": [1000.0] * 24 + [3000.0],
            }
        )
        calls: list[str] = []

        def fake_history(symbol: str, **kwargs) -> pd.DataFrame:
            calls.append(symbol)
            return df

        service._historical_to_df = fake_history  # type: ignore[method-assign]

        payload = service.find_volume_breakouts(
            ["RELIANCE", "reliance", "CIPLA", "CIPLA"],
            timeframe="1d",
            start_date="2026-01-01",
            end_date="2026-01-25",
        )

        self.assertEqual(sorted(calls), ["CIPLA", "RELIANCE"])
        self.assertEqual(payload["summary"]["match_count"], 2)

    def test_historical_to_df_normalizes_intraday_volume_by_session(self) -> None:
        service = NubraService(DummyPortfolioClient())

        def point(ts_text: str, value: int) -> dict[str, int]:
            ts = int(pd.Timestamp(ts_text, tz="Asia/Kolkata").tz_convert("UTC").value)
            return {"ts": ts, "v": value}

        payload = {
            "result": [
                {
                    "values": [
                        {
                            "RELIANCE": {
                                "open": [
                                    point("2026-05-05 09:15", 10000),
                                    point("2026-05-05 09:16", 10100),
                                    point("2026-05-06 09:15", 10200),
                                    point("2026-05-06 09:16", 10300),
                                ],
                                "high": [
                                    point("2026-05-05 09:15", 10100),
                                    point("2026-05-05 09:16", 10200),
                                    point("2026-05-06 09:15", 10300),
                                    point("2026-05-06 09:16", 10400),
                                ],
                                "low": [
                                    point("2026-05-05 09:15", 9900),
                                    point("2026-05-05 09:16", 10000),
                                    point("2026-05-06 09:15", 10100),
                                    point("2026-05-06 09:16", 10200),
                                ],
                                "close": [
                                    point("2026-05-05 09:15", 10050),
                                    point("2026-05-05 09:16", 10150),
                                    point("2026-05-06 09:15", 10250),
                                    point("2026-05-06 09:16", 10350),
                                ],
                                "cumulative_volume": [
                                    point("2026-05-05 09:15", 100),
                                    point("2026-05-05 09:16", 300),
                                    point("2026-05-06 09:15", 50),
                                    point("2026-05-06 09:16", 120),
                                ],
                            }
                        }
                    ]
                }
            ]
        }

        df = service._historical_payload_to_df(payload, symbol="RELIANCE", timeframe="1m")

        self.assertEqual(df["volume"].tolist(), [100.0, 200.0, 50.0, 70.0])

    def test_historical_to_df_fetches_extra_warmup_history(self) -> None:
        service = NubraService(DummyPortfolioClient())
        captured: dict[str, str] = {}

        def point(ts_text: str, value: int) -> dict[str, int]:
            ts = int(pd.Timestamp(ts_text, tz="Asia/Kolkata").tz_convert("UTC").value)
            return {"ts": ts, "v": value}

        payload = {
            "result": [
                {
                    "values": [
                        {
                            "RELIANCE": {
                                "open": [point("2026-04-01 09:15", 10000)],
                                "high": [point("2026-04-01 09:15", 10100)],
                                "low": [point("2026-04-01 09:15", 9900)],
                                "close": [point("2026-04-01 09:15", 10050)],
                                "cumulative_volume": [point("2026-04-01 09:15", 100)],
                            }
                        }
                    ]
                }
            ]
        }

        def fake_historical_data(symbol, *, timeframe, start_date, end_date, exchange, instrument_type, fields):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return payload

        service.historical_data = fake_historical_data  # type: ignore[method-assign]

        service._historical_to_df(
            "RELIANCE",
            timeframe="1d",
            start_date="2026-05-01",
            end_date="2026-05-06",
            instrument_type="STOCK",
            warmup_bars=20,
        )

        self.assertLess(pd.Timestamp(captured["start_date"]), pd.Timestamp("2026-05-01T00:00:00Z"))


class PortfolioAndResolverTests(unittest.TestCase):
    def test_portfolio_summary_contains_exposures_and_flags(self) -> None:
        service = NubraService(DummyPortfolioClient())
        payload = service.get_portfolio_summary()

        self.assertEqual(payload["account_overview"]["holdings_count"], 1)
        self.assertEqual(payload["account_overview"]["open_positions_count"], 1)
        self.assertTrue(payload["largest_exposures"])
        self.assertIn("Net margin available is negative.", payload["risk_flags"])

    def test_top_exposures_groups_by_symbol(self) -> None:
        service = NubraService(DummyPortfolioClient())
        payload = service.get_top_exposures(limit=5, group_by="symbol")

        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["exposures"][0]["group"], "TCS")

    def test_smart_resolver_prefers_exact_match(self) -> None:
        service = NubraService(DummyPortfolioClient())
        payload = service.resolve_instrument_smart("RELIANCE")

        self.assertEqual(payload["best_match"]["stock_name"], "RELIANCE")
        self.assertGreater(payload["confidence"], 0.9)

    def test_generate_portfolio_report_contains_headline_metrics(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service.get_position_risk_report = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "net_directional_bias": "net_long",
            "largest_risk_positions": [{"symbol": "TCS", "stress_test_estimate": 40.0}],
            "risk_flags": ["Sample risk flag"],
        }
        payload = service.generate_portfolio_report()

        self.assertIn("headline_metrics", payload)
        self.assertIn("portfolio_summary", payload)
        self.assertTrue(payload["narrative"])

    def test_export_portfolio_report_html_writes_file(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service.get_position_risk_report = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "net_directional_bias": "net_long",
            "largest_risk_positions": [{"symbol": "TCS", "derivative_type": "STOCK", "pnl": -50.0, "market_value": 4000.0, "stress_test_estimate": 40.0}],
            "risk_flags": ["Sample risk flag"],
        }
        payload = service.export_portfolio_report_html()

        self.assertTrue(Path(payload["report_path"]).exists())
        self.assertTrue(payload["download_ready"])
        self.assertEqual(payload["file_kind"], "html")
        html_text = Path(payload["report_path"]).read_text(encoding="utf-8")
        self.assertIn("Portfolio Report", html_text)
        self.assertIn("Largest Exposures", html_text)

    def test_export_portfolio_report_image_writes_file(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service.get_position_risk_report = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "net_directional_bias": "net_long",
            "largest_risk_positions": [{"symbol": "TCS", "derivative_type": "STOCK", "pnl": -50.0, "market_value": 4000.0, "stress_test_estimate": 40.0}],
            "risk_flags": ["Sample risk flag"],
        }
        payload = service.export_portfolio_report_image()

        self.assertTrue(Path(payload["image_path"]).exists())
        self.assertTrue(payload["download_ready"])
        self.assertEqual(payload["file_kind"], "image")
        self.assertEqual(Path(payload["image_path"]).suffix.lower(), ".png")

    def test_export_historical_data_csv_writes_file(self) -> None:
        service = NubraService(DummyPortfolioClient())

        class FakeFrame:
            columns = ["timestamp", "open", "high", "low", "close", "volume"]

            def __len__(self):
                return 2

            def copy(self):
                return self

            def __contains__(self, key):
                return key == "timestamp"

            def __getitem__(self, key):
                if key == "timestamp":
                    return self
                raise KeyError(key)

            def __setitem__(self, key, value):
                return None

            def astype(self, _dtype):
                return self

            def to_csv(self, path, index=False):
                Path(path).write_text("timestamp,open,high,low,close,volume\n2026-03-01,1,2,0.5,1.5,100\n", encoding="utf-8")

            def head(self, _count):
                return self

            def to_dict(self, orient="records"):
                return [{"timestamp": "2026-03-01", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100}]

        service._historical_to_df = lambda *args, **kwargs: FakeFrame()  # type: ignore[method-assign]
        payload = service.export_historical_data_csv("RELIANCE", timeframe="1d")

        self.assertTrue(Path(payload["csv_path"]).exists())
        self.assertEqual(payload["row_count"], 2)
        self.assertIn("close", payload["columns"])
        self.assertEqual(payload["preview_mode"], "inline_table")
        self.assertEqual(payload["file_kind"], "csv")
        self.assertEqual(payload["file_name"], Path(payload["csv_path"]).name)
        self.assertTrue(payload["download_ready"])

    def test_current_price_by_symbol_includes_nubra_source_and_rupee_display(self) -> None:
        class DummyPriceClient:
            def resolve_symbol(self, symbol: str, exchange: str = "NSE"):
                class Instrument:
                    def model_dump(self_inner):
                        return {"symbol": symbol, "exchange": exchange, "ref_id": 123}

                return Instrument()

            def get_current_price(self, symbol: str, exchange: str = "NSE") -> dict:
                return {"exchange": exchange, "price": 1220.0, "prev_close": 1200.0, "change": 1.6667}

        service = NubraService(DummyPriceClient())
        payload = service.current_price_by_symbol("CIPLA")

        self.assertEqual(payload["data_source"], "nubra")
        self.assertEqual(payload["current_price_display"], "Rs. 1,220.00")
        self.assertEqual(payload["previous_close_display"], "Rs. 1,200.00")

    def test_auth_status_includes_intro_and_prod_market_data_guidance(self) -> None:
        class DummyAuthClient:
            def auth_status(self):
                return {
                    "intro_message": "Connected to nubra-mcp. Authentication, market data, analytics, and environment switching are available.",
                    "market_data_environment_recommendation": "Use PROD for market data to access the most complete data coverage. Use UAT for order placement testing.",
                    "environment": "UAT",
                }

        service = NubraService(DummyAuthClient())
        payload = service.auth_status()

        self.assertIn("Connected to nubra-mcp", payload["intro_message"])
        self.assertIn("Use PROD for market data", payload["market_data_environment_recommendation"])

    def test_get_portfolio_sector_exposure_uses_asset_type_fallback(self) -> None:
        service = NubraService(DummyPortfolioClient())
        payload = service.get_portfolio_sector_exposure()

        self.assertTrue(payload["fallback_used"])
        self.assertTrue(payload["rows"])
        self.assertIn("asset_type_fallback", payload["rows"][0]["classification_sources"])

    def test_get_portfolio_concentration_risk_reports_hhi(self) -> None:
        service = NubraService(DummyPortfolioClient())
        payload = service.get_portfolio_concentration_risk(top_n=3)

        self.assertIn("hhi", payload)
        self.assertIn("top_exposures", payload)
        self.assertGreaterEqual(payload["largest_single_name_pct"], 0.0)

    def test_get_portfolio_rolling_drawdown_builds_preview(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service._portfolio_instruments = lambda **kwargs: [  # type: ignore[method-assign]
            {"symbol": "RELIANCE", "instrument_type": "STOCK", "signed_quantity": 2.0, "market_value": 3000.0},
            {"symbol": "TCS", "instrument_type": "STOCK", "signed_quantity": 1.0, "market_value": 4000.0},
        ]

        def make_df(closes):
            return pd.DataFrame(
                {
                    "timestamp": pd.date_range("2026-01-01", periods=len(closes), freq="D"),
                    "close": closes,
                }
            )

        service._fetch_histories_for_symbols = lambda symbols, **kwargs: (  # type: ignore[method-assign]
            {"RELIANCE": make_df([100, 110, 90, 95]), "TCS": make_df([200, 210, 220, 205])},
            [],
        )

        payload = service.get_portfolio_rolling_drawdown(timeframe="1d")

        self.assertIn("max_drawdown_pct", payload)
        self.assertTrue(payload["preview_rows"])
        self.assertEqual(payload["component_count"], 2)

    def test_get_portfolio_correlation_matrix_returns_pairs(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service._portfolio_instruments = lambda **kwargs: [  # type: ignore[method-assign]
            {"symbol": "RELIANCE", "instrument_type": "STOCK", "signed_quantity": 2.0, "market_value": 3000.0},
            {"symbol": "TCS", "instrument_type": "STOCK", "signed_quantity": 1.0, "market_value": 4000.0},
        ]

        def make_df(closes):
            return pd.DataFrame(
                {
                    "timestamp": pd.date_range("2026-01-01", periods=len(closes), freq="D"),
                    "close": closes,
                }
            )

        service._fetch_histories_for_symbols = lambda symbols, **kwargs: (  # type: ignore[method-assign]
            {"RELIANCE": make_df([100, 102, 101, 103]), "TCS": make_df([200, 204, 202, 206])},
            [],
        )

        payload = service.get_portfolio_correlation_matrix(timeframe="1d")

        self.assertEqual(payload["component_count"], 2)
        self.assertTrue(payload["matrix"])
        self.assertTrue(payload["top_pairs"])


class UatOrderGuardTests(unittest.TestCase):
    def test_place_order_rejects_prod(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="PROD", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)
            with self.assertRaises(NubraAPIError) as ctx:
                service.place_order(
                    {
                        "ref_id": 71878,
                        "exchange": "NSE",
                        "order_type": "ORDER_TYPE_REGULAR",
                        "order_qty": 1,
                        "order_side": "ORDER_SIDE_BUY",
                        "order_delivery_type": "ORDER_DELIVERY_TYPE_IDAY",
                        "validity_type": "DAY",
                        "price_type": "LIMIT",
                        "order_price": 1400,
                    }
                )
            self.assertIn("UAT", str(ctx.exception))

    def test_place_order_allows_uat_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)

            def fake_place_order(order):
                return {"order_id": 12345, "ref_id": order.ref_id, "exchange": order.exchange}

            client.place_order = fake_place_order  # type: ignore[method-assign]

            payload = service.place_order(
                {
                    "ref_id": 71878,
                    "exchange": "NSE",
                    "order_type": "ORDER_TYPE_REGULAR",
                    "order_qty": 1,
                    "order_side": "ORDER_SIDE_BUY",
                    "order_delivery_type": "ORDER_DELIVERY_TYPE_IDAY",
                    "validity_type": "DAY",
                    "price_type": "LIMIT",
                    "order_price": 1400,
                }
            )
            self.assertEqual(payload["environment"], "UAT")
            self.assertTrue(payload["guardrail_passed"])
            self.assertEqual(payload["order_mode"], "uat_only")

    def test_preview_order_returns_table_rows_and_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)
            service.current_price_by_symbol = lambda *args, **kwargs: {"current_price": 1413.3}  # type: ignore[method-assign]

            payload = service.preview_order(
                {
                    "ref_id": 71878,
                    "symbol": "RELIANCE",
                    "exchange": "NSE",
                    "order_type": "ORDER_TYPE_REGULAR",
                    "order_qty": 1,
                    "order_side": "ORDER_SIDE_BUY",
                    "order_delivery_type": "ORDER_DELIVERY_TYPE_CNC",
                    "validity_type": "DAY",
                    "price_type": "LIMIT",
                    "order_price": 1500,
                }
            )

            self.assertEqual(payload["environment"], "UAT")
            self.assertEqual(payload["preview_columns"], ["field", "value"])
            self.assertEqual(payload["preview_table"]["title"], "UAT Order Preview")
            self.assertTrue(any(row["field"] == "Symbol" and row["value"] == "RELIANCE" for row in payload["preview_rows"]))
            self.assertTrue(payload["warnings"])

    def test_cancel_order_rejects_prod(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="PROD", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)
            with self.assertRaises(NubraAPIError):
                service.cancel_order(12345)

    def test_modify_order_rejects_prod(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="PROD", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)
            with self.assertRaises(NubraAPIError):
                service.modify_order(
                    {
                        "order_id": 12345,
                        "exchange": "NSE",
                        "order_type": "ORDER_TYPE_REGULAR",
                        "order_qty": 1,
                        "order_price": 1400,
                    }
                )

    def test_cancel_order_allows_uat_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)

            def fake_cancel_order(order_id):
                return {"status": "cancelled", "order_id": order_id}

            client.cancel_order = fake_cancel_order  # type: ignore[method-assign]
            payload = service.cancel_order(12345)
            self.assertEqual(payload["environment"], "UAT")
            self.assertEqual(payload["cancel_result"]["order_id"], 12345)

    def test_modify_order_allows_uat_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)

            def fake_modify_order(request):
                return {"status": "modified", "order_id": request.order_id}

            client.modify_order = fake_modify_order  # type: ignore[method-assign]
            payload = service.modify_order(
                {
                    "order_id": 12345,
                    "exchange": "NSE",
                    "order_type": "ORDER_TYPE_REGULAR",
                    "order_qty": 1,
                    "order_price": 1400,
                }
            )
            self.assertEqual(payload["environment"], "UAT")
            self.assertEqual(payload["modify_result"]["order_id"], 12345)

    def test_modify_order_backfills_missing_fields_from_live_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            client = NubraClient(Settings(environment="UAT", auth_state_file=str(Path(temp_dir) / "auth.json")))
            service = NubraService(client)

            client.get_orders = lambda **kwargs: [  # type: ignore[method-assign]
                {
                    "order_id": 12345,
                    "exchange": "NSE",
                    "order_type": "ORDER_TYPE_REGULAR",
                    "order_qty": 10,
                    "price_type": "LIMIT",
                    "order_price": 1000,
                }
            ]

            def fake_modify_order(request):
                self.assertEqual(request.exchange, "NSE")
                self.assertEqual(request.order_type, "ORDER_TYPE_REGULAR")
                self.assertEqual(request.order_qty, 10)
                self.assertEqual(request.price_type, "LIMIT")
                self.assertEqual(request.order_price, 1030)
                return {"status": "modified", "order_id": request.order_id}

            client.modify_order = fake_modify_order  # type: ignore[method-assign]

            payload = service.modify_order({"order_id": 12345, "order_price": 1030})

            self.assertEqual(payload["environment"], "UAT")
            self.assertEqual(payload["modify_result"]["order_id"], 12345)


class ScreenerExtensionsTests(unittest.TestCase):
    def test_find_gap_up_candidates_matches(self) -> None:
        service = NubraService(DummyPortfolioClient())
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=3, freq="D"),
                "open": [100.0, 101.0, 110.0],
                "high": [102.0, 103.0, 112.0],
                "low": [99.0, 100.0, 108.0],
                "close": [101.0, 102.0, 111.0],
                "volume": [1000.0, 1200.0, 1500.0],
            }
        )
        service._fetch_histories_for_symbols = lambda symbols, **kwargs: ({"RELIANCE": df}, [])  # type: ignore[method-assign]

        payload = service.find_gap_up_candidates(["RELIANCE"], min_gap_pct=5.0)

        self.assertEqual(payload["summary"]["match_count"], 1)
        self.assertGreater(payload["matches"][0]["gap_pct"], 5.0)

    def test_find_unusual_volume_matches(self) -> None:
        service = NubraService(DummyPortfolioClient())
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=6, freq="D"),
                "open": [100.0] * 6,
                "high": [101.0] * 6,
                "low": [99.0] * 6,
                "close": [100.5] * 6,
                "volume": [100.0, 102.0, 98.0, 101.0, 99.0, 250.0],
            }
        )
        service._fetch_histories_for_symbols = lambda symbols, **kwargs: ({"RELIANCE": df}, [])  # type: ignore[method-assign]

        payload = service.find_unusual_volume(["RELIANCE"], timeframe="1d", start_date="", end_date="", lookback_bars=5, min_spike_ratio=2.0)

        self.assertEqual(payload["summary"]["match_count"], 1)
        self.assertGreaterEqual(payload["matches"][0]["spike_ratio"], 2.0)

    def test_find_oi_build_up_uses_atm_chain_snapshot(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service.option_chain = lambda symbol, **kwargs: {  # type: ignore[method-assign]
            "expiry": "20260512",
            "atm": 24350.0,
            "current_price": 24320.0,
            "calls": [{"sp": 24350.0, "ref_id": 1, "oi": 1500, "prev_oi": 1000, "iv": 0.2, "ltp": 120.0}],
            "puts": [{"sp": 24350.0, "ref_id": 2, "oi": 1800, "prev_oi": 1000, "iv": 0.22, "ltp": 115.0}],
        }

        payload = service.find_oi_build_up(["NIFTY"], min_combined_oi_change_pct=20.0)

        self.assertEqual(payload["summary"]["match_count"], 1)
        self.assertEqual(payload["matches"][0]["build_up_bias"], "two_sided_build_up")

    def test_find_iv_expansion_aggregates_legs(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service._resolve_atm_option_symbols = lambda symbols, **kwargs: (  # type: ignore[method-assign]
            ["NIFTY_CE", "NIFTY_PE"],
            {
                "NIFTY_CE": {"underlying": "NIFTY", "option_type": "CE", "expiry": "20260512", "atm_strike": 24350.0},
                "NIFTY_PE": {"underlying": "NIFTY", "option_type": "PE", "expiry": "20260512", "atm_strike": 24350.0},
            },
        )

        def point(ts, value):
            return {"ts": ts, "v": value}

        base_ts = [int(pd.Timestamp(f"2026-01-0{i}T00:00:00Z").value) for i in range(1, 5)]
        service.historical_data = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "result": [
                {
                    "values": [
                        {"NIFTY_CE": {"iv_mid": [point(base_ts[0], 0.10), point(base_ts[1], 0.11), point(base_ts[2], 0.12), point(base_ts[3], 0.20)]}},
                        {"NIFTY_PE": {"iv_mid": [point(base_ts[0], 0.12), point(base_ts[1], 0.13), point(base_ts[2], 0.14), point(base_ts[3], 0.22)]}},
                    ]
                }
            ]
        }

        payload = service.find_iv_expansion(["NIFTY"], lookback_bars=3, min_expansion_ratio=1.4)

        self.assertEqual(payload["summary"]["match_count"], 1)
        self.assertGreaterEqual(payload["matches"][0]["expansion_ratio"], 1.4)


class BacktestTests(unittest.TestCase):
    def _sample_backtest_df(self):
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=80, freq="D"),
                "open": [100 + i for i in range(80)],
                "high": [101 + i for i in range(80)],
                "low": [99 + i for i in range(80)],
                "close": [100 + i * 0.8 for i in range(80)],
                "volume": [1000 + i * 10 for i in range(80)],
            }
        )

    def test_run_ma_crossover_backtest_returns_metrics(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service._historical_to_df = lambda *args, **kwargs: self._sample_backtest_df()  # type: ignore[method-assign]
        payload = service.run_ma_crossover_backtest("RELIANCE", timeframe="1d", fast_window=5, slow_window=20)

        self.assertEqual(payload["strategy_type"], "ma_crossover")
        self.assertIn("total_return_pct", payload)
        self.assertIn("trade_count", payload)
        self.assertIn("equity_curve_image", payload)
        self.assertTrue(Path(payload["equity_curve_image"]["image_path"]).exists())

    def test_export_backtest_report_and_image_write_files(self) -> None:
        service = NubraService(DummyPortfolioClient())
        service._historical_to_df = lambda *args, **kwargs: self._sample_backtest_df()  # type: ignore[method-assign]

        html_payload = service.export_backtest_report_html("RELIANCE", timeframe="1d", strategy_type="ma_crossover")
        image_payload = service.export_backtest_equity_curve_image("RELIANCE", timeframe="1d", strategy_type="ma_crossover")

        self.assertTrue(Path(html_payload["report_path"]).exists())
        self.assertTrue(html_payload["download_ready"])
        self.assertEqual(html_payload["file_kind"], "html")
        self.assertEqual(html_payload["preview_columns"], ["metric", "value"])
        self.assertTrue(Path(image_payload["image_path"]).exists())
        self.assertTrue(image_payload["download_ready"])
        self.assertEqual(image_payload["file_kind"], "image")
        self.assertEqual(Path(image_payload["image_path"]).suffix.lower(), ".png")


if __name__ == "__main__":
    unittest.main()
