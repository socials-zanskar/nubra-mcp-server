from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import Settings
from nubra_client import AuthState, HistoricalQuery, NubraAPIError, NubraClient, NubraService


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
    def test_auth_state_path_resolves_relative_to_repo(self) -> None:
        settings = Settings(auth_state_file="tmp/auth_state.json")
        self.assertTrue(settings.auth_state_path.is_absolute())
        self.assertTrue(str(settings.auth_state_path).endswith("tmp\\auth_state.json"))

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
        self.assertTrue(query["intraDay"])


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
