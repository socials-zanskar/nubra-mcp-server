from __future__ import annotations

import json
import os
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from config import Settings
from nubra_client import NubraClient, NubraService
from strategy_backtest_engine import default_strategy_template


def _current_settings() -> Settings:
    settings = Settings.from_env()
    requested_env = os.getenv("NUBRA_BACKTEST_UI_ENV", "").strip().upper()
    if requested_env in {"PROD", "UAT"}:
        settings.environment = requested_env
        return settings
    settings.environment = "PROD"
    return settings


class BacktestUi(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Nubra Backtest UI")
        self.geometry("1440x900")
        self.minsize(1100, 760)
        self.settings = _current_settings()
        self.client = NubraClient(self.settings)
        self.service = NubraService(self.client)
        self._curve_image = None
        self._build_ui()
        self._load_template()
        self._load_catalog()

    def _build_ui(self) -> None:
        container = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        container.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(container, padding=16)
        right = ttk.Frame(container, padding=16)
        container.add(left, weight=3)
        container.add(right, weight=4)

        ttk.Label(left, text="Strategy Payload", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        ttk.Label(
            left,
            text="Edit a NubraOSS-style strategy JSON payload, then validate or run the backtest using your current Nubra MCP PROD session.",
            wraplength=480,
        ).pack(anchor="w", pady=(6, 12))

        toolbar = ttk.Frame(left)
        toolbar.pack(fill=tk.X, pady=(0, 10))
        ttk.Button(toolbar, text="Load Template", command=self._load_template).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Load Catalog", command=self._load_catalog).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(toolbar, text="Validate", command=self._validate_payload).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(toolbar, text="Run Backtest", command=self._run_backtest).pack(side=tk.LEFT, padx=(8, 0))

        self.payload_text = tk.Text(left, wrap="none", font=("Consolas", 10))
        payload_scroll_y = ttk.Scrollbar(left, orient=tk.VERTICAL, command=self.payload_text.yview)
        payload_scroll_x = ttk.Scrollbar(left, orient=tk.HORIZONTAL, command=self.payload_text.xview)
        self.payload_text.configure(yscrollcommand=payload_scroll_y.set, xscrollcommand=payload_scroll_x.set)
        self.payload_text.pack(fill=tk.BOTH, expand=True)
        payload_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        payload_scroll_x.pack(fill=tk.X)

        right_top = ttk.Frame(right)
        right_top.pack(fill=tk.X)
        ttk.Label(right_top, text="Results", font=("Segoe UI", 16, "bold")).pack(anchor="w")
        self.meta_label = ttk.Label(
            right_top,
            text=f"Environment: {self.settings.environment} | Auth state file: {self.settings.auth_state_path}",
            wraplength=720,
        )
        self.meta_label.pack(anchor="w", pady=(6, 10))

        notebook = ttk.Notebook(right)
        notebook.pack(fill=tk.BOTH, expand=True)

        summary_tab = ttk.Frame(notebook, padding=12)
        trades_tab = ttk.Frame(notebook, padding=12)
        image_tab = ttk.Frame(notebook, padding=12)
        catalog_tab = ttk.Frame(notebook, padding=12)

        notebook.add(summary_tab, text="Summary")
        notebook.add(trades_tab, text="Trades")
        notebook.add(image_tab, text="Equity Curve")
        notebook.add(catalog_tab, text="Catalog")

        self.summary_text = tk.Text(summary_tab, wrap="word", font=("Consolas", 10))
        self.summary_text.pack(fill=tk.BOTH, expand=True)

        self.trades_text = tk.Text(trades_tab, wrap="word", font=("Consolas", 10))
        self.trades_text.pack(fill=tk.BOTH, expand=True)

        self.image_label = ttk.Label(image_tab, text="Run a backtest to render the equity curve.")
        self.image_label.pack(anchor="center", expand=True)

        self.catalog_text = tk.Text(catalog_tab, wrap="word", font=("Consolas", 10))
        self.catalog_text.pack(fill=tk.BOTH, expand=True)

    def _load_template(self) -> None:
        self.payload_text.delete("1.0", tk.END)
        self.payload_text.insert("1.0", json.dumps(default_strategy_template(), indent=2))

    def _load_catalog(self) -> None:
        try:
            payload = self.service.get_strategy_backtest_catalog()
        except Exception as exc:
            messagebox.showerror("Catalog Load Failed", str(exc))
            return
        self.catalog_text.delete("1.0", tk.END)
        self.catalog_text.insert("1.0", json.dumps(payload, indent=2))

    def _read_payload(self) -> dict:
        raw = self.payload_text.get("1.0", tk.END).strip()
        if not raw:
            raise ValueError("Payload cannot be empty.")
        return json.loads(raw)

    def _validate_payload(self) -> None:
        try:
            payload = self._read_payload()
            result = self.service.validate_strategy_backtest_payload(payload)
        except Exception as exc:
            messagebox.showerror("Validation Failed", str(exc))
            return
        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert("1.0", json.dumps(result, indent=2))

    def _run_backtest(self) -> None:
        try:
            payload = self._read_payload()
            result = self.service.run_strategy_backtest(payload)
        except Exception as exc:
            message = str(exc)
            if "Session expired or missing" in message:
                message = (
                    "PROD backtest requires an active PROD Nubra session.\n\n"
                    "Please log in to PROD through the MCP first, then reopen or rerun the Backtest UI."
                )
            messagebox.showerror("Backtest Failed", message)
            return

        self.summary_text.delete("1.0", tk.END)
        self.summary_text.insert("1.0", json.dumps(result.get("portfolio", {}), indent=2))

        instrument_blocks: list[str] = []
        for instrument in result.get("instruments", []):
            instrument_blocks.append(json.dumps({"symbol": instrument.get("symbol"), "metrics": instrument.get("metrics"), "warning": instrument.get("warning")}, indent=2))
            trades = instrument.get("trades") or []
            if trades:
                instrument_blocks.append(json.dumps({"trades": trades[:20]}, indent=2))
        self.trades_text.delete("1.0", tk.END)
        self.trades_text.insert("1.0", "\n\n".join(instrument_blocks) if instrument_blocks else "No trades returned.")

        image_path = str(((result.get("equity_curve_image") or {}).get("path")) or "").strip()
        if image_path and Path(image_path).exists():
            try:
                self._curve_image = tk.PhotoImage(file=image_path)
                self.image_label.configure(image=self._curve_image, text="")
            except Exception:
                self.image_label.configure(image="", text=f"Equity curve saved to:\n{image_path}")
        else:
            self.image_label.configure(image="", text="No equity curve image returned.")


def main() -> None:
    app = BacktestUi()
    app.mainloop()


if __name__ == "__main__":
    main()
