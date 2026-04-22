import argparse
import csv
import os
import threading
import time
import queue
from dataclasses import dataclass
from typing import Dict, List, Optional

import panel as pn
import pyads
from pyads import constants

pn.extension(sizing_mode="stretch_width")


@dataclass
class VariableSpec:
    name: str


@dataclass
class GroupSpec:
    title: str
    variables: List[VariableSpec]


class AdsMonitor:
    def __init__(
        self,
        ams_net_id: str,
        ams_port: int,
        host: Optional[str] = None,
        csv_path: str = "layout.csv",
        poll_ms: int = 1000,
    ):
        self.ams_net_id = ams_net_id
        self.ams_port = ams_port
        self.host = host
        self.csv_path = csv_path
        self.poll_ms = poll_ms
        self.groups = self._load_csv(csv_path)

        self._plc: Optional[pyads.Connection] = None
        self._lock = threading.Lock()
        self._connected = False
        self._read_errors = 0
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._result_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
        self._connect_in_progress = False
        self._all_variable_names: List[str] = [
            spec.name for group in self.groups for spec in group.variables
        ]
        self._symbol_info = {}

        self.status = pn.pane.Alert("Disconnected", alert_type="warning", sizing_mode="stretch_width")
        self.last_update = pn.pane.Markdown("**Last update:** never")

        self.value_widgets: Dict[str, pn.widgets.StaticText] = {}
        self.boxes = self._build_boxes()

        self.connect_button = pn.widgets.Button(name="Connect", button_type="primary")
        self.disconnect_button = pn.widgets.Button(name="Disconnect", button_type="light")
        self.refresh_button = pn.widgets.Button(name="Refresh now", button_type="default")

        self.connect_button.on_click(self._on_connect)
        self.disconnect_button.on_click(self._on_disconnect)
        self.refresh_button.on_click(self._on_manual_refresh)

        self._ui_callback = pn.state.add_periodic_callback(self._drain_queue, period=200, start=True)

    @staticmethod
    def _load_csv(csv_path: str) -> List[GroupSpec]:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        groups: Dict[str, List[VariableSpec]] = {}
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            required = {"title", "variable"}
            if not reader.fieldnames or not required.issubset({name.strip().lower() for name in reader.fieldnames}):
                raise ValueError("CSV must contain headers: title, variable")

            field_map = {name.strip().lower(): name for name in reader.fieldnames}
            title_key = field_map["title"]
            variable_key = field_map["variable"]

            for row in reader:
                title = (row.get(title_key) or "").strip()
                variable = (row.get(variable_key) or "").strip()
                if not title or not variable:
                    continue
                groups.setdefault(title, []).append(VariableSpec(variable))

        if not groups:
            raise ValueError("No valid rows found in CSV")

        return [GroupSpec(title=title, variables=variables) for title, variables in groups.items()]

    def _build_boxes(self) -> List[pn.Column]:
        boxes: List[pn.Column] = []
        for group in self.groups:
            rows = []
            for spec in group.variables:
                widget = pn.widgets.StaticText(name=spec.name, value="Waiting for data...")
                self.value_widgets[spec.name] = widget
                rows.append(widget)

            box = pn.Column(
                pn.pane.Markdown(f"### {group.title}"),
                *rows,
                styles={
                    "border": "1px solid #d9d9d9",
                    "border-radius": "10px",
                    "padding": "12px",
                    "background": "#fafafa",
                    "box-shadow": "0 1px 3px rgba(0,0,0,0.08)",
                },
            )
            boxes.append(box)
        return boxes

    def _connect(self) -> None:
        with self._lock:
            if self._connected and self._plc is not None:
                return

            plc = pyads.Connection(self.ams_net_id, self.ams_port, self.host)
            plc.open()
            self._plc = plc
            self._connected = True
            self._read_errors = 0
            self._prime_symbol_info()

    def _disconnect(self) -> None:
        with self._lock:
            if self._plc is not None:
                try:
                    self._plc.close()
                except Exception:
                    pass
            self._plc = None
            self._connected = False
            self._symbol_info = {}

    def _prime_symbol_info(self) -> None:
        if self._plc is None:
            return
        symbol_info = {}
        failures = []
        for variable_name in self._all_variable_names:
            try:
                symbol_info[variable_name] = self._plc.get_symbol(variable_name, auto_update=False)
            except Exception as exc:
                failures.append(f"{variable_name}: {exc}")
        self._symbol_info = symbol_info
        if failures:
            self._result_queue.put(("__meta_status__", f"warning::Symbol load issues: {'; '.join(failures[:3])}"))

    def _start_worker(self) -> None:
        if self._worker_thread is not None and self._worker_thread.is_alive():
            return
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._connected:
                self._read_all_values_once()
            time.sleep(max(self.poll_ms / 1000.0, 0.1))

    def _read_all_values_once(self) -> None:
        had_error = False
        status_message = None
        try:
            values = self._read_values_bulk()
            for variable_name in self._all_variable_names:
                value = values.get(variable_name, "<missing>")
                self._result_queue.put((variable_name, self._format_value(value)))
        except Exception as exc:
            had_error = True
            self._read_errors += 1
            status_message = f"Bulk read failed: {exc}"
            for variable_name in self._all_variable_names:
                try:
                    value = self._read_single_value(variable_name)
                    self._result_queue.put((variable_name, self._format_value(value)))
                except Exception as inner_exc:
                    self._result_queue.put((variable_name, f"ERROR: {inner_exc}"))

        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        self._result_queue.put(("__meta_last_update__", now_str))
        if had_error:
            self._result_queue.put(("__meta_status__", f"warning::{status_message}"))
        else:
            self._result_queue.put(("__meta_status__", "success"))

    def _read_values_bulk(self) -> Dict[str, object]:
        with self._lock:
            if self._plc is None:
                raise RuntimeError("PLC is not connected")
            if not self._all_variable_names:
                return {}

            # read_list_by_name is much faster than many individual read_by_name calls.
            return self._plc.read_list_by_name(
                self._all_variable_names,
                cache_symbol_info=True,
            )

    def _read_single_value(self, variable_name: str):
        with self._lock:
            if self._plc is None:
                raise RuntimeError("PLC is not connected")
            symbol = self._symbol_info.get(variable_name)
            if symbol is not None:
                return symbol.read()
            return self._plc.read_by_name(variable_name, plc_datatype=constants.PLCTYPE_STRING)

    def _drain_queue(self) -> None:
        latest_status = None
        last_update = None
        while True:
            try:
                key, value = self._result_queue.get_nowait()
            except queue.Empty:
                break

            if key == "__meta_last_update__":
                last_update = value
            elif key == "__meta_status__":
                latest_status = value
            elif key in self.value_widgets:
                self.value_widgets[key].value = value

        if last_update is not None:
            self.last_update.object = f"**Last update:** {last_update}"

        if latest_status == "success":
            self.status.object = "Connected"
            self.status.alert_type = "success"
        elif isinstance(latest_status, str) and latest_status.startswith("warning::"):
            self.status.object = latest_status.split("::", 1)[1]
            self.status.alert_type = "warning"
        elif isinstance(latest_status, str) and latest_status.startswith("danger::"):
            self.status.object = latest_status.split("::", 1)[1]
            self.status.alert_type = "danger"

    def _connect_background(self) -> None:
        if self._connect_in_progress:
            return
        self._connect_in_progress = True
        self.status.object = "Connecting..."
        self.status.alert_type = "warning"

        def runner():
            try:
                self._connect()
                self._start_worker()
                self._result_queue.put(("__meta_status__", "success"))
            except Exception as exc:
                self._result_queue.put(("__meta_status__", f"danger::{exc}"))
            finally:
                self._connect_in_progress = False

        threading.Thread(target=runner, daemon=True).start()

    def _on_connect(self, _event=None) -> None:
        self._connect_background()

    def _on_disconnect(self, _event=None) -> None:
        self._stop_event.set()
        self._disconnect()
        self.status.object = "Disconnected"
        self.status.alert_type = "warning"

    def _on_manual_refresh(self, _event=None) -> None:
        if not self._connected:
            self._connect_background()
        else:
            threading.Thread(target=self._read_all_values_once, daemon=True).start()

    @staticmethod
    def _format_value(value) -> str:
        if isinstance(value, float):
            return f"{value:.3f}"
        if isinstance(value, (list, tuple)):
            return ", ".join(str(v) for v in value)
        return str(value)

    def view(self):
        controls = pn.Row(
            self.connect_button,
            self.disconnect_button,
            self.refresh_button,
            sizing_mode="stretch_width",
        )

        grid = pn.GridBox(*self.boxes, ncols=2, sizing_mode="stretch_both")

        return pn.Column(
            "# ADS Variable Monitor",
            self.status,
            controls,
            self.last_update,
            grid,
            sizing_mode="stretch_both",
        )

    def close(self):
        self._stop_event.set()
        self._ui_callback.stop()
        self._disconnect()


def build_app(args) -> pn.Column:
    monitor = AdsMonitor(
        ams_net_id=args.ams_net_id,
        ams_port=args.ams_port,
        host=args.host,
        csv_path=args.csv,
        poll_ms=args.poll_ms,
    )

    if args.auto_connect:
        monitor._on_connect()

    pn.state.on_session_destroyed(lambda _context: monitor.close())
    return monitor.view()


def parse_args():
    parser = argparse.ArgumentParser(description="Panel GUI for pyads variable monitoring")
    parser.add_argument("--ams-net-id", required=True, help="Target AMS Net ID, e.g. 5.44.33.22.1.1")
    parser.add_argument("--ams-port", type=int, default=851, help="ADS port, usually 851 for TwinCAT 3 PLC1")
    parser.add_argument("--host", default=None, help="Optional IP or hostname for the target")
    parser.add_argument("--csv", default="layout.csv", help="CSV file with title and variable columns")
    parser.add_argument("--poll-ms", type=int, default=1000, help="Polling interval in milliseconds")
    parser.add_argument("--auto-connect", action="store_true", help="Connect immediately on launch")
    return parser.parse_args()


if __name__.startswith("bokeh"):
    # Running via: panel serve plc_panel_monitor.py --args ...
    ARGS = parse_args()
    pn.config.notifications = True
    app = build_app(ARGS)
    app.servable()
elif __name__ == "__main__":
    ARGS = parse_args()
    pn.serve(build_app(ARGS), show=True, title="ADS Variable Monitor")
