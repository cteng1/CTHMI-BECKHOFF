import argparse
import csv
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import panel as pn
import pyads

pn.extension(sizing_mode="stretch_width")
pn.config.sizing_mode = "stretch_width"


@dataclass
class VariableSpec:
    name: str
    references: str


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
            watch_csv_path: str = "watchlist.csv",
            poll_ms: int = 1000,
    ):
        self.ams_net_id = ams_net_id
        self.ams_port = ams_port
        self.host = host
        self.csv_path = csv_path
        self.watch_csv_path = watch_csv_path
        self.poll_ms = poll_ms

        self.groups = self._load_layout_csv(csv_path)
        self.variable_specs = self._build_variable_index(self.groups)
        self._all_variable_names: List[str] = list(self.variable_specs.keys())

        self._plc: Optional[pyads.Connection] = None
        self._lock = threading.Lock()
        self._connected = False
        self._read_errors = 0
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._result_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
        self._connect_in_progress = False

        self.current_values: Dict[str, str] = {
            variable_name: "Waiting for data..." for variable_name in self._all_variable_names
        }

        self.status = pn.pane.Alert("Disconnected", alert_type="warning", sizing_mode="stretch_width")
        self.last_update = pn.pane.Markdown("**Last update:** never")

        self.value_widgets: Dict[str, pn.pane.HTML] = {}
        self.boxes = self._build_boxes()

        self.watch_set: Set[str] = self._load_watch_csv(self.watch_csv_path)
        self.watch_column = pn.Column(sizing_mode="stretch_both")

        self.search_input = pn.widgets.TextInput(name="Search", placeholder="Search variable or references")
        self.search_results = pn.widgets.MultiSelect(name="Matches", options={}, size=12, sizing_mode="stretch_width")
        self.add_watch_button = pn.widgets.Button(name="Add to watch", button_type="primary")

        self.search_input.param.watch(self._update_search_results, "value")
        self.add_watch_button.on_click(self._on_add_to_watch)

        self.connect_button = pn.widgets.Button(name="Connect", button_type="primary")
        self.disconnect_button = pn.widgets.Button(name="Disconnect", button_type="light")
        self.refresh_button = pn.widgets.Button(name="Refresh now", button_type="default")

        self.connect_button.on_click(self._on_connect)
        self.disconnect_button.on_click(self._on_disconnect)
        self.refresh_button.on_click(self._on_manual_refresh)

        self._ui_callback = pn.state.add_periodic_callback(self._drain_queue, period=200, start=True)

        self._update_search_results()
        self._refresh_watch_widgets()

    @staticmethod
    def _escape_html(text: str) -> str:
        return (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    @classmethod
    def _format_row_html(cls, variable_name: str, references: str, value: str) -> str:
        return (
            "<div style='display:grid;grid-template-columns:minmax(180px,1.4fr) minmax(120px,1fr) minmax(120px,1fr);"
            "column-gap:12px;align-items:start;padding:6px 0;border-bottom:1px solid #ececec;'>"
            f"<div><div style='font-size:11px;color:#666;'>Variable</div><div>{cls._escape_html(variable_name)}</div></div>"
            f"<div><div style='font-size:11px;color:#666;'>References</div><div>{cls._escape_html(references)}</div></div>"
            f"<div><div style='font-size:11px;color:#666;'>Value</div><div>{cls._escape_html(value)}</div></div>"
            "</div>"
        )

    @staticmethod
    def _load_layout_csv(csv_path: str) -> List[GroupSpec]:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        groups: Dict[str, List[VariableSpec]] = {}
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            required = {"title", "variable", "references"}
            available = {name.strip().lower() for name in (reader.fieldnames or [])}
            if not reader.fieldnames or not required.issubset(available):
                raise ValueError("CSV must contain headers: title, variable, references")

            field_map = {name.strip().lower(): name for name in reader.fieldnames}
            title_key = field_map["title"]
            variable_key = field_map["variable"]
            references_key = field_map["references"]

            for row in reader:
                title = (row.get(title_key) or "").strip()
                variable = (row.get(variable_key) or "").strip()
                references = (row.get(references_key) or "").strip()
                if not title or not variable:
                    continue
                groups.setdefault(title, []).append(VariableSpec(name=variable, references=references))

        if not groups:
            raise ValueError("No valid rows found in CSV")

        return [GroupSpec(title=title, variables=variables) for title, variables in groups.items()]

    @staticmethod
    def _build_variable_index(groups: List[GroupSpec]) -> Dict[str, VariableSpec]:
        variable_specs: Dict[str, VariableSpec] = {}
        for group in groups:
            for spec in group.variables:
                variable_specs[spec.name] = spec
        return variable_specs

    @staticmethod
    def _load_watch_csv(csv_path: str) -> Set[str]:
        if not os.path.exists(csv_path):
            return set()

        watch_set: Set[str] = set()
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return set()
            field_map = {name.strip().lower(): name for name in reader.fieldnames}
            if "variable" not in field_map:
                return set()
            variable_key = field_map["variable"]
            for row in reader:
                variable = (row.get(variable_key) or "").strip()
                if variable:
                    watch_set.add(variable)
        return watch_set

    def _save_watch_csv(self) -> None:
        with open(self.watch_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["variable"])
            writer.writeheader()
            for variable_name in sorted(self.watch_set):
                writer.writerow({"variable": variable_name})

    def _build_variable_widget(self, spec: VariableSpec) -> pn.pane.HTML:
        pane = pn.pane.HTML(
            self._format_row_html(spec.name, spec.references,
                                  self.current_values.get(spec.name, "Waiting for data...")),
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
        )
        self.value_widgets[spec.name] = pane
        return pane

    def _build_boxes(self) -> List[pn.Column]:
        boxes: List[pn.Column] = []
        for group in self.groups:
            rows = [self._build_variable_widget(spec) for spec in group.variables]
            box = pn.Column(
                pn.pane.Markdown(f"### {group.title}", margin=(0, 0, 8, 0)),
                *rows,
                sizing_mode="stretch_both",
                min_height=max(180, 90 + 48 * len(rows)),
                margin=(8, 8, 8, 8),
                styles={
                    "border": "1px solid #d9d9d9",
                    "border-radius": "10px",
                    "padding": "12px 12px 28px 12px",
                    "background": "#fafafa",
                    "box-shadow": "0 1px 3px rgba(0,0,0,0.08)",
                    "height": "100%",
                    "box-sizing": "border-box",
                    "overflow": "hidden",
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

    def _disconnect(self) -> None:
        with self._lock:
            if self._plc is not None:
                try:
                    self._plc.close()
                except Exception:
                    pass
            self._plc = None
            self._connected = False

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
        values: Dict[str, object] = {}
        try:
            with self._lock:
                if self._plc is None:
                    raise RuntimeError("PLC is not connected")
                values = self._plc.read_list_by_name(self._all_variable_names, cache_symbol_info=True)
        except Exception as exc:
            had_error = True
            status_message = f"Bulk read failed: {exc}"
            self._read_errors += 1
            for variable_name in self._all_variable_names:
                try:
                    with self._lock:
                        if self._plc is None:
                            raise RuntimeError("PLC is not connected")
                        values[variable_name] = self._plc.read_by_name(variable_name)
                except Exception as inner_exc:
                    values[variable_name] = f"ERROR: {inner_exc}"

        for variable_name in self._all_variable_names:
            formatted = self._format_value(values.get(variable_name, "<missing>"))
            self._result_queue.put((variable_name, formatted))

        self._result_queue.put(("__meta_last_update__", time.strftime("%Y-%m-%d %H:%M:%S")))
        if had_error:
            self._result_queue.put(("__meta_status__", f"warning::{status_message}"))
        else:
            self._result_queue.put(("__meta_status__", "success"))

    def _drain_queue(self) -> None:
        latest_status = None
        last_update = None
        changed_variables: Set[str] = set()

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
                self.current_values[key] = value
                spec = self.variable_specs[key]
                self.value_widgets[key].object = self._format_row_html(spec.name, spec.references, value)
                changed_variables.add(key)

        if changed_variables & self.watch_set:
            self._refresh_watch_widgets()

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

    def _update_search_results(self, _event=None) -> None:
        query = (self.search_input.value or "").strip().lower()
        options = {}

        variable_width = max(
            [8] + [len(name) for name in self.variable_specs.keys()]
        )

        for variable_name, spec in self.variable_specs.items():
            haystack = f"{variable_name} {spec.references}".lower()
            if query and query not in haystack:
                continue

            label = f"{variable_name:<{variable_width}} | {spec.references}"
            options[label] = variable_name

        self.search_results.options = options

    def _refresh_watch_widgets(self) -> None:
        watch_items = []
        for variable_name in sorted(self.watch_set):
            spec = self.variable_specs.get(variable_name)
            if spec is None:
                continue

            row_html = pn.pane.HTML(
                self._format_row_html(
                    spec.name,
                    spec.references,
                    self.current_values.get(variable_name, "Waiting for data...")
                ),
                sizing_mode="stretch_width",
                margin=(0, 0, 0, 0),
            )

            remove_button = pn.widgets.Button(
                name="Remove",
                button_type="danger",
                width=90,
                align="end",
            )
            remove_button.on_click(lambda _event, v=variable_name: self._remove_watch_item(v))

            row = pn.Row(
                pn.Column(row_html, sizing_mode="stretch_width"),
                remove_button,
                sizing_mode="stretch_width",
                margin=(0, 0, 6, 0),
            )
            watch_items.append(row)

        self.watch_column.objects = watch_items or [
            pn.pane.Markdown("_No watch variables selected._", margin=(0, 0, 8, 0))
        ]

    def _remove_watch_item(self, variable_name: str) -> None:
        self.watch_set.discard(variable_name)
        self._save_watch_csv()
        self._refresh_watch_widgets()

    def _on_add_to_watch(self, _event=None) -> None:
        selected = list(self.search_results.value)
        if not selected:
            return
        for variable_name in selected:
            if variable_name in self.variable_specs:
                self.watch_set.add(variable_name)
        self._save_watch_csv()
        self._refresh_watch_widgets()

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

        grid = pn.GridBox(
            *self.boxes,
            ncols=2,
            sizing_mode="stretch_both",
            min_height=500,
        )

        watch_controls = pn.Column(
            "## Watch Window",
            self.watch_column,
            pn.Spacer(height=16),
            "## Search",
            self.search_input,
            self.search_results,
            pn.Row(self.add_watch_button, sizing_mode="stretch_width"),
            sizing_mode="stretch_both",
            min_width=420,
            styles={
                "border": "1px solid #d9d9d9",
                "border-radius": "10px",
                "padding": "12px 12px 24px 12px",
                "background": "#ffffff",
                "box-shadow": "0 1px 3px rgba(0,0,0,0.08)",
            },
        )

        main_content = pn.Row(
            pn.Column(grid, sizing_mode="stretch_both"),
            watch_controls,
            sizing_mode="stretch_both",
        )

        return pn.Column(
            "# ADS Variable Monitor",
            self.status,
            controls,
            self.last_update,
            pn.Spacer(height=8),
            main_content,
            sizing_mode="stretch_both",
            min_height=700,
            styles={"padding-bottom": "24px"},
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
        watch_csv_path=args.watch_csv,
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
    parser.add_argument("--csv", default="layout.csv", help="CSV file with title, variable, references columns")
    parser.add_argument("--watch-csv", default="watchlist.csv", help="CSV file used to persist the watch window")
    parser.add_argument("--poll-ms", type=int, default=1000, help="Polling interval in milliseconds")
    parser.add_argument("--auto-connect", action="store_true", help="Connect immediately on launch")
    return parser.parse_args()


if __name__.startswith("bokeh"):
    ARGS = parse_args()
    pn.config.notifications = True
    app = build_app(ARGS)
    app.servable()
elif __name__ == "__main__":
    ARGS = parse_args()
    pn.serve(build_app(ARGS), show=True, title="ADS Variable Monitor")
