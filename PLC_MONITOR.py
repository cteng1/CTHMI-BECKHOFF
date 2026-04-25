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

pn.extension(sizing_mode="stretch_width", notifications=True)
pn.config.sizing_mode = "stretch_width"


@dataclass
class VariableSpec:
    name: str
    references: str
    description: str


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
            cleaned_csv_path: str = "layout.cleaned.csv",
            poll_ms: int = 1000,
    ):
        self.ams_net_id = ams_net_id
        self.ams_port = ams_port
        self.host = host
        self.csv_path = csv_path
        self.watch_csv_path = watch_csv_path
        self.cleaned_csv_path = cleaned_csv_path
        self.poll_ms = poll_ms

        self.groups = self._load_layout_csv(csv_path)
        self._save_cleaned_layout_csv(self.cleaned_csv_path, self.groups)
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
        self.expanded_descriptions: Set[str] = set()
        self.watch_row_containers: Dict[str, pn.Column] = {}
        self.main_row_containers: Dict[str, pn.Column] = {}

        self.value_widgets: Dict[str, pn.pane.HTML] = {}
        self.watch_value_widgets: Dict[str, pn.pane.HTML] = {}
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
            "<div style='padding:6px 0;border-bottom:1px solid #ececec;font-size:13px;'>"
            "<div style='display:flex;flex-wrap:wrap;gap:8px 16px;align-items:flex-start;'>"
            f"<div style='min-width:150px;flex:1 1 180px;overflow-wrap:anywhere;'>"
            f"<div style='font-size:10px;color:#666;'>Variable</div>"
            f"<div>{cls._escape_html(variable_name)}</div>"
            f"</div>"
            f"<div style='min-width:90px;flex:1 1 110px;overflow-wrap:anywhere;'>"
            f"<div style='font-size:10px;color:#666;'>References</div>"
            f"<div>{cls._escape_html(references)}</div>"
            f"</div>"
            f"<div style='min-width:120px;flex:1 1 130px;overflow-wrap:anywhere;font-weight:600;'>"
            f"<div style='font-size:10px;color:#666;font-weight:400;'>Value</div>"
            f"<div>{cls._escape_html(value)}</div>"
            f"</div>"
            "</div>"
            "</div>"
        )

    @staticmethod
    def _load_layout_csv(csv_path: str) -> List[GroupSpec]:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        groups: Dict[str, List[VariableSpec]] = {}
        seen_variables: Set[str] = set()

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            required = {"title", "variable", "references", "description"}
            available = {name.strip().lower() for name in (reader.fieldnames or [])}
            if not reader.fieldnames or not required.issubset(available):
                raise ValueError("CSV must contain headers: title, variable, references, description")

            field_map = {name.strip().lower(): name for name in reader.fieldnames}
            title_key = field_map["title"]
            variable_key = field_map["variable"]
            references_key = field_map["references"]
            description_key = field_map["description"]

            for row in reader:
                title = (row.get(title_key) or "").strip()
                raw_variable = row.get(variable_key) or ""
                variable = raw_variable.rstrip()
                references = (row.get(references_key) or "").strip()
                description = (row.get(description_key) or "").strip()

                if not title or not variable:
                    continue

                if any(ch.isspace() for ch in variable):
                    continue

                if variable in seen_variables:
                    continue

                seen_variables.add(variable)
                groups.setdefault(title, []).append(
                    VariableSpec(name=variable, references=references, description=description))

        cleaned_groups = [
            GroupSpec(title=title, variables=variables)
            for title, variables in groups.items()
            if variables
        ]

        if not cleaned_groups:
            raise ValueError("No valid rows found in CSV after cleanup")

        return cleaned_groups

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

    def _build_write_controls(self, spec: VariableSpec) -> pn.Row:
        write_input = pn.widgets.TextInput(
            placeholder="Write value",
            width=120,
            margin=(0, 4, 0, 0),
        )
        write_button = pn.widgets.Button(
            name="Write",
            button_type="warning",
            width=70,
            margin=(0, 0, 0, 0),
        )
        write_button.on_click(lambda _event, s=spec, w=write_input: self._write_variable_background(s, w.value))
        return pn.Row(write_input, write_button, sizing_mode="fixed", margin=(0, 0, 0, 8))

    def _build_variable_widget(self, spec: VariableSpec) -> pn.Column:
        pane = pn.pane.HTML(
            self._format_row_html(spec.name, spec.references,
                                  self.current_values.get(spec.name, "Waiting for data...")),
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
        )
        info_button = pn.widgets.Button(name="Info", button_type="light", width=70)
        info_button.on_click(lambda _event, s=spec: self._toggle_description(s))
        write_controls = self._build_write_controls(spec)
        self.value_widgets[spec.name] = pane

        row = pn.Row(
            pane,
            write_controls,
            info_button,
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
        )
        container = pn.Column(row, sizing_mode="stretch_width", margin=(0, 0, 0, 0))
        self.main_row_containers[spec.name] = container
        self._render_main_row(spec.name)
        return container

    def _build_description_pane(self, spec: VariableSpec) -> pn.pane.HTML:
        description = spec.description or "No description available."
        return pn.pane.HTML(
            "<div style='margin:6px 0 10px 0;padding:10px 12px;border-left:3px solid #7a7a7a;"
            "background:#f5f5f5;border-radius:6px;white-space:normal;'>"
            f"<div style='font-size:12px;color:#666;margin-bottom:4px;'>Description</div>"
            f"<div>{self._escape_html(description)}</div>"
            "</div>",
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
        )

    def _toggle_description(self, spec: VariableSpec) -> None:
        if spec.name in self.expanded_descriptions:
            self.expanded_descriptions.discard(spec.name)
        else:
            self.expanded_descriptions.add(spec.name)
        self._render_main_row(spec.name)
        if spec.name in self.watch_set:
            self._refresh_watch_widgets()

    def _render_main_row(self, variable_name: str) -> None:
        spec = self.variable_specs.get(variable_name)
        container = self.main_row_containers.get(variable_name)
        pane = self.value_widgets.get(variable_name)
        if spec is None or container is None or pane is None:
            return

        info_button = pn.widgets.Button(name="Info", button_type="light", width=70)
        info_button.on_click(lambda _event, s=spec: self._toggle_description(s))
        write_controls = self._build_write_controls(spec)
        base_row = pn.Row(
            pane,
            write_controls,
            info_button,
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
        )
        objects = [base_row]
        if variable_name in self.expanded_descriptions:
            objects.append(self._build_description_pane(spec))
        container.objects = objects

    @staticmethod
    def _save_cleaned_layout_csv(csv_path: str, groups: List[GroupSpec]) -> None:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["title", "variable", "references", "description"])
            writer.writeheader()
            for group in groups:
                for spec in group.variables:
                    writer.writerow({
                        "title": group.title,
                        "variable": spec.name,
                        "references": spec.references,
                        "description": spec.description,
                    })

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

        for variable_name in changed_variables & self.watch_set:
            self._update_watch_value_widget(variable_name)

        if last_update is not None:
            self.last_update.object = f"**Last update:** {last_update}"

        if latest_status == "success":
            self.status.object = "Connected"
            self.status.alert_type = "success"
        elif isinstance(latest_status, str) and latest_status.startswith("success::"):
            self.status.object = latest_status.split("::", 1)[1]
            self.status.alert_type = "success"
        elif isinstance(latest_status, str) and latest_status.startswith("warning::"):
            self.status.object = latest_status.split("::", 1)[1]
            self.status.alert_type = "warning"
        elif isinstance(latest_status, str) and latest_status.startswith("danger::"):
            self.status.object = latest_status.split("::", 1)[1]
            self.status.alert_type = "danger"

    def _coerce_write_value(self, variable_name: str, raw_value: str):
        text = (raw_value or "").strip()
        current = self.current_values.get(variable_name, "")
        current_lower = str(current).strip().lower()
        text_lower = text.lower()

        if text_lower in {"true", "false"}:
            return text_lower == "true"
        if current_lower in {"true", "false"} and text_lower in {"1", "0", "yes", "no", "on", "off"}:
            return text_lower in {"1", "yes", "on"}

        try:
            if "." in text:
                return float(text)
            return int(text)
        except ValueError:
            return text

    def _write_variable_background(self, spec: VariableSpec, raw_value: str) -> None:
        if not raw_value or not raw_value.strip():
            self.status.object = f"Write skipped for {spec.name}: empty value"
            self.status.alert_type = "warning"
            return

        def runner():
            try:
                value = self._coerce_write_value(spec.name, raw_value)
                with self._lock:
                    if self._plc is None:
                        raise RuntimeError("PLC is not connected")
                    self._plc.write_by_name(spec.name, value)
                self._result_queue.put(("__meta_status__", f"success::Wrote {raw_value} to {spec.name}"))
                self._read_all_values_once()
            except Exception as exc:
                self._result_queue.put(("__meta_status__", f"danger::Write failed for {spec.name}: {exc}"))

        threading.Thread(target=runner, daemon=True).start()

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

    def _update_watch_value_widget(self, variable_name: str) -> None:
        spec = self.variable_specs.get(variable_name)
        widget = self.watch_value_widgets.get(variable_name)
        if spec is None or widget is None:
            return
        widget.object = self._format_row_html(
            spec.name,
            spec.references,
            self.current_values.get(variable_name, "Waiting for data...")
        )

    def _refresh_watch_widgets(self) -> None:
        watch_items = []
        self.watch_row_containers = {}
        self.watch_value_widgets = {}

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
            self.watch_value_widgets[variable_name] = row_html

            info_button = pn.widgets.Button(
                name="Info",
                button_type="light",
                width=70,
                align="end",
            )
            info_button.on_click(lambda _event, s=spec: self._toggle_description(s))

            remove_button = pn.widgets.Button(
                name="Remove",
                button_type="danger",
                width=90,
                align="end",
            )
            remove_button.on_click(lambda _event, v=variable_name: self._remove_watch_item(v))

            write_controls = self._build_write_controls(spec)

            base_row = pn.Row(
                pn.Column(row_html, sizing_mode="stretch_width"),
                write_controls,
                pn.Row(info_button, remove_button, sizing_mode="fixed", margin=(0, 0, 0, 8)),
                sizing_mode="stretch_width",
                margin=(0, 0, 6, 0),
            )
            container = pn.Column(base_row, sizing_mode="stretch_width", margin=(0, 0, 6, 0))
            if variable_name in self.expanded_descriptions:
                container.append(self._build_description_pane(spec))
            self.watch_row_containers[variable_name] = container
            watch_items.append(container)

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

        grid = pn.Column(
            *self.boxes,
            sizing_mode="stretch_width",
            min_height=500,
        )

        watch_controls = pn.Column(
            pn.pane.Markdown("## Watch Window", margin=(0, 0, 8, 0)),
            self.watch_column,
            pn.Spacer(height=16),
            pn.pane.Markdown("## Search", margin=(0, 0, 8, 0)),
            self.search_input,
            self.search_results,
            pn.Row(self.add_watch_button, sizing_mode="stretch_width"),
            sizing_mode="stretch_width",
            min_width=560,
            width=650,
            max_width=760,
            styles={
                "border": "1px solid #d9d9d9",
                "border-radius": "10px",
                "padding": "12px 12px 24px 12px",
                "background": "#ffffff",
                "box-shadow": "0 1px 3px rgba(0,0,0,0.08)",
                "box-sizing": "border-box",
                "position": "sticky",
                "top": "12px",
                "align-self": "flex-start",
                "max-height": "calc(100vh - 24px)",
                "width": "100%",
                "overflow-y": "auto",
                "z-index": "10",
            },
        )

        main_content = pn.Row(
            pn.Column(grid, sizing_mode="stretch_width", min_width=0),
            watch_controls,
            sizing_mode="stretch_width",
            styles={"align-items": "flex-start", "gap": "12px"},
        )

        return pn.Column(
            pn.pane.Markdown("# ADS Variable Monitor", margin=(0, 0, 8, 0)),
            self.status,
            controls,
            self.last_update,
            pn.Spacer(height=8),
            main_content,
            sizing_mode="stretch_width",
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
        cleaned_csv_path=args.cleaned_csv,
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
    parser.add_argument("--cleaned-csv", default="layout.cleaned.csv", help="Cleaned layout CSV written on startup")
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
