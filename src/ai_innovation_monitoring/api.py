from __future__ import annotations

import json
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ai_innovation_monitoring.orchestrator import MonitorService


def _dashboard_html() -> str:
    path = Path(__file__).resolve().parent / "ui" / "dashboard.html"
    return path.read_text(encoding="utf-8")


def build_http_server(host: str, port: int, monitor: MonitorService) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "AIMonitorHTTP/0.1"

        def _write_json(self, status: int, payload: dict | list) -> None:
            encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _write_html(self, status: int, payload: str) -> None:
            encoded = payload.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/dashboard"}:
                self._write_html(HTTPStatus.OK, _dashboard_html())
                return
            if parsed.path == "/health":
                self._write_json(HTTPStatus.OK, monitor.health())
                return
            if parsed.path == "/dashboard-data":
                params = parse_qs(parsed.query)
                activity_limit = int(params.get("activity_limit", ["80"])[0])
                self._write_json(HTTPStatus.OK, monitor.dashboard_snapshot(activity_limit=activity_limit))
                return
            if parsed.path == "/events":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["20"])[0])
                self._write_json(HTTPStatus.OK, monitor.store.list_recent_events(limit=limit))
                return
            if parsed.path == "/orders":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["20"])[0])
                self._write_json(HTTPStatus.OK, monitor.store.list_recent_orders(limit=limit))
                return
            if parsed.path == "/registry":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["100"])[0])
                self._write_json(HTTPStatus.OK, monitor.store.list_company_profiles(limit=limit))
                return
            if parsed.path == "/impact-assessments":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["50"])[0])
                self._write_json(HTTPStatus.OK, monitor.store.list_recent_impact_assessments(limit=limit))
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/run-once":
                stats = monitor.run_once()
                self._write_json(HTTPStatus.OK, asdict(stats))
                return
            if parsed.path == "/ingest":
                token = self.headers.get("X-Ingest-Token", "")
                if token != monitor.config.runner.ingest_token:
                    self._write_json(HTTPStatus.UNAUTHORIZED, {"error": "invalid_ingest_token"})
                    return
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                result = monitor.ingest_external(payload)
                self._write_json(HTTPStatus.OK, result)
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    return ThreadingHTTPServer((host, port), Handler)
