from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict

from ai_innovation_monitoring.config import config_to_dict, load_config
from ai_innovation_monitoring.orchestrator import build_monitor_service


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI innovation monitoring and trading pipeline")
    parser.add_argument("--config", help="Path to config/app.local.json", default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the monitor once or forever")
    run_parser.add_argument("--once", action="store_true", help="Run a single cycle")

    serve_parser = subparsers.add_parser("serve", help="Serve HTTP API and monitor loop")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", default=8080, type=int)

    worker_parser = subparsers.add_parser("worker", help="Run a specific worker")
    worker_parser.add_argument("name", choices=["collector", "intelligence", "delivery"])
    worker_parser.add_argument("--once", action="store_true", help="Run a single batch")
    worker_parser.add_argument("--max-items", type=int, default=None, help="Max messages for intelligence/delivery")

    subparsers.add_parser("health", help="Print current health snapshot")
    subparsers.add_parser("print-config", help="Print effective configuration")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    monitor = build_monitor_service(config)

    if args.command == "run":
        if args.once:
            stats = monitor.run_once()
            print(json.dumps(asdict(stats), indent=2))
            return 0
        monitor.start_forever()
        try:
            while True:
                monitor._loop_thread.join(timeout=3600)  # noqa: SLF001
        except KeyboardInterrupt:
            monitor.stop()
            return 0

    if args.command == "serve":
        from ai_innovation_monitoring.api import build_http_server

        monitor.start_forever()
        server = build_http_server(args.host, args.port, monitor)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            monitor.stop()
            server.server_close()
        return 0

    if args.command == "worker":
        runner = {
            "collector": lambda: monitor.run_collector_once(),
            "intelligence": lambda: monitor.run_intelligence_once(args.max_items),
            "delivery": lambda: monitor.run_delivery_once(args.max_items),
        }[args.name]
        if args.once:
            print(json.dumps(asdict(runner()), indent=2))
            return 0
        try:
            while True:
                print(json.dumps(asdict(runner()), indent=2))
                time.sleep(config.runner.poll_interval_seconds)
        except KeyboardInterrupt:
            return 0

    if args.command == "health":
        print(json.dumps(monitor.health(), indent=2))
        return 0

    if args.command == "print-config":
        print(json.dumps(config_to_dict(config), indent=2, default=str))
        return 0

    parser.error("Unsupported command")
    return 2
