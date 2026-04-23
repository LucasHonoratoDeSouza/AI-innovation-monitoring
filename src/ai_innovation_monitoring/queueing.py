from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ai_innovation_monitoring.config import QueueConfig
from ai_innovation_monitoring.domain import isoformat, parse_datetime, stable_hash, utcnow


@dataclass(slots=True)
class QueueMessage:
    message_id: str
    queue_name: str
    payload: dict[str, Any]


class QueueBackend(Protocol):
    def publish(self, queue_name: str, payload: dict[str, Any]) -> str: ...
    def consume(self, queue_name: str) -> QueueMessage | None: ...
    def ack(self, message: QueueMessage) -> None: ...
    def size(self, queue_name: str) -> int: ...
    def stats(self) -> dict[str, Any]: ...


SQLITE_QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_messages (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    claimed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_messages_pending ON queue_messages(queue_name, status, created_at);
"""


class SQLiteQueue:
    def __init__(self, path: Path, reclaim_timeout_seconds: int = 300) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.reclaim_timeout_seconds = reclaim_timeout_seconds
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        with self._connection:
            self._connection.executescript(SQLITE_QUEUE_SCHEMA)

    def publish(self, queue_name: str, payload: dict[str, Any]) -> str:
        message_id = stable_hash(queue_name, isoformat(utcnow()) or "", json.dumps(payload, sort_keys=True))
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO queue_messages (id, queue_name, payload_json, status, created_at, claimed_at)
                VALUES (?, ?, ?, 'pending', ?, NULL)
                """,
                (
                    message_id,
                    queue_name,
                    json.dumps(payload),
                    isoformat(utcnow()),
                ),
            )
        return message_id

    def consume(self, queue_name: str) -> QueueMessage | None:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            rows = self._connection.execute(
                "SELECT * FROM queue_messages WHERE queue_name = ? ORDER BY created_at ASC",
                (queue_name,),
            ).fetchall()
            row = None
            now = utcnow()
            for candidate in rows:
                if candidate["status"] == "pending":
                    row = candidate
                    break
                if candidate["status"] == "claimed":
                    claimed_at = parse_datetime(candidate["claimed_at"])
                    if claimed_at is None or (now - claimed_at).total_seconds() >= self.reclaim_timeout_seconds:
                        row = candidate
                        break
            if row is None:
                self._connection.commit()
                return None
            claimed_at = isoformat(now)
            self._connection.execute(
                "UPDATE queue_messages SET status = 'claimed', claimed_at = ? WHERE id = ?",
                (claimed_at, row["id"]),
            )
            self._connection.commit()
        return QueueMessage(
            message_id=row["id"],
            queue_name=row["queue_name"],
            payload=json.loads(row["payload_json"]),
        )

    def ack(self, message: QueueMessage) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM queue_messages WHERE id = ?", (message.message_id,))

    def size(self, queue_name: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM queue_messages WHERE queue_name = ?",
                (queue_name,),
            ).fetchone()
        return int(row["count"])

    def stats(self) -> dict[str, Any]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT queue_name, COUNT(*) AS count FROM queue_messages GROUP BY queue_name ORDER BY queue_name"
            ).fetchall()
        return {"kind": "sqlite", "path": str(self.path), "queues": {row["queue_name"]: int(row["count"]) for row in rows}}


class RedisQueue:
    def __init__(self, redis_url: str) -> None:
        try:
            import redis
        except ImportError as error:  # pragma: no cover - depends on env
            raise RuntimeError("redis package is required for Redis queue support.") from error
        self._redis = redis.Redis.from_url(redis_url)

    def publish(self, queue_name: str, payload: dict[str, Any]) -> str:
        message_id = stable_hash(queue_name, isoformat(utcnow()) or "", json.dumps(payload, sort_keys=True))
        envelope = {"id": message_id, "payload": payload}
        self._redis.rpush(queue_name, json.dumps(envelope))
        return message_id

    def consume(self, queue_name: str) -> QueueMessage | None:
        result = self._redis.blpop(queue_name, timeout=1)
        if result is None:
            return None
        _, raw = result
        envelope = json.loads(raw)
        return QueueMessage(
            message_id=envelope["id"],
            queue_name=queue_name,
            payload=dict(envelope["payload"]),
        )

    def ack(self, message: QueueMessage) -> None:
        return

    def size(self, queue_name: str) -> int:
        return int(self._redis.llen(queue_name))

    def stats(self) -> dict[str, Any]:
        return {"kind": "redis"}


def build_queue(config: QueueConfig) -> QueueBackend:
    if config.kind == "sqlite":
        return SQLiteQueue(config.sqlite_path, reclaim_timeout_seconds=config.reclaim_timeout_seconds)
    if config.kind == "redis":
        import os

        redis_url = os.getenv(config.redis_url_env_var, "")
        if not redis_url:
            raise RuntimeError(f"Environment variable {config.redis_url_env_var} is required for Redis queue.")
        return RedisQueue(redis_url)
    raise ValueError(f"Unsupported queue kind: {config.kind}")
