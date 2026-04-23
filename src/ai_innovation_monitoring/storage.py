from __future__ import annotations

import json
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

from ai_innovation_monitoring.config import StorageConfig
from ai_innovation_monitoring.domain import (
    CompanyProfile,
    ImpactAssessment,
    ImpactRule,
    InnovationEvent,
    LLMUsageRecord,
    OrderIntent,
    OrderResult,
    SourceDocument,
    SourceState,
    isoformat,
    parse_datetime,
    stable_hash,
    utcnow,
)


def _json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    return json.loads(value)


def _document_from_row(row: Any) -> SourceDocument:
    return SourceDocument(
        source_name=row["source_name"],
        url=row["url"],
        title=row["title"],
        content=row["content"],
        published_at=parse_datetime(row["published_at"]),
        fetched_at=parse_datetime(row["fetched_at"]) or utcnow(),
        host=row["host"],
        tags=_json_loads(row["tags_json"], []),
        raw_payload=_json_loads(row["raw_payload_json"], {}),
        fingerprint=row["fingerprint"],
        document_id=row["id"],
    )


def _company_profile_from_row(row: Any) -> CompanyProfile:
    return CompanyProfile(
        company_id=row["id"],
        name=row["name"],
        company_type=row["company_type"],
        size_score=float(row["size_score"]),
        aliases=_json_loads(row["aliases_json"], []),
        domains=_json_loads(row["domains_json"], []),
        listed_symbols=_json_loads(row["listed_symbols_json"], []),
        metadata=_json_loads(row["metadata_json"], {}),
    )


def _impact_rule_from_row(row: Any) -> ImpactRule:
    return ImpactRule(
        rule_id=row["id"],
        source_company=row["source_company"],
        target_symbol=row["target_symbol"],
        target_asset_type=row["target_asset_type"],
        relation=row["relation"],
        direction=row["direction"],
        base_weight=float(row["base_weight"]),
        themes=_json_loads(row["themes_json"], []),
        categories=_json_loads(row["categories_json"], []),
        metadata=_json_loads(row["metadata_json"], {}),
    )


def _event_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": row["id"],
        "document_id": row["document_id"],
        "canonical_id": row["canonical_id"],
        "company": row["company"],
        "category": row["category"],
        "summary": row["summary"],
        "novelty_score": float(row["novelty_score"]),
        "market_impact_score": float(row["market_impact_score"]),
        "confidence": float(row["confidence"]),
        "tickers": _json_loads(row["tickers_json"], []),
        "rationale": row["rationale"],
        "theme_matches": _json_loads(row["theme_matches_json"], []),
        "llm_used": bool(row["llm_used"]),
        "official_source": bool(row["official_source"]),
        "requires_human_review": bool(row["requires_human_review"]),
        "created_at": row["created_at"],
        "metadata": _json_loads(row["metadata_json"], {}),
    }


def _order_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": row["id"],
        "event_id": row["event_id"],
        "ticker": row["ticker"],
        "asset_type": row["asset_type"],
        "side": row["side"],
        "notional_usd": float(row["notional_usd"]),
        "confidence": float(row["confidence"]),
        "reason": row["reason"],
        "idempotency_key": row["idempotency_key"],
        "status": row["status"],
        "delivery_name": row["delivery_name"],
        "delivery_order_id": row["delivery_order_id"],
        "submitted_at": row["submitted_at"],
        "metadata": _json_loads(row["metadata_json"], {}),
        "raw_response": _json_loads(row["raw_response_json"], {}),
    }


def _impact_assessment_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": row["id"],
        "event_id": row["event_id"],
        "source_company": row["source_company"],
        "target_symbol": row["target_symbol"],
        "target_asset_type": row["target_asset_type"],
        "relation": row["relation"],
        "direction": row["direction"],
        "impact_score": float(row["impact_score"]),
        "confidence": float(row["confidence"]),
        "issuer_size_score": float(row["issuer_size_score"]),
        "innovation_strength_score": float(row["innovation_strength_score"]),
        "horizon": row["horizon"],
        "rationale": row["rationale"],
        "created_at": row["created_at"],
        "metadata": _json_loads(row["metadata_json"], {}),
    }


class Store(Protocol):
    def save_document(self, document: SourceDocument) -> bool: ...
    def get_document(self, document_id: str) -> SourceDocument | None: ...
    def save_event(self, event: InnovationEvent) -> None: ...
    def save_impact_assessment(self, assessment: ImpactAssessment) -> None: ...
    def save_order_pending(self, intent: OrderIntent, status: str = "pending") -> None: ...
    def save_order_result(self, intent: OrderIntent, result: OrderResult) -> None: ...
    def get_source_state(self, source_name: str) -> SourceState | None: ...
    def upsert_source_state(self, state: SourceState) -> None: ...
    def recent_orders_for_ticker(self, ticker: str, minutes: int) -> list[Any]: ...
    def daily_order_count(self) -> int: ...
    def total_live_exposure(self) -> float: ...
    def exposure_for_ticker(self, ticker: str) -> float: ...
    def list_recent_events(self, limit: int = 20) -> list[dict]: ...
    def list_recent_orders(self, limit: int = 20) -> list[dict]: ...
    def list_recent_impact_assessments(self, limit: int = 20) -> list[dict]: ...
    def record_llm_usage(self, model: str, cost_usd: float, tokens_in: int, tokens_out: int) -> None: ...
    def llm_spend_last_24h(self) -> float: ...
    def upsert_company_profile(self, profile: CompanyProfile) -> None: ...
    def get_company_profile(self, name: str) -> CompanyProfile | None: ...
    def list_company_profiles(self, limit: int = 100) -> list[dict]: ...
    def upsert_impact_rule(self, rule: ImpactRule) -> None: ...
    def list_impact_rules(self, source_company: str | None = None) -> list[ImpactRule]: ...
    def health_snapshot(self) -> dict[str, float | int | str]: ...


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    host TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    company TEXT NOT NULL,
    category TEXT NOT NULL,
    summary TEXT NOT NULL,
    novelty_score REAL NOT NULL,
    market_impact_score REAL NOT NULL,
    confidence REAL NOT NULL,
    tickers_json TEXT NOT NULL,
    rationale TEXT NOT NULL,
    theme_matches_json TEXT NOT NULL,
    llm_used INTEGER NOT NULL,
    official_source INTEGER NOT NULL,
    requires_human_review INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_document_id ON events(document_id);
CREATE INDEX IF NOT EXISTS idx_events_canonical_id ON events(canonical_id);

CREATE TABLE IF NOT EXISTS impact_assessments (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    source_company TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    target_asset_type TEXT NOT NULL,
    relation TEXT NOT NULL,
    direction TEXT NOT NULL,
    impact_score REAL NOT NULL,
    confidence REAL NOT NULL,
    issuer_size_score REAL NOT NULL,
    innovation_strength_score REAL NOT NULL,
    horizon TEXT NOT NULL,
    rationale TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impact_assessments_event_id ON impact_assessments(event_id);
CREATE INDEX IF NOT EXISTS idx_impact_assessments_symbol ON impact_assessments(target_symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    side TEXT NOT NULL,
    notional_usd REAL NOT NULL,
    confidence REAL NOT NULL,
    reason TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    delivery_name TEXT,
    delivery_order_id TEXT,
    submitted_at TEXT,
    metadata_json TEXT NOT NULL,
    raw_response_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_ticker_submitted_at ON orders(ticker, submitted_at DESC);

CREATE TABLE IF NOT EXISTS source_state (
    source_name TEXT PRIMARY KEY,
    etag TEXT,
    last_modified TEXT,
    cursor TEXT,
    next_poll_at TEXT,
    failure_count INTEGER NOT NULL,
    backoff_until TEXT,
    last_success_at TEXT
);

CREATE TABLE IF NOT EXISTS llm_usage (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    model TEXT NOT NULL,
    cost_usd REAL NOT NULL,
    tokens_in INTEGER NOT NULL,
    tokens_out INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS company_registry (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    company_type TEXT NOT NULL,
    size_score REAL NOT NULL,
    aliases_json TEXT NOT NULL,
    domains_json TEXT NOT NULL,
    listed_symbols_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impact_rules (
    id TEXT PRIMARY KEY,
    source_company TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    target_asset_type TEXT NOT NULL,
    relation TEXT NOT NULL,
    direction TEXT NOT NULL,
    base_weight REAL NOT NULL,
    themes_json TEXT NOT NULL,
    categories_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impact_rules_source_company ON impact_rules(source_company);
"""


POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    host TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    company TEXT NOT NULL,
    category TEXT NOT NULL,
    summary TEXT NOT NULL,
    novelty_score DOUBLE PRECISION NOT NULL,
    market_impact_score DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    tickers_json TEXT NOT NULL,
    rationale TEXT NOT NULL,
    theme_matches_json TEXT NOT NULL,
    llm_used INTEGER NOT NULL,
    official_source INTEGER NOT NULL,
    requires_human_review INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_document_id ON events(document_id);
CREATE INDEX IF NOT EXISTS idx_events_canonical_id ON events(canonical_id);

CREATE TABLE IF NOT EXISTS impact_assessments (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    source_company TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    target_asset_type TEXT NOT NULL,
    relation TEXT NOT NULL,
    direction TEXT NOT NULL,
    impact_score DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    issuer_size_score DOUBLE PRECISION NOT NULL,
    innovation_strength_score DOUBLE PRECISION NOT NULL,
    horizon TEXT NOT NULL,
    rationale TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impact_assessments_event_id ON impact_assessments(event_id);
CREATE INDEX IF NOT EXISTS idx_impact_assessments_symbol ON impact_assessments(target_symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    side TEXT NOT NULL,
    notional_usd DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    delivery_name TEXT,
    delivery_order_id TEXT,
    submitted_at TEXT,
    metadata_json TEXT NOT NULL,
    raw_response_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_ticker_submitted_at ON orders(ticker, submitted_at DESC);

CREATE TABLE IF NOT EXISTS source_state (
    source_name TEXT PRIMARY KEY,
    etag TEXT,
    last_modified TEXT,
    cursor TEXT,
    next_poll_at TEXT,
    failure_count INTEGER NOT NULL,
    backoff_until TEXT,
    last_success_at TEXT
);

CREATE TABLE IF NOT EXISTS llm_usage (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    model TEXT NOT NULL,
    cost_usd DOUBLE PRECISION NOT NULL,
    tokens_in INTEGER NOT NULL,
    tokens_out INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS company_registry (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    company_type TEXT NOT NULL,
    size_score DOUBLE PRECISION NOT NULL,
    aliases_json TEXT NOT NULL,
    domains_json TEXT NOT NULL,
    listed_symbols_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impact_rules (
    id TEXT PRIMARY KEY,
    source_company TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    target_asset_type TEXT NOT NULL,
    relation TEXT NOT NULL,
    direction TEXT NOT NULL,
    base_weight DOUBLE PRECISION NOT NULL,
    themes_json TEXT NOT NULL,
    categories_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impact_rules_source_company ON impact_rules(source_company);
"""


class SQLiteStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    def _initialize(self) -> None:
        with self._connection:
            self._connection.executescript(SQLITE_SCHEMA)

    def save_document(self, document: SourceDocument) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO documents (
                    id, source_name, url, title, content, published_at, fetched_at,
                    host, tags_json, fingerprint, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document.document_id,
                    document.source_name,
                    document.url,
                    document.title,
                    document.content,
                    isoformat(document.published_at),
                    isoformat(document.fetched_at),
                    document.host,
                    json.dumps(document.tags),
                    document.fingerprint,
                    json.dumps(document.raw_payload),
                ),
            )
            return cursor.rowcount == 1

    def get_document(self, document_id: str) -> SourceDocument | None:
        with self._lock:
            row = self._connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
        if row is None:
            return None
        return _document_from_row(row)

    def save_event(self, event: InnovationEvent) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO events (
                    id, document_id, canonical_id, company, category, summary,
                    novelty_score, market_impact_score, confidence, tickers_json,
                    rationale, theme_matches_json, llm_used, official_source,
                    requires_human_review, created_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.document_id,
                    event.canonical_id,
                    event.company,
                    event.category,
                    event.summary,
                    event.novelty_score,
                    event.market_impact_score,
                    event.confidence,
                    json.dumps(event.tickers),
                    event.rationale,
                    json.dumps(event.theme_matches),
                    1 if event.llm_used else 0,
                    1 if event.official_source else 0,
                    1 if event.requires_human_review else 0,
                    isoformat(event.created_at),
                    json.dumps(event.metadata),
                ),
            )

    def save_impact_assessment(self, assessment: ImpactAssessment) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO impact_assessments (
                    id, event_id, source_company, target_symbol, target_asset_type,
                    relation, direction, impact_score, confidence, issuer_size_score,
                    innovation_strength_score, horizon, rationale, created_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    assessment.assessment_id,
                    assessment.event_id,
                    assessment.source_company,
                    assessment.target_symbol,
                    assessment.target_asset_type,
                    assessment.relation,
                    assessment.direction,
                    assessment.impact_score,
                    assessment.confidence,
                    assessment.issuer_size_score,
                    assessment.innovation_strength_score,
                    assessment.horizon,
                    assessment.rationale,
                    isoformat(assessment.created_at),
                    json.dumps(assessment.metadata),
                ),
            )

    def save_order_pending(self, intent: OrderIntent, status: str = "pending") -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO orders (
                    id, event_id, ticker, asset_type, side, notional_usd, confidence, reason,
                    idempotency_key, status, delivery_name, delivery_order_id,
                    submitted_at, metadata_json, raw_response_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.intent_id,
                    intent.event_id,
                    intent.ticker,
                    intent.asset_type,
                    intent.side,
                    intent.notional_usd,
                    intent.confidence,
                    intent.reason,
                    intent.idempotency_key,
                    status,
                    None,
                    None,
                    None,
                    json.dumps(intent.metadata),
                    json.dumps({}),
                ),
            )

    def save_order_result(self, intent: OrderIntent, result: OrderResult) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                UPDATE orders
                SET status = ?, delivery_name = ?, delivery_order_id = ?, submitted_at = ?, raw_response_json = ?
                WHERE id = ?
                """,
                (
                    result.status,
                    result.delivery_name,
                    result.delivery_order_id,
                    isoformat(result.submitted_at),
                    json.dumps(result.raw_response),
                    intent.intent_id,
                ),
            )

    def get_source_state(self, source_name: str) -> SourceState | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM source_state WHERE source_name = ?",
                (source_name,),
            ).fetchone()
        if row is None:
            return None
        return SourceState(
            source_name=row["source_name"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            cursor=row["cursor"],
            next_poll_at=parse_datetime(row["next_poll_at"]),
            failure_count=int(row["failure_count"]),
            backoff_until=parse_datetime(row["backoff_until"]),
            last_success_at=parse_datetime(row["last_success_at"]),
        )

    def upsert_source_state(self, state: SourceState) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO source_state (
                    source_name, etag, last_modified, cursor, next_poll_at,
                    failure_count, backoff_until, last_success_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    etag = excluded.etag,
                    last_modified = excluded.last_modified,
                    cursor = excluded.cursor,
                    next_poll_at = excluded.next_poll_at,
                    failure_count = excluded.failure_count,
                    backoff_until = excluded.backoff_until,
                    last_success_at = excluded.last_success_at
                """,
                (
                    state.source_name,
                    state.etag,
                    state.last_modified,
                    state.cursor,
                    isoformat(state.next_poll_at),
                    state.failure_count,
                    isoformat(state.backoff_until),
                    isoformat(state.last_success_at),
                ),
            )

    def recent_orders_for_ticker(self, ticker: str, minutes: int) -> list[sqlite3.Row]:
        since = isoformat(utcnow() - timedelta(minutes=minutes))
        with self._lock:
            return list(
                self._connection.execute(
                    """
                    SELECT * FROM orders
                    WHERE ticker = ? AND submitted_at IS NOT NULL AND submitted_at >= ?
                    ORDER BY submitted_at DESC
                    """,
                    (ticker, since),
                ).fetchall()
            )

    def daily_order_count(self) -> int:
        since = isoformat(utcnow() - timedelta(hours=24))
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM orders WHERE submitted_at IS NOT NULL AND submitted_at >= ?",
                (since,),
            ).fetchone()
        return int(row["count"])

    def total_live_exposure(self) -> float:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COALESCE(SUM(ABS(notional_usd)), 0) AS exposure
                FROM orders
                WHERE status IN ('submitted', 'accepted', 'filled', 'simulated', 'emitted')
                """
            ).fetchone()
        return float(row["exposure"])

    def exposure_for_ticker(self, ticker: str) -> float:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COALESCE(SUM(ABS(notional_usd)), 0) AS exposure
                FROM orders
                WHERE ticker = ? AND status IN ('submitted', 'accepted', 'filled', 'simulated', 'emitted')
                """,
                (ticker,),
            ).fetchone()
        return float(row["exposure"])

    def list_recent_events(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._connection.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [_event_row_to_dict(row) for row in rows]

    def list_recent_orders(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM orders ORDER BY COALESCE(submitted_at, id) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_order_row_to_dict(row) for row in rows]

    def list_recent_impact_assessments(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM impact_assessments ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_impact_assessment_row_to_dict(row) for row in rows]

    def record_llm_usage(self, model: str, cost_usd: float, tokens_in: int, tokens_out: int) -> None:
        record = LLMUsageRecord(
            usage_id=stable_hash(model, isoformat(utcnow()) or ""),
            created_at=utcnow(),
            model=model,
            cost_usd=cost_usd,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
        )
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO llm_usage (id, created_at, model, cost_usd, tokens_in, tokens_out)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.usage_id,
                    isoformat(record.created_at),
                    record.model,
                    record.cost_usd,
                    record.tokens_in,
                    record.tokens_out,
                ),
            )

    def llm_spend_last_24h(self) -> float:
        since = isoformat(utcnow() - timedelta(hours=24))
        with self._lock:
            row = self._connection.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) AS spend FROM llm_usage WHERE created_at >= ?",
                (since,),
            ).fetchone()
        return float(row["spend"])

    def upsert_company_profile(self, profile: CompanyProfile) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO company_registry (
                    id, name, company_type, size_score, aliases_json,
                    domains_json, listed_symbols_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    company_type = excluded.company_type,
                    size_score = excluded.size_score,
                    aliases_json = excluded.aliases_json,
                    domains_json = excluded.domains_json,
                    listed_symbols_json = excluded.listed_symbols_json,
                    metadata_json = excluded.metadata_json
                """,
                (
                    profile.company_id,
                    profile.name,
                    profile.company_type,
                    profile.size_score,
                    json.dumps(profile.aliases),
                    json.dumps(profile.domains),
                    json.dumps(profile.listed_symbols),
                    json.dumps(profile.metadata),
                ),
            )

    def get_company_profile(self, name: str) -> CompanyProfile | None:
        normalized = name.strip().lower()
        with self._lock:
            rows = self._connection.execute("SELECT * FROM company_registry").fetchall()
        for row in rows:
            aliases = [alias.lower() for alias in _json_loads(row["aliases_json"], [])]
            if row["name"].lower() == normalized or normalized in aliases:
                return _company_profile_from_row(row)
        return None

    def list_company_profiles(self, limit: int = 100) -> list[dict]:
        with self._lock:
            rows = self._connection.execute("SELECT * FROM company_registry ORDER BY name ASC LIMIT ?", (limit,)).fetchall()
        return [_company_profile_from_row(row).to_dict() for row in rows]

    def upsert_impact_rule(self, rule: ImpactRule) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO impact_rules (
                    id, source_company, target_symbol, target_asset_type,
                    relation, direction, base_weight, themes_json,
                    categories_json, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    source_company = excluded.source_company,
                    target_symbol = excluded.target_symbol,
                    target_asset_type = excluded.target_asset_type,
                    relation = excluded.relation,
                    direction = excluded.direction,
                    base_weight = excluded.base_weight,
                    themes_json = excluded.themes_json,
                    categories_json = excluded.categories_json,
                    metadata_json = excluded.metadata_json
                """,
                (
                    rule.rule_id,
                    rule.source_company,
                    rule.target_symbol,
                    rule.target_asset_type,
                    rule.relation,
                    rule.direction,
                    rule.base_weight,
                    json.dumps(rule.themes),
                    json.dumps(rule.categories),
                    json.dumps(rule.metadata),
                ),
            )

    def list_impact_rules(self, source_company: str | None = None) -> list[ImpactRule]:
        with self._lock:
            if source_company is None:
                rows = self._connection.execute("SELECT * FROM impact_rules ORDER BY source_company, target_symbol").fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM impact_rules WHERE source_company IN (?, '*') ORDER BY target_symbol",
                    (source_company,),
                ).fetchall()
        return [_impact_rule_from_row(row) for row in rows]

    def health_snapshot(self) -> dict[str, float | int | str]:
        with self._lock:
            documents = self._connection.execute("SELECT COUNT(*) AS count FROM documents").fetchone()["count"]
            events = self._connection.execute("SELECT COUNT(*) AS count FROM events").fetchone()["count"]
            orders = self._connection.execute("SELECT COUNT(*) AS count FROM orders").fetchone()["count"]
            companies = self._connection.execute("SELECT COUNT(*) AS count FROM company_registry").fetchone()["count"]
            impact_rules = self._connection.execute("SELECT COUNT(*) AS count FROM impact_rules").fetchone()["count"]
            impact_assessments = self._connection.execute("SELECT COUNT(*) AS count FROM impact_assessments").fetchone()["count"]
        return {
            "db_path": str(self.path),
            "documents": int(documents),
            "events": int(events),
            "orders": int(orders),
            "company_profiles": int(companies),
            "impact_rules": int(impact_rules),
            "impact_assessments": int(impact_assessments),
            "llm_spend_last_24h": self.llm_spend_last_24h(),
        }


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as error:  # pragma: no cover - depends on env
            raise RuntimeError("psycopg is required for Postgres store support.") from error

        self._psycopg = psycopg
        self._lock = threading.Lock()
        self._connection = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
        self._initialize()

    def _initialize(self) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(POSTGRES_SCHEMA)

    def save_document(self, document: SourceDocument) -> bool:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO documents (
                    id, source_name, url, title, content, published_at, fetched_at,
                    host, tags_json, fingerprint, raw_payload_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(url) DO NOTHING
                RETURNING id
                """,
                (
                    document.document_id,
                    document.source_name,
                    document.url,
                    document.title,
                    document.content,
                    isoformat(document.published_at),
                    isoformat(document.fetched_at),
                    document.host,
                    json.dumps(document.tags),
                    document.fingerprint,
                    json.dumps(document.raw_payload),
                ),
            )
            return cursor.fetchone() is not None

    def get_document(self, document_id: str) -> SourceDocument | None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM documents WHERE id = %s", (document_id,))
            row = cursor.fetchone()
        if row is None:
            return None
        return _document_from_row(row)

    def save_event(self, event: InnovationEvent) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO events (
                    id, document_id, canonical_id, company, category, summary,
                    novelty_score, market_impact_score, confidence, tickers_json,
                    rationale, theme_matches_json, llm_used, official_source,
                    requires_human_review, created_at, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    canonical_id = EXCLUDED.canonical_id,
                    company = EXCLUDED.company,
                    category = EXCLUDED.category,
                    summary = EXCLUDED.summary,
                    novelty_score = EXCLUDED.novelty_score,
                    market_impact_score = EXCLUDED.market_impact_score,
                    confidence = EXCLUDED.confidence,
                    tickers_json = EXCLUDED.tickers_json,
                    rationale = EXCLUDED.rationale,
                    theme_matches_json = EXCLUDED.theme_matches_json,
                    llm_used = EXCLUDED.llm_used,
                    official_source = EXCLUDED.official_source,
                    requires_human_review = EXCLUDED.requires_human_review,
                    created_at = EXCLUDED.created_at,
                    metadata_json = EXCLUDED.metadata_json
                """,
                (
                    event.event_id,
                    event.document_id,
                    event.canonical_id,
                    event.company,
                    event.category,
                    event.summary,
                    event.novelty_score,
                    event.market_impact_score,
                    event.confidence,
                    json.dumps(event.tickers),
                    event.rationale,
                    json.dumps(event.theme_matches),
                    1 if event.llm_used else 0,
                    1 if event.official_source else 0,
                    1 if event.requires_human_review else 0,
                    isoformat(event.created_at),
                    json.dumps(event.metadata),
                ),
            )

    def save_impact_assessment(self, assessment: ImpactAssessment) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO impact_assessments (
                    id, event_id, source_company, target_symbol, target_asset_type,
                    relation, direction, impact_score, confidence, issuer_size_score,
                    innovation_strength_score, horizon, rationale, created_at, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    source_company = EXCLUDED.source_company,
                    target_symbol = EXCLUDED.target_symbol,
                    target_asset_type = EXCLUDED.target_asset_type,
                    relation = EXCLUDED.relation,
                    direction = EXCLUDED.direction,
                    impact_score = EXCLUDED.impact_score,
                    confidence = EXCLUDED.confidence,
                    issuer_size_score = EXCLUDED.issuer_size_score,
                    innovation_strength_score = EXCLUDED.innovation_strength_score,
                    horizon = EXCLUDED.horizon,
                    rationale = EXCLUDED.rationale,
                    created_at = EXCLUDED.created_at,
                    metadata_json = EXCLUDED.metadata_json
                """,
                (
                    assessment.assessment_id,
                    assessment.event_id,
                    assessment.source_company,
                    assessment.target_symbol,
                    assessment.target_asset_type,
                    assessment.relation,
                    assessment.direction,
                    assessment.impact_score,
                    assessment.confidence,
                    assessment.issuer_size_score,
                    assessment.innovation_strength_score,
                    assessment.horizon,
                    assessment.rationale,
                    isoformat(assessment.created_at),
                    json.dumps(assessment.metadata),
                ),
            )

    def save_order_pending(self, intent: OrderIntent, status: str = "pending") -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO orders (
                    id, event_id, ticker, asset_type, side, notional_usd, confidence, reason,
                    idempotency_key, status, delivery_name, delivery_order_id,
                    submitted_at, metadata_json, raw_response_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    intent.intent_id,
                    intent.event_id,
                    intent.ticker,
                    intent.asset_type,
                    intent.side,
                    intent.notional_usd,
                    intent.confidence,
                    intent.reason,
                    intent.idempotency_key,
                    status,
                    None,
                    None,
                    None,
                    json.dumps(intent.metadata),
                    json.dumps({}),
                ),
            )

    def save_order_result(self, intent: OrderIntent, result: OrderResult) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE orders
                SET status = %s, delivery_name = %s, delivery_order_id = %s, submitted_at = %s, raw_response_json = %s
                WHERE id = %s
                """,
                (
                    result.status,
                    result.delivery_name,
                    result.delivery_order_id,
                    isoformat(result.submitted_at),
                    json.dumps(result.raw_response),
                    intent.intent_id,
                ),
            )

    def get_source_state(self, source_name: str) -> SourceState | None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM source_state WHERE source_name = %s", (source_name,))
            row = cursor.fetchone()
        if row is None:
            return None
        return SourceState(
            source_name=row["source_name"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            cursor=row["cursor"],
            next_poll_at=parse_datetime(row["next_poll_at"]),
            failure_count=int(row["failure_count"]),
            backoff_until=parse_datetime(row["backoff_until"]),
            last_success_at=parse_datetime(row["last_success_at"]),
        )

    def upsert_source_state(self, state: SourceState) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_state (
                    source_name, etag, last_modified, cursor, next_poll_at,
                    failure_count, backoff_until, last_success_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(source_name) DO UPDATE SET
                    etag = EXCLUDED.etag,
                    last_modified = EXCLUDED.last_modified,
                    cursor = EXCLUDED.cursor,
                    next_poll_at = EXCLUDED.next_poll_at,
                    failure_count = EXCLUDED.failure_count,
                    backoff_until = EXCLUDED.backoff_until,
                    last_success_at = EXCLUDED.last_success_at
                """,
                (
                    state.source_name,
                    state.etag,
                    state.last_modified,
                    state.cursor,
                    isoformat(state.next_poll_at),
                    state.failure_count,
                    isoformat(state.backoff_until),
                    isoformat(state.last_success_at),
                ),
            )

    def recent_orders_for_ticker(self, ticker: str, minutes: int) -> list[Any]:
        since = isoformat(utcnow() - timedelta(minutes=minutes))
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM orders
                WHERE ticker = %s AND submitted_at IS NOT NULL AND submitted_at >= %s
                ORDER BY submitted_at DESC
                """,
                (ticker, since),
            )
            return list(cursor.fetchall())

    def daily_order_count(self) -> int:
        since = isoformat(utcnow() - timedelta(hours=24))
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM orders WHERE submitted_at IS NOT NULL AND submitted_at >= %s", (since,))
            row = cursor.fetchone()
        return int(row["count"])

    def total_live_exposure(self) -> float:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(ABS(notional_usd)), 0) AS exposure
                FROM orders
                WHERE status IN ('submitted', 'accepted', 'filled', 'simulated', 'emitted')
                """
            )
            row = cursor.fetchone()
        return float(row["exposure"])

    def exposure_for_ticker(self, ticker: str) -> float:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(SUM(ABS(notional_usd)), 0) AS exposure
                FROM orders
                WHERE ticker = %s AND status IN ('submitted', 'accepted', 'filled', 'simulated', 'emitted')
                """,
                (ticker,),
            )
            row = cursor.fetchone()
        return float(row["exposure"])

    def list_recent_events(self, limit: int = 20) -> list[dict]:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT %s", (limit,))
            return [_event_row_to_dict(row) for row in cursor.fetchall()]

    def list_recent_orders(self, limit: int = 20) -> list[dict]:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM orders ORDER BY COALESCE(submitted_at, id) DESC LIMIT %s", (limit,))
            return [_order_row_to_dict(row) for row in cursor.fetchall()]

    def list_recent_impact_assessments(self, limit: int = 20) -> list[dict]:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM impact_assessments ORDER BY created_at DESC LIMIT %s", (limit,))
            return [_impact_assessment_row_to_dict(row) for row in cursor.fetchall()]

    def record_llm_usage(self, model: str, cost_usd: float, tokens_in: int, tokens_out: int) -> None:
        record = LLMUsageRecord(
            usage_id=stable_hash(model, isoformat(utcnow()) or ""),
            created_at=utcnow(),
            model=model,
            cost_usd=cost_usd,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
        )
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO llm_usage (id, created_at, model, cost_usd, tokens_in, tokens_out)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    record.usage_id,
                    isoformat(record.created_at),
                    record.model,
                    record.cost_usd,
                    record.tokens_in,
                    record.tokens_out,
                ),
            )

    def llm_spend_last_24h(self) -> float:
        since = isoformat(utcnow() - timedelta(hours=24))
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(SUM(cost_usd), 0) AS spend FROM llm_usage WHERE created_at >= %s", (since,))
            row = cursor.fetchone()
        return float(row["spend"])

    def upsert_company_profile(self, profile: CompanyProfile) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO company_registry (
                    id, name, company_type, size_score, aliases_json,
                    domains_json, listed_symbols_json, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    name = EXCLUDED.name,
                    company_type = EXCLUDED.company_type,
                    size_score = EXCLUDED.size_score,
                    aliases_json = EXCLUDED.aliases_json,
                    domains_json = EXCLUDED.domains_json,
                    listed_symbols_json = EXCLUDED.listed_symbols_json,
                    metadata_json = EXCLUDED.metadata_json
                """,
                (
                    profile.company_id,
                    profile.name,
                    profile.company_type,
                    profile.size_score,
                    json.dumps(profile.aliases),
                    json.dumps(profile.domains),
                    json.dumps(profile.listed_symbols),
                    json.dumps(profile.metadata),
                ),
            )

    def get_company_profile(self, name: str) -> CompanyProfile | None:
        normalized = name.strip().lower()
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM company_registry")
            rows = list(cursor.fetchall())
        for row in rows:
            aliases = [alias.lower() for alias in _json_loads(row["aliases_json"], [])]
            if row["name"].lower() == normalized or normalized in aliases:
                return _company_profile_from_row(row)
        return None

    def list_company_profiles(self, limit: int = 100) -> list[dict]:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute("SELECT * FROM company_registry ORDER BY name ASC LIMIT %s", (limit,))
            return [_company_profile_from_row(row).to_dict() for row in cursor.fetchall()]

    def upsert_impact_rule(self, rule: ImpactRule) -> None:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO impact_rules (
                    id, source_company, target_symbol, target_asset_type,
                    relation, direction, base_weight, themes_json,
                    categories_json, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    source_company = EXCLUDED.source_company,
                    target_symbol = EXCLUDED.target_symbol,
                    target_asset_type = EXCLUDED.target_asset_type,
                    relation = EXCLUDED.relation,
                    direction = EXCLUDED.direction,
                    base_weight = EXCLUDED.base_weight,
                    themes_json = EXCLUDED.themes_json,
                    categories_json = EXCLUDED.categories_json,
                    metadata_json = EXCLUDED.metadata_json
                """,
                (
                    rule.rule_id,
                    rule.source_company,
                    rule.target_symbol,
                    rule.target_asset_type,
                    rule.relation,
                    rule.direction,
                    rule.base_weight,
                    json.dumps(rule.themes),
                    json.dumps(rule.categories),
                    json.dumps(rule.metadata),
                ),
            )

    def list_impact_rules(self, source_company: str | None = None) -> list[ImpactRule]:
        with self._lock, self._connection.cursor() as cursor:
            if source_company is None:
                cursor.execute("SELECT * FROM impact_rules ORDER BY source_company, target_symbol")
            else:
                cursor.execute(
                    "SELECT * FROM impact_rules WHERE source_company IN (%s, '*') ORDER BY target_symbol",
                    (source_company,),
                )
            rows = list(cursor.fetchall())
        return [_impact_rule_from_row(row) for row in rows]

    def health_snapshot(self) -> dict[str, float | int | str]:
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM documents) AS documents,
                    (SELECT COUNT(*) FROM events) AS events,
                    (SELECT COUNT(*) FROM orders) AS orders,
                    (SELECT COUNT(*) FROM company_registry) AS company_profiles,
                    (SELECT COUNT(*) FROM impact_rules) AS impact_rules,
                    (SELECT COUNT(*) FROM impact_assessments) AS impact_assessments
                """
            )
            row = cursor.fetchone()
        return {
            "db_path": "postgres",
            "documents": int(row["documents"]),
            "events": int(row["events"]),
            "orders": int(row["orders"]),
            "company_profiles": int(row["company_profiles"]),
            "impact_rules": int(row["impact_rules"]),
            "impact_assessments": int(row["impact_assessments"]),
            "llm_spend_last_24h": self.llm_spend_last_24h(),
        }


def build_store(config: StorageConfig) -> Store:
    if config.kind == "sqlite":
        return SQLiteStore(config.sqlite_path)
    if config.kind == "postgres":
        dsn = __import__("os").getenv(config.postgres_dsn_env_var, "")
        if not dsn:
            raise RuntimeError(f"Environment variable {config.postgres_dsn_env_var} is required for Postgres store.")
        return PostgresStore(dsn)
    raise ValueError(f"Unsupported store kind: {config.kind}")
