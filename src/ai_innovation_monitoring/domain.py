from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from hashlib import sha1
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def stable_hash(*parts: str) -> str:
    payload = "||".join(part.strip().lower() for part in parts if part)
    return sha1(payload.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class SourceDocument:
    source_name: str
    url: str
    title: str
    content: str = ""
    published_at: datetime | None = None
    fetched_at: datetime = field(default_factory=utcnow)
    host: str = ""
    tags: list[str] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    fingerprint: str = ""
    document_id: str = ""

    def __post_init__(self) -> None:
        self.fingerprint = self.fingerprint or stable_hash(self.title, self.url)
        self.document_id = self.document_id or stable_hash(self.source_name, self.url)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["published_at"] = isoformat(self.published_at)
        payload["fetched_at"] = isoformat(self.fetched_at)
        return payload


@dataclass(slots=True)
class InnovationEvent:
    event_id: str
    document_id: str
    canonical_id: str
    company: str
    category: str
    summary: str
    novelty_score: float
    market_impact_score: float
    confidence: float
    tickers: list[str] = field(default_factory=list)
    rationale: str = ""
    theme_matches: list[str] = field(default_factory=list)
    llm_used: bool = False
    official_source: bool = False
    requires_human_review: bool = False
    created_at: datetime = field(default_factory=utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = isoformat(self.created_at)
        return payload


@dataclass(slots=True)
class CompanyProfile:
    company_id: str
    name: str
    company_type: str = "private"
    size_score: float = 0.5
    aliases: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    listed_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ImpactRule:
    rule_id: str
    source_company: str
    target_symbol: str
    target_asset_type: str = "equity"
    relation: str = "competitor"
    direction: str = "negative"
    base_weight: float = 1.0
    themes: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ImpactAssessment:
    assessment_id: str
    event_id: str
    source_company: str
    target_symbol: str
    target_asset_type: str
    relation: str
    direction: str
    impact_score: float
    confidence: float
    issuer_size_score: float
    innovation_strength_score: float
    horizon: str = "near_term"
    rationale: str = ""
    created_at: datetime = field(default_factory=utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = isoformat(self.created_at)
        return payload


@dataclass(slots=True)
class OrderIntent:
    intent_id: str
    event_id: str
    ticker: str
    side: str
    notional_usd: float
    confidence: float
    reason: str
    idempotency_key: str
    asset_type: str = "equity"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class OrderResult:
    intent_id: str
    delivery_name: str
    delivery_order_id: str
    status: str
    submitted_at: datetime = field(default_factory=utcnow)
    raw_response: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["submitted_at"] = isoformat(self.submitted_at)
        return payload


@dataclass(slots=True)
class SourceState:
    source_name: str
    etag: str | None = None
    last_modified: str | None = None
    cursor: str | None = None
    next_poll_at: datetime | None = None
    failure_count: int = 0
    backoff_until: datetime | None = None
    last_success_at: datetime | None = None


@dataclass(slots=True)
class LLMUsageRecord:
    usage_id: str
    created_at: datetime
    model: str
    cost_usd: float
    tokens_in: int
    tokens_out: int
