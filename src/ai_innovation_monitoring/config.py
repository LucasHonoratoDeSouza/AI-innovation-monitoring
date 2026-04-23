from __future__ import annotations

import json
import os
from dataclasses import asdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ai_innovation_monitoring.domain import CompanyProfile, ImpactRule, stable_hash


DEFAULT_APP_CONFIG_PATH = Path("config/app.local.json")
DEFAULT_MARKET_PROFILE_PATH = Path("config/market_profile.json")
DEFAULT_SOURCES_PATH = Path("config/sources.json")
DEFAULT_COMPANY_REGISTRY_PATH = Path("config/company_registry.json")
DEFAULT_IMPACT_GRAPH_PATH = Path("config/impact_graph.json")
DEFAULT_DB_PATH = Path("data/ai_monitor.db")
DEFAULT_QUEUE_DB_PATH = Path("data/ai_monitor.queue.db")
DEFAULT_LLM_ENDPOINT_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_LLM_API_KEY_ENV = "OPENAI_API_KEY"
DEFAULT_LLM_MODEL = "gpt-5.4-mini"


@dataclass(slots=True)
class RunnerConfig:
    poll_interval_seconds: int = 120
    host_min_interval_seconds: float = 1.5
    http_timeout_seconds: int = 15
    dry_run: bool = True
    ingest_token: str = "change-me"
    queue_drain_batch_size: int = 500


@dataclass(slots=True)
class StorageConfig:
    kind: str = "sqlite"
    sqlite_path: Path = DEFAULT_DB_PATH
    postgres_dsn_env_var: str = "POSTGRES_DSN"


@dataclass(slots=True)
class QueueConfig:
    kind: str = "sqlite"
    sqlite_path: Path = DEFAULT_QUEUE_DB_PATH
    redis_url_env_var: str = "REDIS_URL"
    reclaim_timeout_seconds: int = 300
    block_timeout_seconds: int = 1


@dataclass(slots=True)
class LLMConfig:
    enabled: bool = False
    endpoint_url: str = DEFAULT_LLM_ENDPOINT_URL
    api_key_env_var: str = DEFAULT_LLM_API_KEY_ENV
    model: str = DEFAULT_LLM_MODEL
    temperature: float = 0.0
    max_tokens: int = 700
    daily_budget_usd: float = 10.0
    escalate_uncertainty_threshold: float = 0.42
    escalate_market_impact_threshold: float = 0.65
    escalate_novelty_threshold: float = 0.7
    min_minutes_between_rechecks: int = 180


@dataclass(slots=True)
class DeliveryConfig:
    kind: str = "outbox"
    outbox_path: str = "data/orders_outbox.jsonl"


@dataclass(slots=True)
class RiskConfig:
    max_order_notional_usd: float = 2_000.0
    min_order_notional_usd: float = 200.0
    max_position_per_ticker_usd: float = 10_000.0
    max_total_exposure_usd: float = 25_000.0
    max_orders_per_day: int = 20
    cooldown_minutes_per_ticker: int = 240
    min_confidence_to_trade: float = 0.63
    min_market_impact_to_trade: float = 0.55
    min_crypto_impact_to_trade: float = 0.8
    allow_short: bool = True


@dataclass(slots=True)
class SourceConfig:
    name: str
    kind: str
    url: str
    enabled: bool = True
    tags: list[str] = field(default_factory=list)
    poll_interval_seconds: int = 300
    item_path: list[str] = field(default_factory=list)
    field_map: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    include_url_patterns: list[str] = field(default_factory=list)
    exclude_url_patterns: list[str] = field(default_factory=list)
    max_documents_per_poll: int = 50
    bootstrap_lookback_days: int = 14
    article_fetch: str = "auto"
    browser_name: str = "firefox"


@dataclass(slots=True)
class ThemeAction:
    ticker: str
    side: str
    weight: float = 1.0


@dataclass(slots=True)
class ThemeRule:
    name: str
    keywords: list[str]
    actions: list[ThemeAction]
    weight: float = 1.0


@dataclass(slots=True)
class CompanyRule:
    trigger_company: str
    categories: list[str]
    required_themes: list[str]
    min_market_impact: float
    actions: list[ThemeAction]


@dataclass(slots=True)
class MarketProfile:
    issuer_aliases: dict[str, list[str]] = field(default_factory=dict)
    official_domains: dict[str, list[str]] = field(default_factory=dict)
    theme_rules: list[ThemeRule] = field(default_factory=list)
    company_rules: list[CompanyRule] = field(default_factory=list)


@dataclass(slots=True)
class AppConfig:
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    sources: list[SourceConfig] = field(default_factory=list)
    market_profile: MarketProfile = field(default_factory=MarketProfile)
    company_registry: list[CompanyProfile] = field(default_factory=list)
    impact_graph: list[ImpactRule] = field(default_factory=list)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


def _theme_action_from_raw(raw: dict[str, Any]) -> ThemeAction:
    return ThemeAction(
        ticker=raw["ticker"],
        side=raw["side"],
        weight=float(raw.get("weight", 1.0)),
    )


def _load_market_profile(path: Path) -> MarketProfile:
    raw = _load_json(path)
    return MarketProfile(
        issuer_aliases={key: list(value) for key, value in raw.get("issuer_aliases", {}).items()},
        official_domains={key: list(value) for key, value in raw.get("official_domains", {}).items()},
        theme_rules=[
            ThemeRule(
                name=item["name"],
                keywords=list(item.get("keywords", [])),
                actions=[_theme_action_from_raw(action) for action in item.get("actions", [])],
                weight=float(item.get("weight", 1.0)),
            )
            for item in raw.get("theme_rules", [])
        ],
        company_rules=[
            CompanyRule(
                trigger_company=item["trigger_company"],
                categories=list(item.get("categories", [])),
                required_themes=list(item.get("required_themes", [])),
                min_market_impact=float(item.get("min_market_impact", 0.0)),
                actions=[_theme_action_from_raw(action) for action in item.get("actions", [])],
            )
            for item in raw.get("company_rules", [])
        ],
    )


def _load_sources(path: Path) -> list[SourceConfig]:
    raw = _load_json(path)
    return [
        SourceConfig(
            name=item["name"],
            kind=item["kind"],
            url=item["url"],
            enabled=bool(item.get("enabled", True)),
            tags=list(item.get("tags", [])),
            poll_interval_seconds=int(item.get("poll_interval_seconds", 300)),
            item_path=list(item.get("item_path", [])),
            field_map=dict(item.get("field_map", {})),
            headers=dict(item.get("headers", {})),
            include_url_patterns=list(item.get("include_url_patterns", [])),
            exclude_url_patterns=list(item.get("exclude_url_patterns", [])),
            max_documents_per_poll=int(item.get("max_documents_per_poll", 50)),
            bootstrap_lookback_days=int(item.get("bootstrap_lookback_days", 14)),
            article_fetch=str(item.get("article_fetch", "auto")),
            browser_name=str(item.get("browser_name", "firefox")),
        )
        for item in raw.get("sources", [])
    ]


def _load_company_registry(path: Path) -> list[CompanyProfile]:
    raw = _load_json(path)
    profiles: list[CompanyProfile] = []
    for item in raw.get("companies", []):
        name = item["name"]
        profiles.append(
            CompanyProfile(
                company_id=item.get("company_id") or stable_hash(name)[:24],
                name=name,
                company_type=str(item.get("company_type", "private")),
                size_score=float(item.get("size_score", 0.5)),
                aliases=list(item.get("aliases", [])),
                domains=list(item.get("domains", [])),
                listed_symbols=list(item.get("listed_symbols", [])),
                metadata=dict(item.get("metadata", {})),
            )
        )
    return profiles


def _load_impact_graph(path: Path) -> list[ImpactRule]:
    raw = _load_json(path)
    rules: list[ImpactRule] = []
    for item in raw.get("rules", []):
        rules.append(
            ImpactRule(
                rule_id=item.get("rule_id")
                or stable_hash(item["source_company"], item["target_symbol"], item.get("relation", "competitor"))[:24],
                source_company=item["source_company"],
                target_symbol=item["target_symbol"],
                target_asset_type=str(item.get("target_asset_type", "equity")),
                relation=str(item.get("relation", "competitor")),
                direction=str(item.get("direction", "negative")),
                base_weight=float(item.get("base_weight", 1.0)),
                themes=list(item.get("themes", [])),
                categories=list(item.get("categories", [])),
                metadata=dict(item.get("metadata", {})),
            )
        )
    return rules


def _config_from_raw(raw: dict[str, Any], root: Path) -> AppConfig:
    runner_raw = raw.get("runner", {})
    storage_raw = raw.get("storage", {})
    queue_raw = raw.get("queue", {})
    llm_raw = raw.get("llm", {})
    delivery_raw = raw.get("delivery", raw.get("broker", {}))
    risk_raw = raw.get("risk", {})

    sources_path = Path(raw.get("sources_file", DEFAULT_SOURCES_PATH))
    if not sources_path.is_absolute():
        sources_path = root / sources_path

    market_profile_path = Path(raw.get("market_profile_file", DEFAULT_MARKET_PROFILE_PATH))
    if not market_profile_path.is_absolute():
        market_profile_path = root / market_profile_path

    company_registry_path = Path(raw.get("company_registry_file", DEFAULT_COMPANY_REGISTRY_PATH))
    if not company_registry_path.is_absolute():
        company_registry_path = root / company_registry_path

    impact_graph_path = Path(raw.get("impact_graph_file", DEFAULT_IMPACT_GRAPH_PATH))
    if not impact_graph_path.is_absolute():
        impact_graph_path = root / impact_graph_path

    runner = RunnerConfig(
        poll_interval_seconds=int(runner_raw.get("poll_interval_seconds", 120)),
        host_min_interval_seconds=float(runner_raw.get("host_min_interval_seconds", 1.5)),
        http_timeout_seconds=int(runner_raw.get("http_timeout_seconds", 15)),
        dry_run=bool(runner_raw.get("dry_run", True)),
        ingest_token=str(runner_raw.get("ingest_token", os.getenv("INGEST_TOKEN", "change-me"))),
        queue_drain_batch_size=int(runner_raw.get("queue_drain_batch_size", 500)),
    )

    storage = StorageConfig(
        kind=str(storage_raw.get("kind", "sqlite")),
        sqlite_path=(
            (root / storage_raw["sqlite_path"]).resolve()
            if storage_raw.get("sqlite_path")
            else (root / runner_raw["sqlite_path"]).resolve()
            if runner_raw.get("sqlite_path")
            else (root / DEFAULT_DB_PATH).resolve()
        ),
        postgres_dsn_env_var=str(storage_raw.get("postgres_dsn_env_var", "POSTGRES_DSN")),
    )

    queue = QueueConfig(
        kind=str(queue_raw.get("kind", "sqlite")),
        sqlite_path=(
            (root / queue_raw["sqlite_path"]).resolve()
            if queue_raw.get("sqlite_path")
            else (root / DEFAULT_QUEUE_DB_PATH).resolve()
        ),
        redis_url_env_var=str(queue_raw.get("redis_url_env_var", "REDIS_URL")),
        reclaim_timeout_seconds=int(queue_raw.get("reclaim_timeout_seconds", 300)),
        block_timeout_seconds=int(queue_raw.get("block_timeout_seconds", 1)),
    )

    llm = LLMConfig(
        enabled=bool(llm_raw.get("enabled", False)),
        endpoint_url=str(llm_raw.get("endpoint_url", DEFAULT_LLM_ENDPOINT_URL)),
        api_key_env_var=str(llm_raw.get("api_key_env_var", DEFAULT_LLM_API_KEY_ENV)),
        model=str(llm_raw.get("model", DEFAULT_LLM_MODEL)),
        temperature=float(llm_raw.get("temperature", 0.0)),
        max_tokens=int(llm_raw.get("max_tokens", 700)),
        daily_budget_usd=float(llm_raw.get("daily_budget_usd", 10.0)),
        escalate_uncertainty_threshold=float(llm_raw.get("escalate_uncertainty_threshold", 0.42)),
        escalate_market_impact_threshold=float(llm_raw.get("escalate_market_impact_threshold", 0.65)),
        escalate_novelty_threshold=float(llm_raw.get("escalate_novelty_threshold", 0.7)),
        min_minutes_between_rechecks=int(llm_raw.get("min_minutes_between_rechecks", 180)),
    )

    delivery = DeliveryConfig(
        kind=str(delivery_raw.get("kind", "outbox")),
        outbox_path=str(delivery_raw.get("outbox_path", "data/orders_outbox.jsonl")),
    )

    risk = RiskConfig(
        max_order_notional_usd=float(risk_raw.get("max_order_notional_usd", 2_000.0)),
        min_order_notional_usd=float(risk_raw.get("min_order_notional_usd", 200.0)),
        max_position_per_ticker_usd=float(risk_raw.get("max_position_per_ticker_usd", 10_000.0)),
        max_total_exposure_usd=float(risk_raw.get("max_total_exposure_usd", 25_000.0)),
        max_orders_per_day=int(risk_raw.get("max_orders_per_day", 20)),
        cooldown_minutes_per_ticker=int(risk_raw.get("cooldown_minutes_per_ticker", 240)),
        min_confidence_to_trade=float(risk_raw.get("min_confidence_to_trade", 0.63)),
        min_market_impact_to_trade=float(risk_raw.get("min_market_impact_to_trade", 0.55)),
        min_crypto_impact_to_trade=float(risk_raw.get("min_crypto_impact_to_trade", 0.8)),
        allow_short=bool(risk_raw.get("allow_short", True)),
    )

    return AppConfig(
        runner=runner,
        storage=storage,
        queue=queue,
        llm=llm,
        delivery=delivery,
        risk=risk,
        sources=_load_sources(sources_path),
        market_profile=_load_market_profile(market_profile_path),
        company_registry=_load_company_registry(company_registry_path),
        impact_graph=_load_impact_graph(impact_graph_path),
    )


def load_config(config_path: str | Path | None = None) -> AppConfig:
    root = Path.cwd()
    raw: dict[str, Any] = {}
    path = Path(config_path) if config_path else DEFAULT_APP_CONFIG_PATH
    if not path.is_absolute():
        path = root / path
    if path.exists():
        root = path.parent.parent if path.parent.name == "config" else path.parent
        raw = _load_json(path)
    return _config_from_raw(raw, root)


def config_to_dict(config: AppConfig) -> dict[str, Any]:
    return asdict(config)
