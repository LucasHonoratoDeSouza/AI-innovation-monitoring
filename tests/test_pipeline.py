import json

from ai_innovation_monitoring.config import (
    AppConfig,
    DeliveryConfig,
    MarketProfile,
    QueueConfig,
    RiskConfig,
    RunnerConfig,
    StorageConfig,
    ThemeAction,
    ThemeRule,
)
from ai_innovation_monitoring.domain import CompanyProfile, ImpactRule
from ai_innovation_monitoring.orchestrator import build_monitor_service


def test_pipeline_external_ingest_emits_order(tmp_path):
    outbox_path = tmp_path / "orders_outbox.jsonl"
    config = AppConfig(
        runner=RunnerConfig(queue_drain_batch_size=50),
        storage=StorageConfig(kind="sqlite", sqlite_path=tmp_path / "test.db"),
        queue=QueueConfig(kind="sqlite", sqlite_path=tmp_path / "queue.db"),
        delivery=DeliveryConfig(kind="outbox", outbox_path=str(outbox_path)),
        risk=RiskConfig(min_market_impact_to_trade=0.4),
        sources=[],
        market_profile=MarketProfile(
            issuer_aliases={"Anthropic": ["anthropic", "claude"]},
            official_domains={"Anthropic": ["anthropic.com"]},
            theme_rules=[
                ThemeRule(
                    name="ai_design",
                    keywords=["design", "mockup"],
                    actions=[ThemeAction(ticker="ADBE", side="sell", weight=1.0)],
                    weight=1.0,
                )
            ],
        ),
        company_registry=[
            CompanyProfile(
                company_id="anthropic",
                name="Anthropic",
                company_type="private",
                size_score=0.93,
                aliases=["anthropic", "claude"],
                domains=["anthropic.com"],
            )
        ],
        impact_graph=[
            ImpactRule(
                rule_id="rule-1",
                source_company="Anthropic",
                target_symbol="ADBE",
                target_asset_type="equity",
                relation="competitor",
                direction="negative",
                base_weight=0.92,
                themes=["ai_design"],
                categories=["product_launch"],
            )
        ],
    )
    monitor = build_monitor_service(config)
    result = monitor.ingest_external(
        {
            "source_name": "anthropic-newsroom",
            "url": "https://www.anthropic.com/news/claude-design",
            "title": "Anthropic launches Claude Design",
            "content": "New design and mockup workflow for enterprise teams.",
            "host": "www.anthropic.com",
            "tags": ["official", "ai_design"],
        }
    )
    assert result["status"] == "queued"
    intelligence_stats = monitor.run_intelligence_once(10)
    delivery_stats = monitor.run_delivery_once(10)
    assert intelligence_stats.events_generated == 1
    assert delivery_stats.intents_submitted == 1
    lines = outbox_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["intent"]["ticker"] == "ADBE"
    assert payload["intent"]["side"] == "sell"
