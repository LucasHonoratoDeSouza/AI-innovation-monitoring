from ai_innovation_monitoring.config import CompanyRule, MarketProfile, RiskConfig, ThemeAction, ThemeRule
from ai_innovation_monitoring.decision import DecisionEngine, RiskManager
from ai_innovation_monitoring.domain import InnovationEvent
from ai_innovation_monitoring.storage import SQLiteStore


def test_decision_engine_generates_intent(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    risk_manager = RiskManager(RiskConfig(max_order_notional_usd=1000.0), store)
    market_profile = MarketProfile(
        theme_rules=[
            ThemeRule(
                name="ai_design",
                keywords=["design"],
                actions=[ThemeAction(ticker="ADBE", side="sell", weight=1.0)],
                weight=1.0,
            )
        ],
        company_rules=[
            CompanyRule(
                trigger_company="Anthropic",
                categories=["product_launch"],
                required_themes=["ai_design"],
                min_market_impact=0.6,
                actions=[ThemeAction(ticker="ADBE", side="sell", weight=1.0)],
            )
        ],
    )
    engine = DecisionEngine(market_profile, risk_manager)
    event = InnovationEvent(
        event_id="evt-1",
        document_id="doc-1",
        canonical_id="canon-1",
        company="Anthropic",
        category="product_launch",
        summary="New design product",
        novelty_score=0.8,
        market_impact_score=0.8,
        confidence=0.9,
        theme_matches=["ai_design"],
    )
    outcome = engine.evaluate(event)
    assert len(outcome.intents) == 1
    intent = outcome.intents[0]
    assert intent.ticker == "ADBE"
    assert intent.side == "sell"
    assert outcome.blocked_reasons == []


def test_risk_manager_blocks_cooldown(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    risk_manager = RiskManager(RiskConfig(max_order_notional_usd=1000.0, cooldown_minutes_per_ticker=360), store)
    market_profile = MarketProfile(
        theme_rules=[
            ThemeRule(
                name="ai_design",
                keywords=["design"],
                actions=[ThemeAction(ticker="ADBE", side="sell", weight=1.0)],
                weight=1.0,
            )
        ]
    )
    engine = DecisionEngine(market_profile, risk_manager)
    event = InnovationEvent(
        event_id="evt-1",
        document_id="doc-1",
        canonical_id="canon-1",
        company="Anthropic",
        category="product_launch",
        summary="New design product",
        novelty_score=0.8,
        market_impact_score=0.8,
        confidence=0.9,
        theme_matches=["ai_design"],
    )
    first_outcome = engine.evaluate(event)
    intent = first_outcome.intents[0]
    store.save_order_pending(intent)
    store.save_order_result(
        intent,
        __import__("ai_innovation_monitoring.domain", fromlist=["OrderResult"]).OrderResult(
            intent_id=intent.intent_id,
            delivery_name="outbox",
            delivery_order_id="1",
            status="simulated",
        ),
    )
    second_outcome = engine.evaluate(
        InnovationEvent(
            event_id="evt-2",
            document_id="doc-2",
            canonical_id="canon-2",
            company="Anthropic",
            category="product_launch",
            summary="Another design product",
            novelty_score=0.8,
            market_impact_score=0.8,
            confidence=0.9,
            theme_matches=["ai_design"],
        )
    )
    assert second_outcome.intents == []
    assert any(reason.startswith("ADBE:ticker_cooldown") for reason in second_outcome.blocked_reasons)
