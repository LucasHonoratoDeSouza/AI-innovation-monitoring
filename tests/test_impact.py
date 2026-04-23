from ai_innovation_monitoring.domain import CompanyProfile, ImpactRule, InnovationEvent
from ai_innovation_monitoring.impact import ImpactEngine
from ai_innovation_monitoring.storage import SQLiteStore


def test_impact_engine_creates_assessment(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    store.upsert_company_profile(
        CompanyProfile(
            company_id="anthropic",
            name="Anthropic",
            company_type="private",
            size_score=0.92,
            aliases=["anthropic", "claude"],
        )
    )
    store.upsert_impact_rule(
        ImpactRule(
            rule_id="rule-1",
            source_company="Anthropic",
            target_symbol="ADBE",
            target_asset_type="equity",
            relation="competitor",
            direction="negative",
            base_weight=0.95,
            themes=["ai_design"],
            categories=["product_launch"],
        )
    )
    engine = ImpactEngine(store)
    assessments = engine.assess(
        InnovationEvent(
            event_id="evt-1",
            document_id="doc-1",
            canonical_id="canon-1",
            company="Anthropic",
            category="product_launch",
            summary="Claude Design launch",
            novelty_score=0.82,
            market_impact_score=0.88,
            confidence=0.81,
            theme_matches=["ai_design"],
        )
    )
    assert len(assessments) == 1
    assessment = assessments[0]
    assert assessment.target_symbol == "ADBE"
    assert assessment.direction == "negative"
    assert assessment.impact_score > 0.6
    assert assessment.confidence > 0.7
