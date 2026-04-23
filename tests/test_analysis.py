from ai_innovation_monitoring.analysis import CostAwareRouter, HeuristicAnalyzer
from ai_innovation_monitoring.config import LLMConfig, MarketProfile, ThemeAction, ThemeRule
from ai_innovation_monitoring.domain import InnovationEvent, SourceDocument
from ai_innovation_monitoring.storage import SQLiteStore


class StubLLMAnalyzer:
    def __init__(self) -> None:
        self.called = False

    def available(self) -> bool:
        return True

    def analyze(self, document: SourceDocument, heuristic: InnovationEvent) -> InnovationEvent:
        self.called = True
        heuristic.llm_used = True
        heuristic.confidence = 0.91
        return heuristic


def test_router_escalates_high_impact(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    config = LLMConfig(enabled=True, daily_budget_usd=10.0)
    llm = StubLLMAnalyzer()
    router = CostAwareRouter(config=config, store=store, llm_analyzer=llm)
    heuristic = InnovationEvent(
        event_id="evt-1",
        document_id="doc-1",
        canonical_id="canon-1",
        company="Anthropic",
        category="product_launch",
        summary="New design workflow",
        novelty_score=0.8,
        market_impact_score=0.82,
        confidence=0.4,
    )
    document = SourceDocument(
        source_name="x",
        url="https://openai.com/index/introducing-openai",
        title="Introducing OpenAI",
    )
    result = router.route(document, heuristic)
    assert result.escalated is True
    assert llm.called is True
    assert result.event.llm_used is True


def test_heuristic_analyzer_detects_theme():
    market_profile = MarketProfile(
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
    )
    analyzer = HeuristicAnalyzer(market_profile)
    event = analyzer.analyze(
        SourceDocument(
            source_name="anthropic-blog",
            url="https://www.anthropic.com/news/claude-design",
            title="Anthropic launches Claude for design workflows",
            content="The launch targets design and mockup automation for teams.",
            host="www.anthropic.com",
        )
    )
    assert event.company == "Anthropic"
    assert "ai_design" in event.theme_matches
    assert event.official_source is True
