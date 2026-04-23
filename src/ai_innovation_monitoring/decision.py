from __future__ import annotations

from dataclasses import dataclass

from ai_innovation_monitoring.config import CompanyRule, MarketProfile, RiskConfig, ThemeAction
from ai_innovation_monitoring.domain import ImpactAssessment, InnovationEvent, OrderIntent, stable_hash
from ai_innovation_monitoring.storage import Store


@dataclass(slots=True)
class DecisionOutcome:
    intents: list[OrderIntent]
    blocked_reasons: list[str]


@dataclass(slots=True)
class CandidateAction:
    symbol: str
    asset_type: str
    side: str
    weight: float
    confidence: float
    reason: str
    metadata: dict


class RiskManager:
    def __init__(self, config: RiskConfig, store: Store) -> None:
        self.config = config
        self.store = store

    def validate(
        self,
        ticker: str,
        side: str,
        notional_usd: float,
        confidence: float,
        market_impact: float,
        asset_type: str = "equity",
    ) -> list[str]:
        blocked: list[str] = []
        if confidence < self.config.min_confidence_to_trade:
            blocked.append("confidence_below_threshold")
        if market_impact < self.config.min_market_impact_to_trade:
            blocked.append("market_impact_below_threshold")
        if asset_type == "crypto" and market_impact < self.config.min_crypto_impact_to_trade:
            blocked.append("crypto_impact_below_threshold")
        if notional_usd < self.config.min_order_notional_usd:
            blocked.append("order_too_small")
        if notional_usd > self.config.max_order_notional_usd:
            blocked.append("order_too_large")
        if side == "sell" and not self.config.allow_short:
            blocked.append("short_disabled")
        if self.store.daily_order_count() >= self.config.max_orders_per_day:
            blocked.append("daily_order_limit")
        if self.store.exposure_for_ticker(ticker) + abs(notional_usd) > self.config.max_position_per_ticker_usd:
            blocked.append("ticker_exposure_limit")
        if self.store.total_live_exposure() + abs(notional_usd) > self.config.max_total_exposure_usd:
            blocked.append("portfolio_exposure_limit")
        if self.store.recent_orders_for_ticker(ticker, self.config.cooldown_minutes_per_ticker):
            blocked.append("ticker_cooldown")
        return blocked


class DecisionEngine:
    def __init__(self, market_profile: MarketProfile, risk_manager: RiskManager) -> None:
        self.market_profile = market_profile
        self.risk_manager = risk_manager

    def evaluate(self, event: InnovationEvent, assessments: list[ImpactAssessment] | None = None) -> DecisionOutcome:
        candidate_actions = self._impact_actions(assessments or [])
        if not candidate_actions:
            candidate_actions = self._fallback_actions(event)

        deduped: dict[tuple[str, str, str], CandidateAction] = {}
        for action in candidate_actions:
            key = (action.symbol, action.asset_type, action.side)
            current = deduped.get(key)
            if current is None or (action.weight, action.confidence) > (current.weight, current.confidence):
                deduped[key] = action

        intents: list[OrderIntent] = []
        blocked_reasons: list[str] = []
        for action in deduped.values():
            notional_usd = round(
                self.risk_manager.config.max_order_notional_usd * action.weight * action.confidence,
                2,
            )
            blocked = self.risk_manager.validate(
                ticker=action.symbol,
                side=action.side,
                notional_usd=notional_usd,
                confidence=action.confidence,
                market_impact=action.weight,
                asset_type=action.asset_type,
            )
            if blocked:
                blocked_reasons.extend(f"{action.symbol}:{reason}" for reason in blocked)
                continue
            intent_id = stable_hash(event.event_id, action.symbol, action.side)[:24]
            intents.append(
                OrderIntent(
                    intent_id=intent_id,
                    event_id=event.event_id,
                    ticker=action.symbol,
                    side=action.side,
                    notional_usd=notional_usd,
                    confidence=action.confidence,
                    reason=action.reason,
                    idempotency_key=stable_hash(event.canonical_id, action.symbol, action.side),
                    asset_type=action.asset_type,
                    metadata=action.metadata,
                )
            )
        return DecisionOutcome(intents=intents, blocked_reasons=sorted(set(blocked_reasons)))

    def _impact_actions(self, assessments: list[ImpactAssessment]) -> list[CandidateAction]:
        actions: list[CandidateAction] = []
        for assessment in assessments:
            side = "buy" if assessment.direction == "positive" else "sell"
            actions.append(
                CandidateAction(
                    symbol=assessment.target_symbol,
                    asset_type=assessment.target_asset_type,
                    side=side,
                    weight=assessment.impact_score,
                    confidence=assessment.confidence,
                    reason=assessment.rationale,
                    metadata={
                        "source_company": assessment.source_company,
                        "relation": assessment.relation,
                        "horizon": assessment.horizon,
                        **assessment.metadata,
                    },
                )
            )
        return actions

    def _fallback_actions(self, event: InnovationEvent) -> list[CandidateAction]:
        candidate_actions = self._theme_actions(event)
        candidate_actions.extend(self._company_rule_actions(event))
        actions: list[CandidateAction] = []
        for action in candidate_actions:
            actions.append(
                CandidateAction(
                    symbol=action.ticker,
                    asset_type="equity",
                    side=action.side,
                    weight=event.market_impact_score * action.weight,
                    confidence=event.confidence,
                    reason=f"{event.company} {event.category} themes={','.join(event.theme_matches)}",
                    metadata={
                        "theme_matches": event.theme_matches,
                        "event_company": event.company,
                    },
                )
            )
        return actions

    def _theme_actions(self, event: InnovationEvent) -> list[ThemeAction]:
        actions: list[ThemeAction] = []
        for theme_rule in self.market_profile.theme_rules:
            if theme_rule.name in event.theme_matches:
                for action in theme_rule.actions:
                    actions.append(ThemeAction(ticker=action.ticker, side=action.side, weight=action.weight * theme_rule.weight))
        return actions

    def _company_rule_actions(self, event: InnovationEvent) -> list[ThemeAction]:
        actions: list[ThemeAction] = []
        for rule in self.market_profile.company_rules:
            if self._matches_company_rule(rule, event):
                actions.extend(rule.actions)
        return actions

    def _matches_company_rule(self, rule: CompanyRule, event: InnovationEvent) -> bool:
        if rule.trigger_company != event.company:
            return False
        if rule.categories and event.category not in rule.categories:
            return False
        if rule.required_themes and not set(rule.required_themes).issubset(set(event.theme_matches)):
            return False
        if event.market_impact_score < rule.min_market_impact:
            return False
        return True
