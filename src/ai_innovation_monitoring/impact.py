from __future__ import annotations

from ai_innovation_monitoring.domain import ImpactAssessment, InnovationEvent, stable_hash
from ai_innovation_monitoring.storage import Store


def _bounded(value: float) -> float:
    return max(0.0, min(1.0, round(value, 4)))


class ImpactEngine:
    def __init__(self, store: Store) -> None:
        self.store = store

    def assess(self, event: InnovationEvent) -> list[ImpactAssessment]:
        issuer_profile = self.store.get_company_profile(event.company)
        issuer_size_score = issuer_profile.size_score if issuer_profile is not None else 0.45
        innovation_strength = _bounded((event.novelty_score * 0.45) + (event.market_impact_score * 0.55))
        rules = self.store.list_impact_rules(event.company)
        assessments: list[ImpactAssessment] = []

        for rule in rules:
            theme_multiplier = self._theme_multiplier(rule.themes, event.theme_matches)
            category_multiplier = self._category_multiplier(rule.categories, event.category)
            if theme_multiplier == 0.0 or category_multiplier == 0.0:
                continue
            rarity_penalty = 0.85 if rule.metadata.get("rare", False) else 1.0
            impact_score = _bounded(
                innovation_strength * issuer_size_score * rule.base_weight * theme_multiplier * category_multiplier * rarity_penalty
            )
            if impact_score == 0.0:
                continue
            confidence = _bounded(event.confidence * (0.65 + (0.35 * max(theme_multiplier, category_multiplier))))
            rationale = (
                f"source={event.company}; relation={rule.relation}; direction={rule.direction}; "
                f"issuer_size={issuer_size_score}; innovation_strength={innovation_strength}"
            )
            assessments.append(
                ImpactAssessment(
                    assessment_id=stable_hash(event.event_id, rule.target_symbol, rule.direction)[:24],
                    event_id=event.event_id,
                    source_company=event.company,
                    target_symbol=rule.target_symbol,
                    target_asset_type=rule.target_asset_type,
                    relation=rule.relation,
                    direction=rule.direction,
                    impact_score=impact_score,
                    confidence=confidence,
                    issuer_size_score=issuer_size_score,
                    innovation_strength_score=innovation_strength,
                    horizon=str(rule.metadata.get("horizon", "near_term")),
                    rationale=rationale,
                    metadata={
                        "rule_id": rule.rule_id,
                        "event_category": event.category,
                        "event_themes": list(event.theme_matches),
                        **rule.metadata,
                    },
                )
            )
        return sorted(assessments, key=lambda item: (item.impact_score, item.confidence), reverse=True)

    def _theme_multiplier(self, rule_themes: list[str], event_themes: list[str]) -> float:
        if not rule_themes:
            return 1.0
        overlap = set(rule_themes).intersection(event_themes)
        if not overlap:
            return 0.0
        return _bounded(0.5 + (0.5 * (len(overlap) / len(set(rule_themes)))))

    def _category_multiplier(self, rule_categories: list[str], event_category: str) -> float:
        if not rule_categories:
            return 1.0
        return 1.0 if event_category in rule_categories else 0.0
