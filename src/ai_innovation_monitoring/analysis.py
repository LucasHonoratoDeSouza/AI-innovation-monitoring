from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from ai_innovation_monitoring.config import LLMConfig, MarketProfile
from ai_innovation_monitoring.domain import InnovationEvent, SourceDocument, stable_hash
from ai_innovation_monitoring.fetching import html_to_text
from ai_innovation_monitoring.storage import Store


LAUNCH_TERMS = {
    "launch": 0.24,
    "release": 0.22,
    "announces": 0.18,
    "unveils": 0.24,
    "introduces": 0.18,
    "available": 0.08,
    "beta": 0.12,
    "preview": 0.1,
    "agent": 0.1,
    "design": 0.1,
    "workflow": 0.08,
}

HIGH_IMPACT_TERMS = {
    "enterprise": 0.18,
    "api": 0.1,
    "developer": 0.08,
    "studio": 0.1,
    "replaces": 0.16,
    "faster": 0.08,
    "cheaper": 0.08,
    "autonomous": 0.12,
    "benchmark": 0.08,
}

CATEGORY_RULES = {
    "security": ["breach", "exploit", "vulnerability", "security"],
    "funding": ["funding", "raises", "series", "valuation"],
    "acquisition": ["acquires", "acquisition", "merger"],
    "regulation": ["regulation", "law", "eu ai act", "compliance"],
    "model_release": ["model", "weights", "benchmark", "context window"],
    "product_launch": ["launch", "release", "beta", "preview", "available", "introduces", "unveils"],
}


def _bounded(value: float) -> float:
    return max(0.0, min(1.0, round(value, 4)))


def _top_matches(text: str, keywords: list[str]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


class HeuristicAnalyzer:
    def __init__(self, market_profile: MarketProfile) -> None:
        self.market_profile = market_profile

    def analyze(self, document: SourceDocument) -> InnovationEvent:
        text = html_to_text(f"{document.title}\n{document.content}")
        lowered = text.lower()
        company = self._infer_company(document)
        category = self._infer_category(lowered)
        official_source = self._is_official_source(company, document.host)
        novelty = self._score_novelty(lowered, official_source)
        market_impact = self._score_market_impact(lowered, official_source)
        theme_matches = self._theme_matches(lowered)
        confidence = self._score_confidence(company, official_source, document, theme_matches)
        summary = self._build_summary(document)
        rationale = self._build_rationale(company, category, official_source, theme_matches)
        tickers = sorted({action.ticker for theme in self.market_profile.theme_rules for action in theme.actions if theme.name in theme_matches})
        event_id = stable_hash(document.document_id, company, category, summary)[:24]
        canonical_id = stable_hash(company, category, document.title)
        return InnovationEvent(
            event_id=event_id,
            document_id=document.document_id,
            canonical_id=canonical_id,
            company=company,
            category=category,
            summary=summary,
            novelty_score=novelty,
            market_impact_score=market_impact,
            confidence=confidence,
            tickers=tickers,
            rationale=rationale,
            theme_matches=theme_matches,
            official_source=official_source,
            requires_human_review=market_impact > 0.75 and confidence < 0.65,
            metadata={
                "host": document.host,
                "source_name": document.source_name,
            },
        )

    def _infer_company(self, document: SourceDocument) -> str:
        text = f"{document.host} {document.title} {document.content}".lower()
        for issuer, aliases in self.market_profile.issuer_aliases.items():
            for alias in aliases:
                if alias.lower() in text:
                    return issuer
        return document.host.split(".")[0].title()

    def _infer_category(self, lowered_text: str) -> str:
        best_category = "news"
        best_score = 0
        for category, keywords in CATEGORY_RULES.items():
            matches = sum(1 for keyword in keywords if keyword in lowered_text)
            if matches > best_score:
                best_category = category
                best_score = matches
        return best_category

    def _is_official_source(self, company: str, host: str) -> bool:
        host = host.lower()
        if "official" in getattr(document := None, "tags", []):
            return True
        for official_domain in self.market_profile.official_domains.get(company, []):
            if official_domain.lower() in host:
                return True
        return False

    def _score_novelty(self, lowered_text: str, official_source: bool) -> float:
        score = 0.2
        for keyword, weight in LAUNCH_TERMS.items():
            if keyword in lowered_text:
                score += weight
        if official_source:
            score += 0.1
        if "today" in lowered_text or "now available" in lowered_text:
            score += 0.08
        return _bounded(score)

    def _score_market_impact(self, lowered_text: str, official_source: bool) -> float:
        score = 0.18
        for keyword, weight in HIGH_IMPACT_TERMS.items():
            if keyword in lowered_text:
                score += weight
        if official_source:
            score += 0.08
        if "pricing" in lowered_text or "subscription" in lowered_text:
            score += 0.08
        if "free" in lowered_text:
            score += 0.05
        return _bounded(score)

    def _theme_matches(self, lowered_text: str) -> list[str]:
        matches: list[str] = []
        for theme in self.market_profile.theme_rules:
            if _top_matches(lowered_text, theme.keywords):
                matches.append(theme.name)
        return matches

    def _score_confidence(
        self,
        company: str,
        official_source: bool,
        document: SourceDocument,
        theme_matches: list[str],
    ) -> float:
        score = 0.4
        if official_source:
            score += 0.22
        if company and "." not in company:
            score += 0.1
        if document.published_at is not None:
            score += 0.08
        if theme_matches:
            score += 0.08
        if len(document.content) > 120:
            score += 0.06
        return _bounded(score)

    def _build_summary(self, document: SourceDocument) -> str:
        body = document.content.strip() or document.title.strip()
        summary = body[:320].strip()
        if len(body) > 320:
            summary += "..."
        return summary

    def _build_rationale(
        self,
        company: str,
        category: str,
        official_source: bool,
        theme_matches: list[str],
    ) -> str:
        parts = [
            f"company={company}",
            f"category={category}",
            f"official_source={official_source}",
        ]
        if theme_matches:
            parts.append(f"themes={','.join(theme_matches)}")
        return "; ".join(parts)


@dataclass(slots=True)
class RoutedAnalysis:
    event: InnovationEvent
    escalated: bool
    reason: str


class OpenAICompatibleLLMAnalyzer:
    def __init__(self, config: LLMConfig, store: Store) -> None:
        self.config = config
        self.store = store

    def available(self) -> bool:
        return self.config.enabled and bool(os.getenv(self.config.api_key_env_var))

    def analyze(self, document: SourceDocument, heuristic: InnovationEvent) -> InnovationEvent:
        if not self.available():
            return heuristic

        api_key = os.getenv(self.config.api_key_env_var, "")
        prompt = self._build_prompt(document, heuristic)
        request_body = {
            "model": self.config.model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict analyst for AI-launch event extraction. "
                        "Return compact JSON only."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        payload = json.dumps(request_body).encode("utf-8")
        request = Request(
            self.config.endpoint_url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )
        with urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
        content = body["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        usage = body.get("usage", {})
        tokens_in = int(usage.get("prompt_tokens", 0))
        tokens_out = int(usage.get("completion_tokens", 0))
        estimated_cost = self._estimate_cost(tokens_in, tokens_out)
        self.store.record_llm_usage(self.config.model, estimated_cost, tokens_in, tokens_out)
        return InnovationEvent(
            event_id=heuristic.event_id,
            document_id=heuristic.document_id,
            canonical_id=heuristic.canonical_id,
            company=str(parsed.get("company") or heuristic.company),
            category=str(parsed.get("category") or heuristic.category),
            summary=str(parsed.get("summary") or heuristic.summary),
            novelty_score=_bounded(float(parsed.get("novelty_score", heuristic.novelty_score))),
            market_impact_score=_bounded(float(parsed.get("market_impact_score", heuristic.market_impact_score))),
            confidence=_bounded(float(parsed.get("confidence", heuristic.confidence))),
            tickers=list(parsed.get("tickers") or heuristic.tickers),
            rationale=str(parsed.get("rationale") or heuristic.rationale),
            theme_matches=list(parsed.get("theme_matches") or heuristic.theme_matches),
            llm_used=True,
            official_source=bool(parsed.get("official_source", heuristic.official_source)),
            requires_human_review=bool(parsed.get("requires_human_review", heuristic.requires_human_review)),
            metadata={**heuristic.metadata, "llm_model": self.config.model},
        )

    def _build_prompt(self, document: SourceDocument, heuristic: InnovationEvent) -> str:
        return json.dumps(
            {
                "document": document.to_dict(),
                "heuristic_event": heuristic.to_dict(),
                "task": (
                    "Validate whether this is a materially new AI innovation event. "
                    "Infer company, category, novelty_score, market_impact_score, confidence, "
                    "summary, theme_matches, tickers, official_source, requires_human_review, rationale."
                ),
            }
        )

    def _estimate_cost(self, tokens_in: int, tokens_out: int) -> float:
        return round((tokens_in * 0.0000005) + (tokens_out * 0.0000015), 6)


class CostAwareRouter:
    def __init__(self, config: LLMConfig, store: Store, llm_analyzer: OpenAICompatibleLLMAnalyzer | None = None) -> None:
        self.config = config
        self.store = store
        self.llm_analyzer = llm_analyzer

    def route(self, document: SourceDocument, heuristic: InnovationEvent) -> RoutedAnalysis:
        uncertainty = 1.0 - heuristic.confidence
        over_budget = self.store.llm_spend_last_24h() >= self.config.daily_budget_usd
        should_escalate = (
            self.llm_analyzer is not None
            and self.llm_analyzer.available()
            and not over_budget
            and (
                uncertainty >= self.config.escalate_uncertainty_threshold
                or heuristic.market_impact_score >= self.config.escalate_market_impact_threshold
                or heuristic.novelty_score >= self.config.escalate_novelty_threshold
            )
        )
        if not should_escalate:
            reason = "heuristic_only"
            if over_budget:
                reason = "budget_exhausted"
            return RoutedAnalysis(event=heuristic, escalated=False, reason=reason)
        event = self.llm_analyzer.analyze(document, heuristic)
        return RoutedAnalysis(event=event, escalated=True, reason="uncertain_or_high_impact")
