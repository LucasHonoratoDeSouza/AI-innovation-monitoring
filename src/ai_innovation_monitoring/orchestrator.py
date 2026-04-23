from __future__ import annotations

import threading
import time
from dataclasses import asdict
from dataclasses import dataclass, field
from datetime import timedelta
from urllib.parse import urlparse

from ai_innovation_monitoring.analysis import CostAwareRouter, HeuristicAnalyzer, OpenAICompatibleLLMAnalyzer
from ai_innovation_monitoring.config import AppConfig
from ai_innovation_monitoring.decision import DecisionEngine, RiskManager
from ai_innovation_monitoring.domain import OrderIntent, SourceDocument, SourceState, utcnow
from ai_innovation_monitoring.fetching import RobustHttpClient
from ai_innovation_monitoring.impact import ImpactEngine
from ai_innovation_monitoring.order_delivery import OrderDelivery, build_order_delivery
from ai_innovation_monitoring.queueing import QueueBackend, build_queue
from ai_innovation_monitoring.sources import BaseSource, build_source
from ai_innovation_monitoring.storage import Store, build_store
from ai_innovation_monitoring.telemetry import TelemetryHub


DOCUMENT_QUEUE = "documents"
ORDER_QUEUE = "orders"


@dataclass(slots=True)
class CycleStats:
    sources_polled: int = 0
    documents_seen: int = 0
    documents_new: int = 0
    document_tasks_enqueued: int = 0
    intelligence_processed: int = 0
    events_generated: int = 0
    impact_assessments_generated: int = 0
    order_tasks_enqueued: int = 0
    intents_submitted: int = 0
    blocked_orders: int = 0
    last_error: str = ""


@dataclass(slots=True)
class MonitorRuntimeState:
    last_started_at: str | None = None
    last_finished_at: str | None = None
    last_cycle: CycleStats = field(default_factory=CycleStats)
    running: bool = False


class CollectorWorker:
    def __init__(
        self,
        store: Store,
        queue: QueueBackend,
        sources: list[BaseSource],
        http_client: RobustHttpClient,
        telemetry: TelemetryHub,
    ) -> None:
        self.store = store
        self.queue = queue
        self.sources = sources
        self.http_client = http_client
        self.telemetry = telemetry

    def run_once(self) -> CycleStats:
        stats = CycleStats()
        self.telemetry.set_worker("collector", status="running", current_item="scanning sources")
        for source in self.sources:
            if not source.config.enabled:
                continue
            if not self._source_is_due(source.config.name):
                continue
            try:
                self.telemetry.set_worker("collector", status="running", current_item=source.config.name)
                stats.sources_polled += 1
                source_state = self.store.get_source_state(source.config.name)
                poll_result = source.poll(self.http_client, source_state)
                next_poll_at = utcnow() + timedelta(seconds=source.config.poll_interval_seconds)
                self.store.upsert_source_state(
                    SourceState(
                        source_name=source.config.name,
                        etag=poll_result.etag or (source_state.etag if source_state else None),
                        last_modified=poll_result.last_modified or (source_state.last_modified if source_state else None),
                        cursor=poll_result.cursor or (source_state.cursor if source_state else None),
                        next_poll_at=next_poll_at,
                        failure_count=0,
                        backoff_until=None,
                        last_success_at=utcnow(),
                    )
                )
                stats.documents_seen += len(poll_result.documents)
                for document in poll_result.documents:
                    document.host = document.host or urlparse(document.url).netloc
                    if self.store.save_document(document):
                        stats.documents_new += 1
                        self.queue.publish(DOCUMENT_QUEUE, {"document_id": document.document_id})
                        stats.document_tasks_enqueued += 1
                self.telemetry.log(
                    "collector",
                    "source_polled",
                    f"{source.config.name} scanned",
                    {
                        "documents_seen": len(poll_result.documents),
                        "documents_new": stats.documents_new,
                        "queue_size": self.queue.size(DOCUMENT_QUEUE),
                    },
                )
            except Exception as error:
                stats.last_error = f"{source.config.name}: {error}"
                self._apply_source_backoff(source.config.name, source.config.poll_interval_seconds)
                self.telemetry.log(
                    "collector",
                    "source_error",
                    f"{source.config.name} failed",
                    {"error": str(error)},
                )
        self.telemetry.set_worker(
            "collector",
            status="idle",
            current_item="waiting",
            metrics={
                "sources_polled": stats.sources_polled,
                "documents_seen": stats.documents_seen,
                "documents_new": stats.documents_new,
            },
        )
        return stats

    def _source_is_due(self, source_name: str) -> bool:
        state = self.store.get_source_state(source_name)
        if state is None or state.next_poll_at is None:
            return True
        if state.backoff_until is not None and state.backoff_until > utcnow():
            return False
        return state.next_poll_at <= utcnow()

    def _apply_source_backoff(self, source_name: str, base_poll_seconds: int) -> None:
        previous_state = self.store.get_source_state(source_name)
        failure_count = (previous_state.failure_count if previous_state else 0) + 1
        backoff_seconds = min(base_poll_seconds * (2**failure_count), 3600)
        backoff_until = utcnow() + timedelta(seconds=backoff_seconds)
        self.store.upsert_source_state(
            SourceState(
                source_name=source_name,
                etag=previous_state.etag if previous_state else None,
                last_modified=previous_state.last_modified if previous_state else None,
                cursor=previous_state.cursor if previous_state else None,
                next_poll_at=backoff_until,
                failure_count=failure_count,
                backoff_until=backoff_until,
                last_success_at=previous_state.last_success_at if previous_state else None,
            )
        )


class IntelligenceWorker:
    def __init__(
        self,
        store: Store,
        queue: QueueBackend,
        analyzer: HeuristicAnalyzer,
        router: CostAwareRouter,
        impact_engine: ImpactEngine,
        decision_engine: DecisionEngine,
        telemetry: TelemetryHub,
    ) -> None:
        self.store = store
        self.queue = queue
        self.analyzer = analyzer
        self.router = router
        self.impact_engine = impact_engine
        self.decision_engine = decision_engine
        self.telemetry = telemetry

    def run_once(self, max_items: int) -> CycleStats:
        stats = CycleStats()
        self.telemetry.set_worker("intelligence", status="running", current_item="waiting for documents")
        for _ in range(max_items):
            message = self.queue.consume(DOCUMENT_QUEUE)
            if message is None:
                break
            try:
                document_id = str(message.payload["document_id"])
                document = self.store.get_document(document_id)
                if document is None:
                    self.queue.ack(message)
                    continue
                self.telemetry.set_worker(
                    "intelligence",
                    status="running",
                    current_item=document.title[:72],
                )
                heuristic_event = self.analyzer.analyze(document)
                routed = self.router.route(document, heuristic_event)
                self.store.save_event(routed.event)
                stats.intelligence_processed += 1
                stats.events_generated += 1

                assessments = self.impact_engine.assess(routed.event)
                for assessment in assessments:
                    self.store.save_impact_assessment(assessment)
                stats.impact_assessments_generated += len(assessments)

                outcome = self.decision_engine.evaluate(routed.event, assessments=assessments)
                stats.blocked_orders += len(outcome.blocked_reasons)
                for intent in outcome.intents:
                    self.store.save_order_pending(intent)
                    self.queue.publish(ORDER_QUEUE, intent.to_dict())
                    stats.order_tasks_enqueued += 1
                self.telemetry.log(
                    "intelligence",
                    "event_scored",
                    f"{routed.event.company} {routed.event.category}",
                    {
                        "title": document.title,
                        "novelty_score": routed.event.novelty_score,
                        "market_impact_score": routed.event.market_impact_score,
                        "confidence": routed.event.confidence,
                        "llm_used": routed.event.llm_used,
                        "impact_assessments": len(assessments),
                        "orders_enqueued": len(outcome.intents),
                        "blocked_orders": len(outcome.blocked_reasons),
                    },
                )
                self.queue.ack(message)
            except Exception as error:
                stats.last_error = str(error)
                self.telemetry.log(
                    "intelligence",
                    "processing_error",
                    "document processing failed",
                    {"error": str(error)},
                )
                break
        self.telemetry.set_worker(
            "intelligence",
            status="idle",
            current_item="waiting",
            metrics={
                "processed": stats.intelligence_processed,
                "events_generated": stats.events_generated,
                "orders_enqueued": stats.order_tasks_enqueued,
                "blocked_orders": stats.blocked_orders,
            },
        )
        return stats


class DeliveryWorker:
    def __init__(self, store: Store, queue: QueueBackend, delivery: OrderDelivery, telemetry: TelemetryHub) -> None:
        self.store = store
        self.queue = queue
        self.delivery = delivery
        self.telemetry = telemetry

    def run_once(self, max_items: int) -> CycleStats:
        stats = CycleStats()
        self.telemetry.set_worker("delivery", status="running", current_item="waiting for orders")
        for _ in range(max_items):
            message = self.queue.consume(ORDER_QUEUE)
            if message is None:
                break
            try:
                payload = dict(message.payload)
                self.telemetry.set_worker(
                    "delivery",
                    status="running",
                    current_item=f"{payload.get('ticker', '')} {payload.get('side', '')}".strip(),
                )
                intent = OrderIntent(
                    intent_id=payload["intent_id"],
                    event_id=payload["event_id"],
                    ticker=payload["ticker"],
                    side=payload["side"],
                    notional_usd=float(payload["notional_usd"]),
                    confidence=float(payload["confidence"]),
                    reason=payload["reason"],
                    idempotency_key=payload["idempotency_key"],
                    asset_type=str(payload.get("asset_type", "equity")),
                    metadata=dict(payload.get("metadata", {})),
                )
                result = self.delivery.submit(intent)
                self.store.save_order_result(intent, result)
                stats.intents_submitted += 1
                self.telemetry.log(
                    "delivery",
                    "order_emitted",
                    f"{intent.ticker} {intent.side} emitted",
                    {
                        "asset_type": intent.asset_type,
                        "notional_usd": intent.notional_usd,
                        "delivery_name": result.delivery_name,
                        "delivery_order_id": result.delivery_order_id,
                    },
                )
                self.queue.ack(message)
            except Exception as error:
                stats.last_error = str(error)
                self.telemetry.log(
                    "delivery",
                    "delivery_error",
                    "order delivery failed",
                    {"error": str(error)},
                )
                break
        self.telemetry.set_worker(
            "delivery",
            status="idle",
            current_item="waiting",
            metrics={
                "intents_submitted": stats.intents_submitted,
            },
        )
        return stats


class MonitorService:
    def __init__(
        self,
        config: AppConfig,
        store: Store,
        queue: QueueBackend,
        collector: CollectorWorker,
        intelligence: IntelligenceWorker,
        delivery_worker: DeliveryWorker,
        telemetry: TelemetryHub,
    ) -> None:
        self.config = config
        self.store = store
        self.queue = queue
        self.collector = collector
        self.intelligence = intelligence
        self.delivery_worker = delivery_worker
        self.telemetry = telemetry
        self.runtime = MonitorRuntimeState()
        self._loop_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def run_once(self) -> CycleStats:
        self.runtime.running = True
        self.runtime.last_started_at = utcnow().isoformat()
        stats = CycleStats()
        try:
            collector_stats = self.collector.run_once()
            stats = self._merge_stats(stats, collector_stats)
            stats = self._merge_stats(stats, self._drain_documents())
            stats = self._merge_stats(stats, self._drain_orders())
        finally:
            self.runtime.last_cycle = stats
            self.runtime.last_finished_at = utcnow().isoformat()
            self.runtime.running = False
        return stats

    def run_collector_once(self) -> CycleStats:
        return self.collector.run_once()

    def run_intelligence_once(self, max_items: int | None = None) -> CycleStats:
        return self.intelligence.run_once(max_items or self.config.runner.queue_drain_batch_size)

    def run_delivery_once(self, max_items: int | None = None) -> CycleStats:
        return self.delivery_worker.run_once(max_items or self.config.runner.queue_drain_batch_size)

    def ingest_external(self, payload: dict) -> dict[str, str]:
        document = SourceDocument(
            source_name=payload["source_name"],
            url=payload["url"],
            title=payload["title"],
            content=payload.get("content", ""),
            published_at=None,
            host=payload.get("host") or urlparse(payload["url"]).netloc,
            tags=list(payload.get("tags", [])),
            raw_payload=payload,
        )
        is_new = self.store.save_document(document)
        if not is_new:
            self.telemetry.log(
                "collector",
                "duplicate_document",
                "external document ignored as duplicate",
                {"title": document.title, "url": document.url},
            )
            return {"status": "duplicate"}
        self.queue.publish(DOCUMENT_QUEUE, {"document_id": document.document_id})
        self.telemetry.log(
            "collector",
            "external_ingest",
            "external document queued",
            {"title": document.title, "source_name": document.source_name},
        )
        return {"status": "queued", "document_id": document.document_id}

    def start_forever(self) -> None:
        if self._loop_thread and self._loop_thread.is_alive():
            return
        self._stop_event.clear()
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=5)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as error:
                self.runtime.last_cycle.last_error = str(error)
            time.sleep(self.config.runner.poll_interval_seconds)

    def _drain_documents(self) -> CycleStats:
        aggregated = CycleStats()
        while self.queue.size(DOCUMENT_QUEUE) > 0:
            batch = self.intelligence.run_once(self.config.runner.queue_drain_batch_size)
            aggregated = self._merge_stats(aggregated, batch)
            if batch.intelligence_processed == 0:
                break
        return aggregated

    def _drain_orders(self) -> CycleStats:
        aggregated = CycleStats()
        while self.queue.size(ORDER_QUEUE) > 0:
            batch = self.delivery_worker.run_once(self.config.runner.queue_drain_batch_size)
            aggregated = self._merge_stats(aggregated, batch)
            if batch.intents_submitted == 0:
                break
        return aggregated

    def _merge_stats(self, left: CycleStats, right: CycleStats) -> CycleStats:
        merged = CycleStats(
            sources_polled=left.sources_polled + right.sources_polled,
            documents_seen=left.documents_seen + right.documents_seen,
            documents_new=left.documents_new + right.documents_new,
            document_tasks_enqueued=left.document_tasks_enqueued + right.document_tasks_enqueued,
            intelligence_processed=left.intelligence_processed + right.intelligence_processed,
            events_generated=left.events_generated + right.events_generated,
            impact_assessments_generated=left.impact_assessments_generated + right.impact_assessments_generated,
            order_tasks_enqueued=left.order_tasks_enqueued + right.order_tasks_enqueued,
            intents_submitted=left.intents_submitted + right.intents_submitted,
            blocked_orders=left.blocked_orders + right.blocked_orders,
            last_error=right.last_error or left.last_error,
        )
        return merged

    def health(self) -> dict:
        snapshot = self.store.health_snapshot()
        snapshot["queue"] = self.queue.stats()
        snapshot["telemetry"] = self.telemetry.snapshot(limit=25)
        snapshot["runtime"] = {
            "last_started_at": self.runtime.last_started_at,
            "last_finished_at": self.runtime.last_finished_at,
            "running": self.runtime.running,
            "last_cycle": asdict(self.runtime.last_cycle),
        }
        return snapshot

    def dashboard_snapshot(self, activity_limit: int = 80) -> dict:
        return {
            "health": self.health(),
            "events": self.store.list_recent_events(limit=12),
            "orders": self.store.list_recent_orders(limit=12),
            "impact_assessments": self.store.list_recent_impact_assessments(limit=16),
            "registry": self.store.list_company_profiles(limit=24),
            "activity": self.telemetry.snapshot(limit=activity_limit),
        }


def build_monitor_service(config: AppConfig) -> MonitorService:
    store = build_store(config.storage)
    for profile in config.company_registry:
        store.upsert_company_profile(profile)
    for rule in config.impact_graph:
        store.upsert_impact_rule(rule)

    queue = build_queue(config.queue)
    telemetry = TelemetryHub()
    sources = [build_source(source_config) for source_config in config.sources]
    http_client = RobustHttpClient(
        timeout_seconds=config.runner.http_timeout_seconds,
        min_interval_seconds=config.runner.host_min_interval_seconds,
    )
    analyzer = HeuristicAnalyzer(config.market_profile)
    router = CostAwareRouter(
        config.llm,
        store,
        llm_analyzer=None if not config.llm.enabled else OpenAICompatibleLLMAnalyzer(config.llm, store),
    )
    impact_engine = ImpactEngine(store)
    decision_engine = DecisionEngine(config.market_profile, risk_manager=RiskManager(config.risk, store))
    delivery = build_order_delivery(config.delivery)
    telemetry.set_worker("collector", status="idle", current_item="waiting")
    telemetry.set_worker("intelligence", status="idle", current_item="waiting")
    telemetry.set_worker("delivery", status="idle", current_item="waiting")
    collector = CollectorWorker(store=store, queue=queue, sources=sources, http_client=http_client, telemetry=telemetry)
    intelligence = IntelligenceWorker(
        store=store,
        queue=queue,
        analyzer=analyzer,
        router=router,
        impact_engine=impact_engine,
        decision_engine=decision_engine,
        telemetry=telemetry,
    )
    delivery_worker = DeliveryWorker(store=store, queue=queue, delivery=delivery, telemetry=telemetry)
    return MonitorService(
        config=config,
        store=store,
        queue=queue,
        collector=collector,
        intelligence=intelligence,
        delivery_worker=delivery_worker,
        telemetry=telemetry,
    )
