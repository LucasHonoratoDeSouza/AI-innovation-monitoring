from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ai_innovation_monitoring.config import DeliveryConfig
from ai_innovation_monitoring.domain import OrderIntent, OrderResult, stable_hash


class OrderDelivery:
    name = "delivery"

    def submit(self, intent: OrderIntent) -> OrderResult:
        raise NotImplementedError


@dataclass(slots=True)
class OutboxOrderDelivery(OrderDelivery):
    outbox_path: Path
    name: str = "outbox"

    def __post_init__(self) -> None:
        self.outbox_path.parent.mkdir(parents=True, exist_ok=True)

    def submit(self, intent: OrderIntent) -> OrderResult:
        reference = stable_hash(intent.intent_id, str(self.outbox_path))[:24]
        envelope = {
            "reference": reference,
            "status": "emitted",
            "intent": intent.to_dict(),
        }
        with self.outbox_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(envelope, ensure_ascii=True) + "\n")
        return OrderResult(
            intent_id=intent.intent_id,
            delivery_name=self.name,
            delivery_order_id=reference,
            status="emitted",
            raw_response={"outbox_path": str(self.outbox_path), "reference": reference},
        )


def build_order_delivery(config: DeliveryConfig) -> OrderDelivery:
    if config.kind != "outbox":
        raise ValueError(f"Unsupported delivery kind: {config.kind}")
    return OutboxOrderDelivery(outbox_path=Path(config.outbox_path).resolve())
