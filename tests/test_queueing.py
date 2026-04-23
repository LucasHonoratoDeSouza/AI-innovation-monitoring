from ai_innovation_monitoring.queueing import SQLiteQueue


def test_sqlite_queue_roundtrip(tmp_path):
    queue = SQLiteQueue(tmp_path / "queue.db", reclaim_timeout_seconds=0)
    queue.publish("documents", {"document_id": "doc-1"})
    assert queue.size("documents") == 1
    message = queue.consume("documents")
    assert message is not None
    assert message.payload["document_id"] == "doc-1"
    queue.ack(message)
    assert queue.size("documents") == 0
