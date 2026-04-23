from ai_innovation_monitoring.domain import SourceDocument
from ai_innovation_monitoring.storage import SQLiteStore


def test_store_deduplicates_documents(tmp_path):
    store = SQLiteStore(tmp_path / "test.db")
    document = SourceDocument(
        source_name="feed",
        url="https://openai.com/index/introducing-openai",
        title="Introducing OpenAI",
        content="Body",
    )
    assert store.save_document(document) is True
    assert store.save_document(document) is False
