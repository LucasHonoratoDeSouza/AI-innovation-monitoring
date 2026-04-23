from ai_innovation_monitoring.config import SourceConfig
from ai_innovation_monitoring.domain import SourceState, parse_datetime
from ai_innovation_monitoring.fetching import BrowserPage, HttpResponse
from ai_innovation_monitoring.sources import SitemapSource


class FakeHttpClient:
    def __init__(self, payloads: dict[str, str]) -> None:
        self.payloads = payloads
        self.timeout_seconds = 15

    def get(self, url: str, *, headers=None, etag=None, last_modified=None):  # noqa: ANN001
        return HttpResponse(status_code=200, text=self.payloads[url], headers={})


def test_sitemap_source_fetches_recent_filtered_articles():
    sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://blog.google/innovation-and-ai/technology/ai/lyria-3-pro/</loc>
        <lastmod>2026-04-20T12:00:00Z</lastmod>
      </url>
      <url>
        <loc>https://blog.google/company-info/</loc>
        <lastmod>2026-04-20T12:00:00Z</lastmod>
      </url>
      <url>
        <loc>https://blog.google/innovation-and-ai/technology/ai/google-ai-updates-march-2026/</loc>
        <lastmod>2025-01-01T00:00:00Z</lastmod>
      </url>
    </urlset>
    """
    article_html = """
    <html>
      <head>
        <title>Lyria 3 Pro: Create longer tracks in more Google products</title>
        <meta name="description" content="We are bringing Lyria 3 to the tools where professionals work and create every day." />
      </head>
      <body>
        <main><p>Google is expanding Lyria 3 Pro across creator tools.</p></main>
      </body>
    </html>
    """
    client = FakeHttpClient(
        {
            "https://blog.google/sitemap.xml": sitemap_xml,
            "https://blog.google/innovation-and-ai/technology/ai/lyria-3-pro/": article_html,
        }
    )
    source = SitemapSource(
        SourceConfig(
            name="google-blog-sitemap",
            kind="sitemap",
            url="https://blog.google/sitemap.xml",
            include_url_patterns=[r"/innovation-and-ai/"],
            bootstrap_lookback_days=30,
            article_fetch="http",
        )
    )

    result = source.poll(client, None)

    assert len(result.documents) == 1
    assert result.documents[0].title == "Lyria 3 Pro: Create longer tracks in more Google products"
    assert "tools where professionals work" in result.documents[0].content
    assert "creator tools" in result.documents[0].content
    assert result.cursor == "2026-04-20T12:00:00+00:00"


def test_sitemap_source_respects_cursor_for_incremental_fetch():
    sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://blog.google/innovation-and-ai/models-and-research/gemini-models/gemini-3-1-flash-live/</loc>
        <lastmod>2026-04-22T10:00:00Z</lastmod>
      </url>
      <url>
        <loc>https://blog.google/innovation-and-ai/technology/ai/google-ai-updates-march-2026/</loc>
        <lastmod>2026-04-20T10:00:00Z</lastmod>
      </url>
    </urlset>
    """
    article_html = (
        "<html><head><title>Gemini 3.1 Flash Live: Making audio AI more natural and reliable</title></head>"
        "<body><main>Latest body</main></body></html>"
    )
    client = FakeHttpClient(
        {
            "https://blog.google/sitemap.xml": sitemap_xml,
            "https://blog.google/innovation-and-ai/models-and-research/gemini-models/gemini-3-1-flash-live/": article_html,
        }
    )
    source = SitemapSource(
        SourceConfig(
            name="google-blog-sitemap",
            kind="sitemap",
            url="https://blog.google/sitemap.xml",
            include_url_patterns=[r"/innovation-and-ai/"],
            article_fetch="http",
        )
    )
    state = SourceState(source_name="google-blog-sitemap", cursor="2026-04-21T00:00:00+00:00")

    result = source.poll(client, state)

    assert [document.url for document in result.documents] == [
        "https://blog.google/innovation-and-ai/models-and-research/gemini-models/gemini-3-1-flash-live/"
    ]
    assert result.cursor == "2026-04-22T10:00:00+00:00"


def test_sitemap_source_uses_browser_fallback_when_http_is_blocked(monkeypatch):
    sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://openai.com/index/introducing-openai</loc>
        <lastmod>2026-04-22T10:00:00Z</lastmod>
      </url>
    </urlset>
    """
    blocked_html = "<html><body>Enable JavaScript and cookies to continue</body></html>"
    client = FakeHttpClient(
        {
            "https://openai.com/sitemap.xml/product/": sitemap_xml,
            "https://openai.com/index/introducing-openai": blocked_html,
        }
    )

    class FakeBrowserClient:
        def render(self, url: str) -> BrowserPage:
            return BrowserPage(
                url=url,
                title="Introducing OpenAI",
                html="<html><body><main>Launch details from browser render.</main></body></html>",
                text="Launch details from browser render.",
            )

    monkeypatch.setattr("ai_innovation_monitoring.sources.get_shared_browser_page_client", lambda **kwargs: FakeBrowserClient())

    source = SitemapSource(
        SourceConfig(
            name="openai-product",
            kind="sitemap",
            url="https://openai.com/sitemap.xml/product/",
            include_url_patterns=[r"https://openai\.com/index/"],
            article_fetch="auto",
        )
    )

    result = source.poll(client, SourceState(source_name="openai-product", cursor=None))

    assert len(result.documents) == 1
    assert result.documents[0].title == "Introducing OpenAI"
    assert "browser render" in result.documents[0].content
    assert parse_datetime(result.cursor) == parse_datetime("2026-04-22T10:00:00Z")
