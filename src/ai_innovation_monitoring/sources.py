from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Iterable
from urllib.parse import urlparse
from xml.etree import ElementTree

from ai_innovation_monitoring.config import SourceConfig
from ai_innovation_monitoring.domain import SourceDocument, SourceState, isoformat, parse_datetime, utcnow
from ai_innovation_monitoring.fetching import (
    RobustHttpClient,
    extract_main_text,
    extract_page_description,
    extract_page_title,
    get_shared_browser_page_client,
    html_to_text,
    looks_like_bot_challenge,
)


ATOM_NAMESPACE = {"atom": "http://www.w3.org/2005/Atom"}
SITEMAP_NAMESPACE = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _lookup_field(payload: Any, field: str) -> Any:
    current = payload
    for part in field.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _lookup_items(payload: Any, path: list[str]) -> Iterable[dict[str, Any]]:
    current = payload
    for part in path:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return []
    if isinstance(current, list):
        return [item for item in current if isinstance(item, dict)]
    return []


def _build_document(config: SourceConfig, item: dict[str, Any]) -> SourceDocument | None:
    title = str(_lookup_field(item, config.field_map.get("title", "title")) or "").strip()
    url = str(_lookup_field(item, config.field_map.get("url", "url")) or "").strip()
    if not title or not url:
        return None
    content_field = config.field_map.get("content", "content")
    content = str(_lookup_field(item, content_field) or "").strip()
    published_field = config.field_map.get("published_at", "published_at")
    published_at = parse_datetime(str(_lookup_field(item, published_field) or ""))
    host = urlparse(url).netloc
    return SourceDocument(
        source_name=config.name,
        url=url,
        title=html_to_text(title),
        content=html_to_text(content),
        published_at=published_at,
        host=host,
        tags=list(config.tags),
        raw_payload=item,
    )


def _slug_to_title(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    return slug.replace("-", " ").replace("_", " ").strip().title()


def _matches_patterns(url: str, include_patterns: list[str], exclude_patterns: list[str]) -> bool:
    if include_patterns and not any(re.search(pattern, url) for pattern in include_patterns):
        return False
    if exclude_patterns and any(re.search(pattern, url) for pattern in exclude_patterns):
        return False
    return True


def _build_html_document(
    config: SourceConfig,
    url: str,
    html: str,
    *,
    visible_text: str = "",
    preferred_title: str = "",
    published_at: Any = None,
    raw_payload: dict[str, Any] | None = None,
) -> SourceDocument:
    title = preferred_title.strip() or extract_page_title(html) or _slug_to_title(url)
    description = extract_page_description(html)
    main_text = visible_text.strip() or extract_main_text(html)
    content_parts = [part for part in (description, main_text) if part]
    content = " ".join(content_parts).strip()
    return SourceDocument(
        source_name=config.name,
        url=url,
        title=title,
        content=content,
        published_at=published_at,
        host=urlparse(url).netloc,
        tags=list(config.tags),
        raw_payload=raw_payload or {},
    )


@dataclass(slots=True)
class SourcePollResult:
    documents: list[SourceDocument]
    etag: str | None = None
    last_modified: str | None = None
    cursor: str | None = None


@dataclass(slots=True)
class SitemapEntry:
    url: str
    lastmod: Any = None


class BaseSource:
    def __init__(self, config: SourceConfig) -> None:
        self.config = config

    def poll(self, client: RobustHttpClient, state: SourceState | None) -> SourcePollResult:
        raise NotImplementedError


class RSSFeedSource(BaseSource):
    def poll(self, client: RobustHttpClient, state: SourceState | None) -> SourcePollResult:
        response = client.get(
            self.config.url,
            headers=self.config.headers,
            etag=state.etag if state else None,
            last_modified=state.last_modified if state else None,
        )
        if response.status_code == 304:
            return SourcePollResult(documents=[], cursor=state.cursor if state else None)
        documents = self._parse_items(response.text)
        return SourcePollResult(
            documents=documents,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            cursor=state.cursor if state else None,
        )

    def _parse_items(self, xml_payload: str) -> list[SourceDocument]:
        root = ElementTree.fromstring(xml_payload)
        documents: list[SourceDocument] = []

        channel = root.find("channel")
        if channel is not None:
            items = channel.findall("item")
            for item in items:
                title = html_to_text(item.findtext("title", default=""))
                link = item.findtext("link", default="").strip()
                if not title or not link:
                    continue
                content = html_to_text(item.findtext("description", default=""))
                published_at = parse_datetime(
                    item.findtext("pubDate", default="")
                    or item.findtext("{http://purl.org/dc/elements/1.1/}date", default="")
                )
                documents.append(
                    SourceDocument(
                        source_name=self.config.name,
                        url=link,
                        title=title,
                        content=content,
                        published_at=published_at,
                        host=urlparse(link).netloc,
                        tags=list(self.config.tags),
                        raw_payload={"xml": ElementTree.tostring(item, encoding="unicode")},
                    )
                )
            return documents

        for item in root.findall("atom:entry", ATOM_NAMESPACE):
            title = html_to_text(item.findtext("atom:title", default="", namespaces=ATOM_NAMESPACE))
            link_element = item.find("atom:link", ATOM_NAMESPACE)
            link = link_element.get("href", "").strip() if link_element is not None else ""
            if not title or not link:
                continue
            content = html_to_text(
                item.findtext("atom:summary", default="", namespaces=ATOM_NAMESPACE)
                or item.findtext("atom:content", default="", namespaces=ATOM_NAMESPACE)
            )
            published_at = parse_datetime(
                item.findtext("atom:updated", default="", namespaces=ATOM_NAMESPACE)
                or item.findtext("atom:published", default="", namespaces=ATOM_NAMESPACE)
            )
            documents.append(
                SourceDocument(
                    source_name=self.config.name,
                    url=link,
                    title=title,
                    content=content,
                    published_at=published_at,
                    host=urlparse(link).netloc,
                    tags=list(self.config.tags),
                    raw_payload={"xml": ElementTree.tostring(item, encoding="unicode")},
                )
            )
        return documents


class JsonApiSource(BaseSource):
    def poll(self, client: RobustHttpClient, state: SourceState | None) -> SourcePollResult:
        response = client.get(
            self.config.url,
            headers=self.config.headers,
            etag=state.etag if state else None,
            last_modified=state.last_modified if state else None,
        )
        if response.status_code == 304:
            return SourcePollResult(documents=[], cursor=state.cursor if state else None)
        payload = json.loads(response.text)
        items = _lookup_items(payload, self.config.item_path)
        documents = [doc for doc in (_build_document(self.config, item) for item in items) if doc is not None]
        return SourcePollResult(
            documents=documents,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            cursor=state.cursor if state else None,
        )


class SitemapSource(BaseSource):
    def poll(self, client: RobustHttpClient, state: SourceState | None) -> SourcePollResult:
        response = client.get(
            self.config.url,
            headers=self.config.headers,
            etag=state.etag if state else None,
            last_modified=state.last_modified if state else None,
        )
        if response.status_code == 304:
            return SourcePollResult(documents=[], cursor=state.cursor if state else None)
        entries = self._parse_sitemap(response.text, client)
        entries = [
            entry
            for entry in entries
            if _matches_patterns(entry.url, self.config.include_url_patterns, self.config.exclude_url_patterns)
        ]
        entries.sort(key=lambda entry: entry.lastmod or utcnow(), reverse=True)

        cursor_dt = parse_datetime(state.cursor) if state and state.cursor else None
        if cursor_dt is None:
            bootstrap_cutoff = utcnow() - timedelta(days=self.config.bootstrap_lookback_days)
            selected_entries = [entry for entry in entries if entry.lastmod is None or entry.lastmod >= bootstrap_cutoff]
        else:
            selected_entries = [entry for entry in entries if entry.lastmod is None or entry.lastmod > cursor_dt]

        selected_entries = selected_entries[: self.config.max_documents_per_poll]
        documents = [
            document
            for document in (self._fetch_article_document(client, entry) for entry in selected_entries)
            if document is not None
        ]
        next_cursor = state.cursor if state else None
        if selected_entries:
            dated_entries = [entry.lastmod for entry in selected_entries if entry.lastmod is not None]
            if dated_entries:
                next_cursor = isoformat(max(dated_entries))
        return SourcePollResult(
            documents=documents,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            cursor=next_cursor,
        )

    def _parse_sitemap(self, xml_payload: str, client: RobustHttpClient) -> list[SitemapEntry]:
        root = ElementTree.fromstring(xml_payload)
        tag_name = root.tag.split("}")[-1]
        if tag_name == "sitemapindex":
            entries: list[SitemapEntry] = []
            for sitemap in root.findall("sm:sitemap", SITEMAP_NAMESPACE):
                nested_url = sitemap.findtext("sm:loc", default="", namespaces=SITEMAP_NAMESPACE).strip()
                if not nested_url:
                    continue
                nested_response = client.get(nested_url, headers=self.config.headers)
                entries.extend(self._parse_sitemap(nested_response.text, client))
            return entries

        entries: list[SitemapEntry] = []
        for item in root.findall("sm:url", SITEMAP_NAMESPACE):
            url = item.findtext("sm:loc", default="", namespaces=SITEMAP_NAMESPACE).strip()
            if not url:
                continue
            lastmod = parse_datetime(item.findtext("sm:lastmod", default="", namespaces=SITEMAP_NAMESPACE))
            entries.append(SitemapEntry(url=url, lastmod=lastmod))
        return entries

    def _fetch_article_document(self, client: RobustHttpClient, entry: SitemapEntry) -> SourceDocument | None:
        mode = self.config.article_fetch.lower()
        if mode == "browser":
            page = get_shared_browser_page_client(
                browser_name=self.config.browser_name,
                timeout_seconds=max(client.timeout_seconds, 30),
            ).render(entry.url)
            return _build_html_document(
                self.config,
                page.url,
                page.html,
                visible_text=page.text,
                preferred_title=page.title,
                published_at=entry.lastmod,
                raw_payload={"url": entry.url, "lastmod": isoformat(entry.lastmod), "fetch_mode": "browser"},
            )

        try:
            response = client.get(entry.url, headers=self.config.headers)
        except Exception:
            if mode != "auto":
                raise
            page = get_shared_browser_page_client(
                browser_name=self.config.browser_name,
                timeout_seconds=max(client.timeout_seconds, 30),
            ).render(entry.url)
            return _build_html_document(
                self.config,
                page.url,
                page.html,
                visible_text=page.text,
                preferred_title=page.title,
                published_at=entry.lastmod,
                raw_payload={"url": entry.url, "lastmod": isoformat(entry.lastmod), "fetch_mode": "browser"},
            )
        if mode == "auto" and looks_like_bot_challenge(response.text):
            page = get_shared_browser_page_client(
                browser_name=self.config.browser_name,
                timeout_seconds=max(client.timeout_seconds, 30),
            ).render(entry.url)
            return _build_html_document(
                self.config,
                page.url,
                page.html,
                visible_text=page.text,
                preferred_title=page.title,
                published_at=entry.lastmod,
                raw_payload={"url": entry.url, "lastmod": isoformat(entry.lastmod), "fetch_mode": "browser"},
            )
        return _build_html_document(
            self.config,
            entry.url,
            response.text,
            published_at=entry.lastmod,
            raw_payload={"url": entry.url, "lastmod": isoformat(entry.lastmod), "fetch_mode": "http"},
        )


def build_source(config: SourceConfig) -> BaseSource:
    if config.kind == "rss":
        return RSSFeedSource(config)
    if config.kind == "json_api":
        return JsonApiSource(config)
    if config.kind == "sitemap":
        return SitemapSource(config)
    raise ValueError(f"Unsupported source kind: {config.kind}")
