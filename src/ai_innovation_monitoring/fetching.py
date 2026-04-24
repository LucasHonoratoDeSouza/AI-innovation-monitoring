from __future__ import annotations

import atexit
import json
import random
import re
import time
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_ARTICLE_RE = re.compile(r"<article\b[^>]*>(.*?)</article>", re.IGNORECASE | re.DOTALL)
_MAIN_RE = re.compile(r"<main\b[^>]*>(.*?)</main>", re.IGNORECASE | re.DOTALL)
_BODY_RE = re.compile(r"<body\b[^>]*>(.*?)</body>", re.IGNORECASE | re.DOTALL)
_META_RE = re.compile(
    r"<meta\b[^>]*(?:name|property)\s*=\s*[\"'](?P<key>[^\"']+)[\"'][^>]*content\s*=\s*[\"'](?P<content>[^\"']*)[\"'][^>]*>",
    re.IGNORECASE,
)
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)


def html_to_text(value: str) -> str:
    if not value:
        return ""
    text = unescape(value)
    text = _TAG_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()

# Heuristic to detect if a page is showing a bot challenge (like Cloudflare's anti-bot page) instead of the real content.   
def looks_like_bot_challenge(value: str) -> bool:
    lowered = value.lower()
    challenge_markers = (
        "enable javascript and cookies to continue",
        "cdn-cgi/challenge-platform",
        "__cf_chl_",
        "cf_chl_opt",
        "just a moment",
    )
    return any(marker in lowered for marker in challenge_markers)


def extract_page_title(html: str) -> str:
    for match in _META_RE.finditer(html):
        key = match.group("key").strip().lower()
        if key in {"og:title", "twitter:title"}:
            return html_to_text(match.group("content"))
    title_match = _TITLE_RE.search(html)
    if title_match:
        return html_to_text(title_match.group(1))
    return ""


def extract_page_description(html: str) -> str:
    for match in _META_RE.finditer(html):
        key = match.group("key").strip().lower()
        if key in {"description", "og:description", "twitter:description"}:
            return html_to_text(match.group("content"))
    return ""


def extract_main_text(html: str, max_chars: int = 8000) -> str:
    cleaned = _SCRIPT_STYLE_RE.sub(" ", html)
    for pattern in (_ARTICLE_RE, _MAIN_RE, _BODY_RE):
        match = pattern.search(cleaned)
        if match:
            text = html_to_text(match.group(1))
            if text:
                return text[:max_chars]
    return html_to_text(cleaned)[:max_chars]


@dataclass(slots=True)
class HttpResponse:
    status_code: int
    text: str
    headers: dict[str, str]

    def json(self) -> Any:
        return json.loads(self.text)


@dataclass(slots=True)
class BrowserPage:
    url: str
    title: str
    html: str
    text: str


class RobustHttpClient:
    def __init__(self, timeout_seconds: int = 15, min_interval_seconds: float = 1.5) -> None:
        self.timeout_seconds = timeout_seconds
        self.min_interval_seconds = min_interval_seconds
        self._host_last_request: dict[str, float] = {}

    def _wait_for_slot(self, url: str) -> None:
        host = urlparse(url).netloc
        last_request = self._host_last_request.get(host, 0.0)
        elapsed = time.monotonic() - last_request
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._host_last_request[host] = time.monotonic()

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> HttpResponse:
        merged_headers = {
            "User-Agent": "AI-Innovation-Monitor/0.1 (+https://localhost)",
            "Accept": "*/*",
        }
        if headers:
            merged_headers.update(headers)
        if etag:
            merged_headers["If-None-Match"] = etag
        if last_modified:
            merged_headers["If-Modified-Since"] = last_modified

        self._wait_for_slot(url)
        attempts = 0
        while attempts < 4:
            attempts += 1
            request = Request(url, headers=merged_headers, method="GET")
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8", errors="replace")
                    return HttpResponse(
                        status_code=response.getcode(),
                        text=body,
                        headers={key.lower(): value for key, value in response.headers.items()},
                    )
            except HTTPError as error:
                status_code = error.code
                if status_code == 304:
                    return HttpResponse(status_code=304, text="", headers=dict(error.headers.items()))
                if status_code in {429, 500, 502, 503, 504} and attempts < 4:
                    time.sleep((2**attempts) + random.random())
                    continue
                raise
            except URLError:
                if attempts < 4:
                    time.sleep((2**attempts) + random.random())
                    continue
                raise


class BrowserPageClient:
    def __init__(self, browser_name: str = "firefox", timeout_seconds: int = 45) -> None:
        self.browser_name = browser_name
        self.timeout_seconds = timeout_seconds
        self._driver = None

    def _ensure_driver(self) -> Any:
        if self._driver is not None:
            return self._driver
        from selenium import webdriver

        browser = self.browser_name.lower()
        if browser == "firefox":
            from selenium.webdriver.firefox.options import Options as FirefoxOptions

            options = FirefoxOptions()
            options.add_argument("-headless")
            self._driver = webdriver.Firefox(options=options)
        elif browser in {"chrome", "chromium"}:
            from selenium.webdriver.chrome.options import Options as ChromeOptions

            options = ChromeOptions()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            self._driver = webdriver.Chrome(options=options)
        else:
            raise ValueError(f"Unsupported browser for source fetching: {self.browser_name}")
        self._driver.set_page_load_timeout(self.timeout_seconds)
        return self._driver

    def render(self, url: str) -> BrowserPage:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait

        driver = self._ensure_driver()
        driver.get(url)
        WebDriverWait(driver, self.timeout_seconds).until(
            lambda current_driver: current_driver.execute_script("return document.readyState") == "complete"
        )
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            html = driver.page_source
            body_text = driver.find_element(By.TAG_NAME, "body").text.strip()
            if body_text and not looks_like_bot_challenge(f"{html}\n{body_text}"):
                return BrowserPage(url=driver.current_url, title=driver.title.strip(), html=html, text=body_text)
            if time.monotonic() >= deadline:
                return BrowserPage(url=driver.current_url, title=driver.title.strip(), html=html, text=body_text)
            time.sleep(1)

    def close(self) -> None:
        if self._driver is None:
            return
        self._driver.quit()
        self._driver = None


_BROWSER_CLIENTS: dict[tuple[str, int], BrowserPageClient] = {}


def get_shared_browser_page_client(browser_name: str = "firefox", timeout_seconds: int = 45) -> BrowserPageClient:
    key = (browser_name.lower(), int(timeout_seconds))
    client = _BROWSER_CLIENTS.get(key)
    if client is None:
        client = BrowserPageClient(browser_name=browser_name, timeout_seconds=timeout_seconds)
        _BROWSER_CLIENTS[key] = client
    return client


def _close_browser_clients() -> None:
    for client in _BROWSER_CLIENTS.values():
        client.close()


atexit.register(_close_browser_clients)
