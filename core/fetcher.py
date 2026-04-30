import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog

from core.database import get_engine, raw_artifacts
from sqlalchemy import insert

log = structlog.get_logger()
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_HEADERS = {
    "User-Agent": "MotivatedSellerPipeline/1.0 (real estate research)"
}

_robots_cache = {}


def _get_robots(base_url):
    parsed = urlparse(base_url)
    domain = f"{parsed.scheme}://{parsed.netloc}"
    if domain in _robots_cache:
        return _robots_cache[domain]
    rp = RobotFileParser()
    rp.set_url(f"{domain}/robots.txt")
    try:
        rp.read()
    except Exception:
        pass
    _robots_cache[domain] = rp
    return rp


def _can_fetch(url):
    rp = _get_robots(url)
    ua = DEFAULT_HEADERS["User-Agent"]
    return rp.can_fetch(ua, url)


def _hash_content(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def fetch_url(url: str, headers: dict = None, timeout: int = 30) -> requests.Response:
    hdrs = {**DEFAULT_HEADERS, **(headers or {})}
    if not _can_fetch(url):
        log.warning("fetcher.robots_disallowed", url=url)
        raise PermissionError(f"robots.txt disallows: {url}")
    resp = requests.get(url, headers=hdrs, timeout=timeout)
    resp.raise_for_status()
    return resp


def save_artifact(
    source_id: str,
    source_url: str,
    content: bytes,
    artifact_type: str = "html",
    parser_version: str = "1.0",
    http_status: int = 200,
    engine=None,
) -> Path:
    eng = engine or get_engine()
    content_hash = _hash_content(content)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = f"{source_id}_{ts}.{artifact_type}"
    fpath = RAW_DIR / fname
    fpath.write_bytes(content)

    with eng.begin() as conn:
        conn.execute(
            insert(raw_artifacts).values(
                source_id=source_id,
                source_url=source_url,
                content_hash=content_hash,
                http_status=http_status,
                parser_version=parser_version,
                artifact_path=str(fpath),
                artifact_type=artifact_type,
            )
        )
    log.info("fetcher.saved", source_id=source_id, path=str(fpath))
    return fpath
