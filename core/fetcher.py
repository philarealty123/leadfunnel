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
                                                            _robots_cache[domain] = rp
                                                                    return rp
                                                                        except Exception:
                                                                                return None


                                                                                def is_allowed(url):
                                                                                    rp = _get_robots(url)
                                                                                        if rp is None:
                                                                                                return True
                                                                                                    return rp.can_fetch(DEFAULT_HEADERS["User-Agent"], url)


                                                                                                    @retry(
                                                                                                        stop=stop_after_attempt(4),
                                                                                                            wait=wait_exponential(multiplier=1, min=2, max=30),
                                                                                                                retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
                                                                                                                    reraise=True,
                                                                                                                    )
                                                                                                                    def fetch_html(url, params=None, rate_limit_sec=1.5):
                                                                                                                            if not is_allowed(url):
                                                                                                                                    raise PermissionError(f"robots.txt disallows: {url}")
                                                                                                                                        time.sleep(rate_limit_sec)
                                                                                                                                            resp = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=30)
                                                                                                                                                resp.raise_for_status()
                                                                                                                                                    return resp


                                                                                                                                                    def fetch_json_api(url, params=None, rate_limit_sec=0.5):
                                                                                                                                                        resp = fetch_html(url, params=params, rate_limit_sec=rate_limit_sec)
                                                                                                                                                            return resp.json()


                                                                                                                                                            def fetch_pdf_bytes(url, rate_limit_sec=2.0):
                                                                                                                                                                resp = fetch_html(url, rate_limit_sec=rate_limit_sec)
                                                                                                                                                                    return resp.content


                                                                                                                                                                    def save_artifact(source_id, url, content, artifact_type, parser_version,
                                                                                                                                                                                      http_status=200, engine=None):
                                                                                                                                                                                          eng = engine or get_engine()
                                                                                                                                                                                              if isinstance(content, (list, dict)):
                                                                                                                                                                                                        raw_bytes = json.dumps(content, indent=2).encode("utf-8")
                                                                                                                                                                                                            elif isinstance(content, str):
                                                                                                                                                                                                                    raw_bytes = content.encode("utf-8")
                                                                                                                                                                                                                        else:
                                                                                                                                                                                                                                raw_bytes = content

                                                                                                                                                                                                                                    content_hash = hashlib.sha256(raw_bytes).hexdigest()
                                                                                                                                                                                                                                        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                                                                                                                                                                                                                                            filename = f"{source_id}_{ts}_{content_hash[:8]}.{artifact_type}"
                                                                                                                                                                                                                                                artifact_dir = RAW_DIR / source_id
                                                                                                                                                                                                                                                    artifact_dir.mkdir(parents=True, exist_ok=True)
                                                                                                                                                                                                                                                        artifact_path = artifact_dir / filename
                                                                                                                                                                                                                                                            artifact_path.write_bytes(raw_bytes)

                                                                                                                                                                                                                                                                with eng.begin() as conn:
                                                                                                                                                                                                                                                                        conn.execute(insert(raw_artifacts).values(
                                                                                                                                                                                                                                                                                    source_id=source_id,
                                                                                                                                                                                                                                                                                                source_url=url,
                                                                                                                                                                                                                                                                                                            fetched_at=datetime.now(timezone.utc),
                                                                                                                                                                                                                                                                                                                        content_hash=content_hash,
                                                                                                                                                                                                                                                                                                                                    http_status=http_status,
                                                                                                                                                                                                                                                                                                                                                parser_version=parser_version,
                                                                                                                                                                                                                                                                                                                                                            artifact_path=str(artifact_path),
                                                                                                                                                                                                                                                                                                                                                                        artifact_type=artifact_type,
                                                                                                                                                                                                                                                                        ))
                                                                                                                                                                                                                                                                            return artifact_path
}