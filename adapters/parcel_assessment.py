import math
import urllib.parse
from datetime import datetime, timedelta

from adapters.base import BaseAdapter
from core.fetcher import fetch_json_api, save_artifact
from core.normalizer import normalize_address, normalize_parcel_id, normalize_owner_name
from core.scoring import compute_score

PARSER_VERSION = "1.0.0"
PAGE_SIZE = 1000
BASE_URL = "https://data.phila.gov/resource/w7rb-qrn8.json"
LOOKBACK_DAYS = 30


def _cutoff_date():
    """Return ISO date string for LOOKBACK_DAYS ago."""
    return (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT00:00:00")


class ParcelAssessmentAdapter(BaseAdapter):
    VERSION = PARSER_VERSION

    def discover(self):
        cutoff = _cutoff_date()
        try:
            url = (
                f"{BASE_URL}"
                f"?$select=count(*)"
                f"&$where=category_code='1' AND sale_date>='{cutoff}'"
            )
            count_data = fetch_json_api(url)
            total = int(count_data[0].get("count", 0))
        except Exception:
            total = 5_000
        return [str(o) for o in range(0, total + PAGE_SIZE, PAGE_SIZE)]

    def fetch(self, offset_str):
        cutoff = _cutoff_date()
        url = (
            f"{BASE_URL}"
            f"?$limit={PAGE_SIZE}"
            f"&$offset={int(offset_str)}"
            f"&$where=category_code='1' AND sale_date>='{cutoff}'"
            f"&$order=sale_date DESC"
        )
        data = fetch_json_api(url)
        if offset_str == "0":
            save_artifact(
                self.source_id, BASE_URL, data[:50], "json",
                self.VERSION, engine=self.engine,
            )
        return data

    def parse(self, raw):
        results = []
        for rec in raw:
            prop_addr = (rec.get("location") or "").strip().upper()
            mailing = ", ".join(
                p.strip() for p in [
                    rec.get("mailing_address", ""),
                    rec.get("mailing_address_2", ""),
                    rec.get("mailing_city", ""),
                    rec.get("mailing_state", ""),
                    rec.get("mailing_zip", ""),
                ] if p and p.strip()
            )
            if not prop_addr or not mailing:
                continue
            if prop_addr in mailing.upper():
                continue
            owner = " / ".join(
                p.strip() for p in [rec.get("owner_1", ""), rec.get("owner_2", "")]
                if p and p.strip()
            )
            results.append({
                "parcel_id_raw": rec.get("parcel_number", "").strip(),
                "property_address_raw": prop_addr,
                "owner_name_raw": owner,
                "mailing_address_raw": mailing,
                "sale_date": (rec.get("sale_date") or "")[:10],
                "source_url": (
                    f"https://property.phila.gov/?p="
                    f"{rec.get('parcel_number', '').strip()}"
                ),
            })
        return results

    def normalize(self, raw):
        score, priority = compute_score({"source_type": "absentee"})
        return {
            "source_id": self.source_id,
            "source_type": "absentee",
            "state": "PA",
            "county": "Philadelphia",
            "property_address_raw": raw["property_address_raw"],
            "property_address_normalized": normalize_address(raw["property_address_raw"]),
            "owner_name_raw": raw["owner_name_raw"],
            "owner_name_normalized": normalize_owner_name(raw["owner_name_raw"]),
            "mailing_address_raw": raw["mailing_address_raw"],
            "mailing_address_normalized": normalize_address(raw["mailing_address_raw"]),
            "parcel_id_raw": raw["parcel_id_raw"],
            "parcel_id_normalized": normalize_parcel_id(
                raw["parcel_id_raw"], "Philadelphia"
            ),
            "sale_date": raw.get("sale_date", ""),
            "motivation_category": "Absentee Owner",
            "motivation_score": score,
            "motivation_score_label": priority,
            "source_url": raw["source_url"],
        }
