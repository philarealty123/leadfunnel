import urllib.parse
from datetime import datetime, timedelta

from adapters.base import BaseAdapter
from core.fetcher import fetch_json_api, save_artifact
from core.normalizer import normalize_address, normalize_parcel_id, normalize_owner_name
from core.scoring import compute_score

PARSER_VERSION = "1.0.0"
PAGE_SIZE = 1000
CARTO_SQL_URL = "https://phl.carto.com/api/v2/sql"
TABLE = "violations"
LOOKBACK_DAYS = 30


def _cutoff_date():
    """Return ISO date string for LOOKBACK_DAYS ago."""
    return (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")


class PhillyLiViolationsAdapter(BaseAdapter):
    VERSION = PARSER_VERSION

    def discover(self):
        cutoff = _cutoff_date()
        try:
            sql = (
                f"SELECT COUNT(*) as cnt FROM {TABLE}"
                f" WHERE violationstatus = 'Open'"
                f" AND violationdate >= '{cutoff}'"
            )
            url = CARTO_SQL_URL + "?q=" + urllib.parse.quote(sql)
            resp = fetch_json_api(url)
            total = int(resp["rows"][0].get("cnt", 0))
        except Exception:
            total = 5_000
        return [str(o) for o in range(0, total + PAGE_SIZE, PAGE_SIZE)]

    def fetch(self, offset_str):
        cutoff = _cutoff_date()
        offset = int(offset_str)
        sql = (
            f"SELECT * FROM {TABLE}"
            f" WHERE violationstatus = 'Open'"
            f" AND violationdate >= '{cutoff}'"
            f" ORDER BY violationdate DESC"
            f" LIMIT {PAGE_SIZE} OFFSET {offset}"
        )
        url = CARTO_SQL_URL + "?q=" + urllib.parse.quote(sql)
        resp = fetch_json_api(url)
        data = resp.get("rows", [])
        if offset_str == "0":
            save_artifact(
                self.source_id, CARTO_SQL_URL, data, "json",
                self.VERSION, engine=self.engine,
            )
        return data

    def parse(self, raw):
        results = []
        for rec in raw:
            if not rec.get("violationaddress"):
                continue
            mailing = ", ".join(
                p.strip() for p in [
                    rec.get("owneraddress", ""),
                    rec.get("ownercity", ""),
                    rec.get("ownerstate", ""),
                    rec.get("ownerzip", ""),
                ] if p and p.strip()
            )
            results.append({
                "parcel_id_raw": rec.get("parcel_id_num", "").strip(),
                "property_address_raw": rec.get("violationaddress", "").strip(),
                "owner_name_raw": rec.get("ownername", "").strip(),
                "mailing_address_raw": mailing,
                "docket_number": rec.get("casenumber", "").strip(),
                "case_type": rec.get("casetype", "").strip(),
                "violation_type": rec.get("violationdescription", "").strip(),
                "violation_status": rec.get("violationstatus", "").strip(),
                "filing_date": (rec.get("violationdate") or "")[:10],
                "source_url": (
                    f"https://li.phila.gov/Property-History/pi/?key="
                    f"{rec.get('parcel_id_num', '').strip()}"
                ),
            })
        return results

    def normalize(self, raw):
        score, priority = compute_score({"source_type": "code_violation"})
        return {
            "source_id": self.source_id,
            "source_type": "code_violation",
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
            "docket_number": raw["docket_number"],
            "case_type": raw["case_type"],
            "violation_type": raw["violation_type"],
            "violation_status": raw["violation_status"],
            "filing_date": raw["filing_date"],
            "motivation_category": "Code Violation",
            "motivation_score": score,
            "motivation_score_label": priority,
            "source_url": raw["source_url"],
        }
