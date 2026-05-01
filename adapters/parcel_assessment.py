import math

from adapters.base import BaseAdapter
from core.fetcher import fetch_json_api, save_artifact
from core.normalizer import normalize_address, normalize_parcel_id, normalize_owner_name
from core.scoring import compute_score

PARSER_VERSION = "1.0.0"
PAGE_SIZE = 1000
BASE_URL = "https://data.phila.gov/resource/w7rb-qrn8.json"
FILTER = "category_code='1'"


class ParcelAssessmentAdapter(BaseAdapter):
    VERSION = PARSER_VERSION

    def discover(self):
        try:
            count_data = fetch_json_api(
                f"{BASE_URL}?$select=count(*)&$where={FILTER}"
            )
            total = int(count_data[0].get("count", 0))
        except Exception:
            total = 600_000
        return [str(o) for o in range(0, total + PAGE_SIZE, PAGE_SIZE)]

    def fetch(self, offset_str):
        params = {
            "$limit":  PAGE_SIZE,
            "$offset": int(offset_str),
            "$where":  FILTER,
        }
        data = fetch_json_api(BASE_URL, params=params)
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
                "parcel_id_raw":        rec.get("parcel_number", "").strip(),
                "property_address_raw": prop_addr,
                "owner_name_raw":       owner,
                "mailing_address_raw":  mailing,
                "source_url": (
                    f"https://property.phila.gov/?p="
                    f"{rec.get('parcel_number', '').strip()}"
                ),
            })
        return results

    def normalize(self, raw):
        score, priority = compute_score({"source_type": "absentee"})
        return {
            "source_id":                   self.source_id,
            "source_type":                 "absentee",
            "state":                       "PA",
            "county":                      "Philadelphia",
            "property_address_raw":        raw["property_address_raw"],
            "property_address_normalized": normalize_address(raw["property_address_raw"]),
            "owner_name_raw":              raw["owner_name_raw"],
            "owner_name_normalized":       normalize_owner_name(raw["owner_name_raw"]),
            "mailing_address_raw":         raw["mailing_address_raw"],
            "mailing_address_normalized":  normalize_address(raw["mailing_address_raw"]),
            "parcel_id_raw":               raw["parcel_id_raw"],
            "parcel_id_normalized":        normalize_parcel_id(
                raw["parcel_id_raw"], "Philadelphia"
            ),
            "motivation_category":         "Absentee Owner",
            "motivation_score":            score,
            "motivation_score_label":      priority,
            "source_url":                  raw["source_url"],
        }
