import math
from adapters.base import BaseAdapter
from core.fetcher import fetch_json_api, save_artifact
from core.normalizer import normalize_address, normalize_parcel_id, normalize_owner_name
from core.scoring import compute_score

PARSER_VERSION = "1.0.0"
PAGE_SIZE = 1000
BASE_URL = "https://data.phila.gov/resource/dc-n4j4.json"
OPEN_FILTER = "violationstatus='Open'"


class PhillyLiViolationsAdapter(BaseAdapter):
    VERSION = PARSER_VERSION

    def discover(self):
        try:
            count_data = fetch_json_api(f"{BASE_URL}?$select=count(*)&$where={OPEN_FILTER}")
            total = int(count_data[0].get("count", 0))
        except Exception:
            total = 50_000
        return [str(o) for o in range(0, total + PAGE_SIZE, PAGE_SIZE)]

    def fetch(self, offset_str):
        params = {
            "$limit": PAGE_SIZE,
            "$offset": int(offset_str),
            "$where": OPEN_FILTER,
            "$order": "casecreateddate DESC",
        }
        data = fetch_json_api(BASE_URL, params=params)
        if offset_str == "0":
            save_artifact(self.source_id, BASE_URL, data, "json", self.VERSION, engine=self.engine)
        return data

    def parse(self, raw):
        results = []
        for rec in raw:
            if not rec.get("violationaddress"):
                continue
            mailing = ", ".join(p.strip() for p in [
                rec.get("owneraddress", ""),
                rec.get("ownercity", ""),
                rec.get("ownerstate", ""),
                rec.get("ownerzip", ""),
            ] if p and p.strip())
            results.append({
                "parcel_id_raw": rec.get("parcel_id_num", "").strip(),
                "property_address_raw": rec.get("violationaddress", "").strip(),
                "owner_name_raw": rec.get("ownername", "").strip(),
                "mailing_address_raw": mailing,
                "docket_number": rec.get("casenumber", "").strip(),
                "case_type": rec.get("casetype", "").strip(),
                "violation_type": rec.get("violationdescription", "").strip(),
                "violation_status": rec.get("violationstatus", "").strip(),
                "filing_date": (rec.get("casecreateddate") or "")[:10],
                "source_url": f"https://li.phila.gov/Property-History/pi/?key={rec.get('parcel_id_num', '').strip()}",
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
            "parcel_id_normalized": normalize_parcel_id(raw["parcel_id_raw"], "Philadelphia"),
            "docket_number": raw["docket_number"],
            "case_type": raw["case_type"],
            "violation_type": raw["violation_type"],
            "violation_status": raw["violation_status"],
            "filing_date": raw["filing_date"],
            "motivation_category": "code_violation",
            "motivation_score": score,
            "source_url": raw["source_url"],
        }
