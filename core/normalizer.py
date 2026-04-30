import re
from typing import Optional

try:
    import usaddress
    HAS_USADDRESS = True
except ImportError:
    HAS_USADDRESS = False

try:
    from nameparser import HumanName
    HAS_NAMEPARSER = True
except ImportError:
    HAS_NAMEPARSER = False

import structlog
log = structlog.get_logger()

_STATE_ABBREVS = {"pennsylvania": "PA", "new jersey": "NJ"}
_ENTITY_SUFFIXES = re.compile(
    r"\b(LLC|LP|INC|CORP|TRUST|ESTATE|ETUX|ETAL|ET UX|ET AL|LTD|CO|COMPANY|ASSOC)\b",
    re.IGNORECASE,
)


def normalize_address(raw):
    if not raw:
        return ""
    cleaned = re.sub(r"\s+", " ", raw.strip().upper())
    if HAS_USADDRESS:
        try:
            tagged, _ = usaddress.tag(cleaned)
            parts = []
            for key in ["AddressNumber", "StreetNamePreDirectional", "StreetName",
                        "StreetNamePostType", "StreetNamePostDirectional",
                        "OccupancyType", "OccupancyIdentifier",
                        "PlaceName", "StateName", "ZipCode"]:
                if key in tagged:
                    parts.append(tagged[key])
            result = " ".join(parts).strip()
            return result if result else cleaned
        except Exception:
            pass
    return cleaned


def normalize_owner_name(raw):
    if not raw:
        return ""
    cleaned = raw.strip().upper()
    cleaned = _ENTITY_SUFFIXES.sub("", cleaned).strip()
    cleaned = re.sub(r"[^A-Z0-9 ]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if HAS_NAMEPARSER:
        try:
            name = HumanName(cleaned)
            return str(name).upper().strip()
        except Exception:
            pass
    return cleaned


def normalize_parcel_id(raw):
    if not raw:
        return ""
    return re.sub(r"[^A-Z0-9]", "", raw.upper())


def normalize_state(raw):
    if not raw:
        return ""
    key = raw.strip().lower()
    return _STATE_ABBREVS.get(key, raw.strip().upper()[:2])
