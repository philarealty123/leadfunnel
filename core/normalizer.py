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
                                                    if not HAS_USADDRESS:
                                                            return cleaned
                                                                try:
                                                                        tagged, _ = usaddress.tag(cleaned)
                                                                                parts = []
                                                                                        for key in ["AddressNumber", "StreetNamePreDirectional", "StreetName",
                                                                                                            "StreetNamePostType", "StreetNamePostDirectional",
                                                                                                                                "OccupancyType", "OccupancyIdentifier",
                                                                                                                                                    "PlaceName", "StateName", "ZipCode"]:
                                                                                                                                                                val = tagged.get(key, "").strip()
                                                                                                                                                                            if val:
                                                                                                                                                                                            parts.append(_STATE_ABBREVS.get(val.lower(), val))
                                                                                                                                                                                                    return " ".join(parts)
                                                                                                                                                                                                        except Exception:
                                                                                                                                                                                                                return cleaned
                                                                                                                                                                                                                
                                                                                                                                                                                                                
                                                                                                                                                                                                                def normalize_parcel_id(raw, county=""):
                                                                                                                                                                                                                    if not raw:
                                                                                                                                                                                                                            return ""
                                                                                                                                                                                                                                return re.sub(r"[^\w\-]", "", raw.strip()).upper()
                                                                                                                                                                                                                                
                                                                                                                                                                                                                                
                                                                                                                                                                                                                                def normalize_owner_name(raw):
                                                                                                                                                                                                                                        if not raw:
                                                                                                                                                                                                                                                return ""
                                                                                                                                                                                                                                                    cleaned = re.sub(r"\s+", " ", raw.strip().upper())
                                                                                                                                                                                                                                                        if _ENTITY_SUFFIXES.search(cleaned):
                                                                                                                                                                                                                                                                return cleaned
                                                                                                                                                                                                                                                                    if HAS_NAMEPARSER:
                                                                                                                                                                                                                                                                            try:
                                                                                                                                                                                                                                                                                        name = HumanName(cleaned)
                                                                                                                                                                                                                                                                                                    parts = [p for p in [name.last, name.first, name.middle] if p]
                                                                                                                                                                                                                                                                                                                return " ".join(p.upper() for p in parts)
                                                                                                                                                                                                                                                                                                                        except Exception:
                                                                                                                                                                                                                                                                                                                                    pass
                                                                                                                                                                                                                                                                                                                                        return cleaned]