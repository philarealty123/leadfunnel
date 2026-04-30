from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import select, update, insert
import structlog

from core.database import get_engine, leads, dedup_log

log = structlog.get_logger()


def _find_existing(conn, record):
    county = record.get("county", "")
    pid = record.get("parcel_id_normalized", "")
    if pid and county:
        row = conn.execute(
            select(leads.c.id).where(
                leads.c.parcel_id_normalized == pid,
                leads.c.county == county,
            )
        ).fetchone()
        if row:
            return row[0], "parcel_id"
    return None, None


def upsert_lead(record: dict, engine=None) -> tuple:
    eng = engine or get_engine()
    now = datetime.now(timezone.utc)

    with eng.begin() as conn:
        existing_id, method = _find_existing(conn, record)

        if existing_id:
            conn.execute(
                update(leads)
                .where(leads.c.id == existing_id)
                .values(
                    last_seen_at=now,
                    motivation_score=record.get("motivation_score"),
                    motivation_category=record.get("motivation_category"),
                    status=record.get("status", "new"),
                )
            )
            log.info("dedupe.updated", lead_id=existing_id, method=method)
            return existing_id, False
        else:
            result = conn.execute(
                insert(leads).values(
                    source_id=record.get("source_id"),
                    source_type=record.get("source_type"),
                    state=record.get("state"),
                    county=record.get("county"),
                    municipality=record.get("municipality"),
                    property_address_raw=record.get("property_address_raw"),
                    property_address_normalized=record.get("property_address_normalized"),
                    owner_name_raw=record.get("owner_name_raw"),
                    owner_name_normalized=record.get("owner_name_normalized"),
                    mailing_address_raw=record.get("mailing_address_raw"),
                    mailing_address_normalized=record.get("mailing_address_normalized"),
                    parcel_id_raw=record.get("parcel_id_raw"),
                    parcel_id_normalized=record.get("parcel_id_normalized"),
                    docket_number=record.get("docket_number"),
                    sale_number=record.get("sale_number"),
                    case_type=record.get("case_type"),
                    filing_date=record.get("filing_date"),
                    sale_date=record.get("sale_date"),
                    tax_years_due=record.get("tax_years_due"),
                    amount_due=record.get("amount_due"),
                    violation_type=record.get("violation_type"),
                    violation_status=record.get("violation_status"),
                    motivation_category=record.get("motivation_category"),
                    motivation_score=record.get("motivation_score"),
                    source_url=record.get("source_url"),
                    first_seen_at=now,
                    last_seen_at=now,
                    status=record.get("status", "new"),
                )
            )
            new_id = result.inserted_primary_key[0]
            log.info("dedupe.inserted", lead_id=new_id)
            return new_id, True
