import os
from datetime import datetime, timezone
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import select, update
import structlog

from core.database import get_engine, leads, sheet_pushes

log = structlog.get_logger()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DAILY_REVIEW_HEADERS = [
    "Review Status",
    "Priority",
    "Motivation Category",
    "Motivation Score",
    "Property Address",
    "Owner",
    "Mailing Address",
    "County",
    "State",
    "Parcel ID",
    "Source",
    "Source Type",
    "Source Link",
    "Sale Date",
    "Filing Date",
    "Amount Due",
    "Violation Type",
    "Docket Number",
    "Notes",
    "Outreach Status",
    "Date Added",
    "First Seen",
    "Lead ID",
]


def _get_service(credentials_path=None):
    import json
    creds_path = credentials_path or os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_path:
        raise EnvironmentError("Set GOOGLE_CREDENTIALS_JSON env var.")
    with open(creds_path) as f:
        info = json.load(f)
    # Force universe_domain to the plain string googleapis.com regardless of what's in the JSON
    info["universe_domain"] = "googleapis.com"
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )
    # Explicitly set universe_domain on the credentials object too
    creds._universe_domain = "googleapis.com"
    return build("sheets", "v4", credentials=creds)


def _build_row(lead, uploaded_at):
    return [
        "",
        lead.get("motivation_score_label", ""),
        lead.get("motivation_category", ""),
        str(lead.get("motivation_score", "")),
        lead.get("property_address_normalized") or lead.get("property_address_raw", ""),
        lead.get("owner_name_normalized") or lead.get("owner_name_raw", ""),
        lead.get("mailing_address_normalized") or lead.get("mailing_address_raw", ""),
        lead.get("county", ""),
        lead.get("state", ""),
        lead.get("parcel_id_normalized") or lead.get("parcel_id_raw", ""),
        lead.get("source_id", ""),
        lead.get("source_type", ""),
        lead.get("source_url", ""),
        lead.get("sale_date", ""),
        lead.get("filing_date", ""),
        str(lead.get("amount_due", "") or ""),
        lead.get("violation_type", ""),
        lead.get("docket_number", ""),
        "",
        "",
        uploaded_at.strftime("%Y-%m-%d %H:%M UTC"),
        str(lead.get("first_seen_at", "")),
        str(lead.get("id", "")),
    ]


def ensure_header_row(service, spreadsheet_id, tab_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A1:A1"
    ).execute()
    if not result.get("values"):
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [DAILY_REVIEW_HEADERS]},
        ).execute()


def push_leads_to_sheet(
    spreadsheet_id,
    tab_name="Daily_Review",
    credentials_path=None,
    engine=None,
    limit=500,
):
    eng = engine or get_engine()
    service = _get_service(credentials_path)
    now = datetime.now(timezone.utc)
    with eng.connect() as conn:
        rows = conn.execute(
            select(leads)
            .where(leads.c.date_uploaded == None)
            .where(leads.c.status == "new")
            .order_by(leads.c.motivation_score.desc())
            .limit(limit)
        ).mappings().all()
    if not rows:
        log.info("sheets.no_new_leads")
        return 0
    ensure_header_row(service, spreadsheet_id, tab_name)
    values = [_build_row(dict(r), now) for r in rows]
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()
    with eng.begin() as conn:
        for r in rows:
            conn.execute(
                update(leads).where(leads.c.id == r["id"])
                .values(date_uploaded=now, status="reviewed")
            )
            conn.execute(sheet_pushes.insert().values(
                lead_id=r["id"],
                sheet_tab=tab_name,
                pushed_at=now,
            ))
    log.info("sheets.pushed", count=len(rows), tab=tab_name)
    return len(rows)


def _col_index_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result
