import os
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Text, Numeric, DateTime, UniqueConstraint, ForeignKey, Index
)
from sqlalchemy.pool import NullPool
import structlog

log = structlog.get_logger()
metadata = MetaData()

raw_artifacts = Table(
    "raw_artifacts", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, nullable=False),
    Column("source_url", Text),
    Column("fetched_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
    Column("content_hash", Text),
    Column("http_status", Integer),
    Column("parser_version", Text),
    Column("artifact_path", Text),
    Column("artifact_type", Text),
)

leads = Table(
    "leads", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("state", Text),
    Column("county", Text),
    Column("municipality", Text),
    Column("property_address_raw", Text),
    Column("property_address_normalized", Text),
    Column("owner_name_raw", Text),
    Column("owner_name_normalized", Text),
    Column("mailing_address_raw", Text),
    Column("mailing_address_normalized", Text),
    Column("parcel_id_raw", Text),
    Column("parcel_id_normalized", Text),
    Column("docket_number", Text),
    Column("sale_number", Text),
    Column("case_type", Text),
    Column("filing_date", Text),
    Column("sale_date", Text),
    Column("tax_years_due", Text),
    Column("amount_due", Numeric),
    Column("violation_type", Text),
    Column("violation_status", Text),
    Column("motivation_category", Text),
    Column("motivation_score", Integer),
    Column("source_url", Text),
    Column("first_seen_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
    Column("last_seen_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
    Column("date_uploaded", DateTime(timezone=True)),
    Column("status", Text, default="new"),
    UniqueConstraint("parcel_id_normalized", "county", name="uq_parcel_county"),
)

dedup_log = Table(
    "dedup_log", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("canonical_id", Integer, ForeignKey("leads.id")),
    Column("duplicate_id", Integer),
    Column("dedup_method", Text),
    Column("resolved_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
)

sheet_pushes = Table(
    "sheet_pushes", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lead_id", Integer, ForeignKey("leads.id")),
    Column("sheet_tab", Text),
    Column("row_index", Integer),
    Column("pushed_at", DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
)

Index("idx_leads_county_source", leads.c.county, leads.c.source_type)
Index("idx_leads_status", leads.c.status)
Index("idx_leads_score", leads.c.motivation_score)
Index("idx_leads_parcel", leads.c.parcel_id_normalized)


def get_engine(db_url=None):
    url = db_url or os.environ.get("DATABASE_URL", "sqlite:///data/leads.db")
    return create_engine(url, poolclass=NullPool, echo=False)


def init_db(engine=None):
    eng = engine or get_engine()
    metadata.create_all(eng)
    log.info("database.init_db", tables=list(metadata.tables.keys()))
    return eng
