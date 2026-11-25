from datetime import datetime, timezone

from core.sheets_export import _build_row


def test_build_row_uses_database_column_names():
    uploaded = datetime(2026, 11, 25, tzinfo=timezone.utc)
    row = _build_row({
        "id": 42,
        "motivation_score": 55,
        "motivation_category": "Code Violation",
        "property_address_raw": "123 MARKET ST",
        "owner_name_raw": "TEST OWNER",
        "mailing_address_raw": "456 CHESTNUT ST",
        "county": "Philadelphia",
        "state": "PA",
        "parcel_id_normalized": "123456789",
        "source_id": "pa_philadelphia_li_violations",
        "source_type": "code_violation",
        "source_url": "https://example.test/source",
        "filing_date": "2026-11-20",
        "amount_due": 1234.50,
        "violation_type": "Unsafe structure",
        "docket_number": "ABC-123",
        "first_seen_at": uploaded,
    }, uploaded)

    assert row[1] == "WARM"
    assert row[4:13] == [
        "123 MARKET ST", "TEST OWNER", "456 CHESTNUT ST", "Philadelphia", "PA",
        "123456789", "pa_philadelphia_li_violations", "code_violation",
        "https://example.test/source",
    ]
    assert row[15] == 1234.50
    assert row[22] == "42"
