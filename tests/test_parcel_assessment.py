from adapters.parcel_assessment import ParcelAssessmentAdapter


def test_parcel_assessment_normalizes_carto_row():
    adapter = ParcelAssessmentAdapter({"id": "opa", "parser_module": "parcel_assessment"})
    rows = adapter.parse([{
        "parcel_number": "001-002-003",
        "location": "123 Market St",
        "owner_1": "Example Holdings LLC",
        "owner_2": None,
        "mailing_street": "456 Chestnut St",
        "mailing_city_state": "Philadelphia PA",
        "mailing_zip": "19106",
        "recording_date": "2026-10-15T04:00:00Z",
    }])
    assert len(rows) == 1
    assert rows[0]["parcel_id_raw"] == "001-002-003"
    assert rows[0]["recording_date"] == "2026-10-15"
