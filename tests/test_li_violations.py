from adapters.philly_li_violations import PhillyLiViolationsAdapter


def test_current_eclipse_columns_are_parsed():
    adapter = object.__new__(PhillyLiViolationsAdapter)
    rows = adapter.parse([{
        "address": "6649 CASTOR AVE",
        "opa_account_num": "871279550",
        "opa_owner": "CHOI DARL Y, CHOI YU S",
        "casenumber": "CF-2026-157318",
        "casetype": "NOTICE OF VIOLATION",
        "violationcodetitle": "ROOF DEFICIENCIES",
        "violationstatus": "OPEN",
        "violationdate": "2026-11-22T18:24:35Z",
    }])
    assert rows[0]["property_address_raw"] == "6649 CASTOR AVE"
    assert rows[0]["parcel_id_raw"] == "871279550"
    assert rows[0]["owner_name_raw"] == "CHOI DARL Y, CHOI YU S"
    assert rows[0]["violation_type"] == "ROOF DEFICIENCIES"
