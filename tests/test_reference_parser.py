from collections import defaultdict

from darwin_lakehouse.reference.parser import iter_timetable, parse_reference

SERVICE = "202610097682962"


def test_reference_tables(gzipped):
    tables = parse_reference(gzipped("ref_v4_sample.xml"))

    assert tables["locations"][0] == {"location_code": "WATRLMN", "station_code": "WAT", "operator_code": "NR",
                                      "location_name": "London Waterloo"}
    assert tables["locations"][2]["station_code"] is None  # junctions have no station code
    assert tables["operators"] == [{"operator_code": "SW", "operator_name": "South Western Railway",
                                    "website": "http://www.nationalrail.co.uk/tocs_maps/tocs/SW.aspx"}]
    assert tables["vias"][0] == {"display_station_code": "WAT", "destination_location_code": "WOKING",
                                 "first_via_location_code": "CLPHMJC", "second_via_location_code": None,
                                 "via_text": "via Clapham Junction"}
    assert tables["customer_information_system_sources"] == [
        {"source_code": "at45", "source_name": "South Western Railway"}]


def test_reason_codes_are_split_by_section(gzipped):
    tables = parse_reference(gzipped("ref_v4_sample.xml"))

    assert [r["reason_code"] for r in tables["late_reasons"]] == ["100", "541"]
    assert tables["cancellation_reasons"] == [
        {"reason_code": "100", "reason_text": "This train has been cancelled because of a broken down train"}]


def timetable(gzipped):
    rows = defaultdict(list)
    for table, row in iter_timetable(gzipped("timetable_v8_sample.xml")):
        rows[table].append(row)
    return rows


def test_calling_points_keep_route_order_across_element_types(gzipped):
    stops = [r for r in timetable(gzipped)["schedule_locations"] if r["train_service_id"] == SERVICE]

    assert [(s["sequence"], s["location_type"], s["location_code"]) for s in stops] == [
        (0, "OPOR", "WATRLMS"), (1, "OR", "WATRLMN"), (2, "PP", "VAUXHLM"),
        (3, "IP", "CLPHMJC"), (4, "IP", "WIMBLDN"), (5, "DT", "WATRLMN")]


def test_calling_point_fields(gzipped):
    stops = {s["location_code"]: s for s in timetable(gzipped)["schedule_locations"]
             if s["train_service_id"] == SERVICE}

    clapham = stops["CLPHMJC"]
    assert clapham["activity"] == "T"  # fixed-width "T " is trimmed
    assert clapham["public_scheduled_arrival"] == "21:36"
    assert clapham["working_scheduled_arrival"] == "21:35:30"
    assert clapham["scheduled_start_date"] == "2026-10-09"
    assert stops["VAUXHLM"]["activity"] is None
    assert stops["VAUXHLM"]["working_scheduled_pass"] == "21:33:30"
    assert stops["WIMBLDN"]["is_cancelled"] is True
    assert clapham["is_cancelled"] is False
    assert clapham["route_delay"] == 0


def test_schedule_uses_public_origin_and_destination(gzipped):
    schedule = timetable(gzipped)["schedules"][0]

    # The operational origin (OPOR at the depot) precedes the public origin; the public one wins
    assert schedule["origin_location_code"] == "WATRLMN"
    assert schedule["origin_scheduled_departure"] == "21:30"
    assert schedule["destination_location_code"] == "WATRLMN"
    assert schedule["destination_scheduled_arrival"] == "22:46"
    assert schedule["calling_point_count"] == 6
    assert schedule["cancellation_reason_code"] == "541"
    assert schedule["cancellation_reason_location_code"] == "WIMBLDN"


def test_schedule_defaults_follow_the_xsd(gzipped):
    passenger, empty_stock = timetable(gzipped)["schedules"]

    assert passenger["train_category"] == "OO" and passenger["schedule_status"] == "P"
    assert passenger["is_passenger_service"] is True and passenger["runs_as_required"] is False
    assert empty_stock["train_category"] == "EE" and empty_stock["is_passenger_service"] is False
    # Operational-only journeys fall back to OPOR / OPDT and working times
    assert (empty_stock["origin_location_code"], empty_stock["origin_scheduled_departure"]) == ("WLWYCSD", "03:56")
    assert (empty_stock["destination_location_code"],
            empty_stock["destination_scheduled_arrival"]) == ("WLWYNGC", "03:58")
    assert empty_stock["cancellation_reason_code"] is None


def test_associations(gzipped):
    assert timetable(gzipped)["associations"] == [{
        "main_train_service_id": SERVICE, "associated_train_service_id": "202610097128250",
        "association_category": "VV", "location_code": "CLPHMJC", "is_cancelled": False, "is_deleted": False,
        "main_scheduled_time": "21:35:30", "associated_scheduled_time": "21:40"}]
