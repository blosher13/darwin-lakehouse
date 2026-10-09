from collections import defaultdict

from darwin_lakehouse.reference.parser import iter_timetable, parse_reference


def test_reference_tables(gzipped):
    tables = parse_reference(gzipped("ref_v4_sample.xml"))

    assert tables["locations"][0] == {"tiploc": "WATRLMN", "crs": "WAT", "toc": "NR",
                                      "location_name": "London Waterloo"}
    assert tables["locations"][2]["crs"] is None  # junctions have no CRS code
    assert tables["operators"] == [{"toc": "SW", "toc_name": "South Western Railway",
                                    "url": "http://www.nationalrail.co.uk/tocs_maps/tocs/SW.aspx"}]
    assert tables["vias"][0]["via_text"] == "via Clapham Junction"
    assert tables["cis_sources"] == [{"code": "at45", "name": "South Western Railway"}]


def test_reason_codes_are_split_by_section(gzipped):
    tables = parse_reference(gzipped("ref_v4_sample.xml"))

    assert [r["code"] for r in tables["late_reasons"]] == ["100", "541"]
    assert tables["cancellation_reasons"] == [
        {"code": "100", "reason_text": "This train has been cancelled because of a broken down train"}]


def timetable(gzipped):
    rows = defaultdict(list)
    for table, row in iter_timetable(gzipped("timetable_v8_sample.xml")):
        rows[table].append(row)
    return rows


def test_calling_points_keep_route_order_across_element_types(gzipped):
    stops = [r for r in timetable(gzipped)["schedule_locations"] if r["rid"] == "202610097682962"]

    assert [(s["seq"], s["location_type"], s["tiploc"]) for s in stops] == [
        (0, "OPOR", "WATRLMS"), (1, "OR", "WATRLMN"), (2, "PP", "VAUXHLM"),
        (3, "IP", "CLPHMJC"), (4, "IP", "WIMBLDN"), (5, "DT", "WATRLMN")]


def test_calling_point_fields(gzipped):
    stops = {s["tiploc"]: s for s in timetable(gzipped)["schedule_locations"] if s["rid"] == "202610097682962"}

    assert stops["CLPHMJC"]["activity"] == "T"  # fixed-width "T " is trimmed
    assert stops["CLPHMJC"]["pta"] == "21:36" and stops["CLPHMJC"]["wta"] == "21:35:30"
    assert stops["VAUXHLM"]["activity"] is None and stops["VAUXHLM"]["wtp"] == "21:33:30"
    assert stops["WIMBLDN"]["is_cancelled"] is True
    assert stops["CLPHMJC"]["is_cancelled"] is False
    assert stops["CLPHMJC"]["route_delay"] == 0


def test_schedule_uses_public_origin_and_destination(gzipped):
    schedule = timetable(gzipped)["schedules"][0]

    # The operational origin (OPOR at the depot) precedes the public origin; the public one wins
    assert schedule["origin_tiploc"] == "WATRLMN"
    assert schedule["origin_departure"] == "21:30"
    assert schedule["destination_tiploc"] == "WATRLMN"
    assert schedule["destination_arrival"] == "22:46"
    assert schedule["calling_points"] == 6
    assert schedule["cancel_reason_code"] == "541" and schedule["cancel_reason_tiploc"] == "WIMBLDN"


def test_schedule_defaults_follow_the_xsd(gzipped):
    passenger, empty_stock = timetable(gzipped)["schedules"]

    assert passenger["train_category"] == "OO" and passenger["status"] == "P"
    assert passenger["is_passenger"] is True
    assert empty_stock["train_category"] == "EE" and empty_stock["is_passenger"] is False
    # Operational-only journeys fall back to OPOR / OPDT and working times
    assert (empty_stock["origin_tiploc"], empty_stock["origin_departure"]) == ("WLWYCSD", "03:56")
    assert (empty_stock["destination_tiploc"], empty_stock["destination_arrival"]) == ("WLWYNGC", "03:58")
    assert empty_stock["cancel_reason_code"] is None


def test_associations(gzipped):
    assert timetable(gzipped)["associations"] == [{
        "main_rid": "202610097682962", "assoc_rid": "202610097128250", "category": "VV", "tiploc": "CLPHMJC",
        "is_cancelled": False, "is_deleted": False, "main_time": "21:35:30", "assoc_time": "21:40"}]
