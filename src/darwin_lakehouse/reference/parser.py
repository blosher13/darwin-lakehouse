"""Parsers for Darwin's daily reference (*_ref_v4.xml.gz) and timetable (*_v8.xml.gz) files.

Both stream with iterparse so the ~1M-row timetable never sits in memory. The timetable is
parsed here rather than with Spark's XML reader because a journey's calling points are a
sequence of differently-named elements (OR, IP, PP, DT, ...) whose order *is* the route,
and Spark would group them by element name.
"""

from __future__ import annotations

import gzip
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from os import PathLike
from typing import Any

Row = dict[str, Any]

CALLING_POINTS = {"OR", "OPOR", "IP", "OPIP", "PP", "DT", "OPDT"}
ORIGINS = ("OR", "OPOR")
DESTINATIONS = ("DT", "OPDT")


def _local(tag: str) -> str:
    """'{namespace}Journey' -> 'Journey'."""
    return tag.rsplit("}", 1)[-1]


def _bool(value: str | None, default: bool = False) -> bool:
    return default if value is None else value == "true"


def _activity(value: str | None) -> str | None:
    """Activity codes are fixed-width ('T ', 'TB'); blank means none."""
    return (value or "").strip() or None


def parse_reference(path: str | PathLike) -> dict[str, list[Row]]:
    """Returns the reference file as lists of rows keyed by target table name."""
    tables: dict[str, list[Row]] = {name: [] for name in (
        "locations", "operators", "late_reasons", "cancellation_reasons", "vias", "cis_sources")}
    section = None
    with gzip.open(path) as f:
        for event, el in ET.iterparse(f, events=("start", "end")):
            tag = _local(el.tag)
            if event == "start":
                if tag in ("LateRunningReasons", "CancellationReasons"):
                    section = tag
                continue
            a = el.attrib
            if tag == "LocationRef":
                tables["locations"].append({"tiploc": a["tpl"], "crs": a.get("crs"), "toc": a.get("toc"),
                                            "location_name": a.get("locname")})
            elif tag == "TocRef":
                tables["operators"].append({"toc": a["toc"], "toc_name": a.get("tocname"), "url": a.get("url")})
            elif tag == "Reason":
                table = "late_reasons" if section == "LateRunningReasons" else "cancellation_reasons"
                tables[table].append({"code": a["code"], "reason_text": a.get("reasontext")})
            elif tag == "Via":
                tables["vias"].append({"at_crs": a.get("at"), "destination_tiploc": a.get("dest"),
                                       "loc1_tiploc": a.get("loc1"), "loc2_tiploc": a.get("loc2"),
                                       "via_text": a.get("viatext")})
            elif tag == "CISSource":
                tables["cis_sources"].append({"code": a["code"], "name": a.get("name")})
            el.clear()
    return tables


def _calling_point(seq: int, tag: str, a: dict[str, str]) -> Row:
    return {
        "seq": seq,
        "tiploc": a["tpl"],
        "location_type": tag,
        "activity": _activity(a.get("act")),
        "planned_activity": _activity(a.get("planAct")),
        "platform": a.get("plat"),
        "pta": a.get("pta"), "ptd": a.get("ptd"),
        "wta": a.get("wta"), "wtd": a.get("wtd"), "wtp": a.get("wtp"),
        "is_cancelled": _bool(a.get("can")),
        "false_destination_tiploc": a.get("fd"),
        "route_delay": int(a.get("rdelay", 0)),
    }


def _endpoint(stops: list[Row], types: tuple[str, str], last: bool) -> Row | None:
    """Origin/destination: the public stop (OR/DT) if any, else the operational one (OPOR/OPDT)."""
    ordered = list(reversed(stops)) if last else stops
    for location_type in types:
        match = next((s for s in ordered if s["location_type"] == location_type), None)
        if match:
            return match
    return ordered[0] if ordered else None


def _schedule(a: dict[str, str], stops: list[Row], cancel_reason: Row | None) -> Row:
    origin = _endpoint(stops, ORIGINS, last=False)
    dest = _endpoint(stops, DESTINATIONS, last=True)
    return {
        "rid": a["rid"], "uid": a["uid"], "headcode": a.get("trainId"), "ssd": a["ssd"], "toc": a.get("toc"),
        # Defaults below are the XSD defaults (rttiCTTSchema_v8.xsd) for omitted attributes
        "train_category": a.get("trainCat", "OO"),
        "status": a.get("status", "P"),
        "is_passenger": _bool(a.get("isPassengerSvc"), default=True),
        "is_charter": _bool(a.get("isCharter")),
        "is_q_train": _bool(a.get("qtrain")),
        "is_deleted": _bool(a.get("deleted")),
        "is_cancelled": _bool(a.get("can")),
        "cancel_reason_code": cancel_reason["code"] if cancel_reason else None,
        "cancel_reason_tiploc": cancel_reason["tiploc"] if cancel_reason else None,
        "origin_tiploc": origin["tiploc"] if origin else None,
        "origin_departure": (origin["ptd"] or origin["wtd"]) if origin else None,
        "destination_tiploc": dest["tiploc"] if dest else None,
        "destination_arrival": (dest["pta"] or dest["wta"]) if dest else None,
        "calling_points": len(stops),
    }


def _first_time(a: dict[str, str]) -> str | None:
    return a.get("pta") or a.get("ptd") or a.get("wta") or a.get("wtd")


def iter_timetable(path: str | PathLike) -> Iterator[tuple[str, Row]]:
    """Yields (table, row) for schedules, schedule_locations and associations."""
    stops: list[Row] = []
    cancel_reason: Row | None = None
    with gzip.open(path) as f:
        for _, el in ET.iterparse(f, events=("end",)):
            tag = _local(el.tag)
            a = el.attrib
            if tag in CALLING_POINTS:
                stops.append(_calling_point(len(stops), tag, a))
            elif tag == "cancelReason":
                cancel_reason = {"code": el.text, "tiploc": a.get("tiploc")}
            elif tag == "Journey":
                yield "schedules", _schedule(a, stops, cancel_reason)
                for stop in stops:
                    yield "schedule_locations", {"rid": a["rid"], "ssd": a["ssd"], **stop}
                stops, cancel_reason = [], None
                el.clear()
            elif tag == "Association":
                main, assoc = el.find("{*}main").attrib, el.find("{*}assoc").attrib
                yield "associations", {
                    "main_rid": main["rid"], "assoc_rid": assoc["rid"], "category": a["category"],
                    "tiploc": a["tiploc"], "is_cancelled": _bool(a.get("isCancelled")),
                    "is_deleted": _bool(a.get("isDeleted")),
                    "main_time": _first_time(main), "assoc_time": _first_time(assoc),
                }
                el.clear()
