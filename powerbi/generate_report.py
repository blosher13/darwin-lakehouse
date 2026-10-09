"""Generates the Power BI report definition (PBIR) for DarwinRail.Report from a compact spec.

PBIR is verbose JSON - one file per page and per visual. Keeping the layout here makes the
report reviewable as code; run `python powerbi/generate_report.py` after editing, then
`python powerbi/validate_pbir.py` to check every file against Microsoft's published schemas.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPORT = HERE / "DarwinRail.Report"
SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric"
FACT = "fact_train_calls"

PAGE_W, PAGE_H, GUTTER = 1280, 720, 16
LEFT, RIGHT = 24, 1280 - 24
HALF = (RIGHT - LEFT - GUTTER) // 2

# Single-hue design: every chart is one series, so blue carries "the data" and nothing relies on
# telling hues apart. The rest of the categorical order is there for anyone extending the report.
THEME = {
    "name": "Darwin Lakehouse",
    "dataColors": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"],
    "background": "#FFFFFF",
    "foreground": "#0b0b0b",
    "tableAccent": "#2a78d6",
    "good": "#008300",
    "neutral": "#eda100",
    "bad": "#e34948",
}


# ----------------------------------------------------------------------------- field helpers

def measure(name: str, entity: str = FACT) -> dict:
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def column(name: str, entity: str = FACT) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": name}}


def projection(field: dict, display_name: str | None = None) -> dict:
    kind = "Measure" if "Measure" in field else "Column"
    entity = field[kind]["Expression"]["SourceRef"]["Entity"]
    prop = field[kind]["Property"]
    p = {"field": field, "queryRef": f"{entity}.{prop}", "nativeQueryRef": prop}
    if display_name:
        p["displayName"] = display_name
    return p


def literal(value) -> dict:
    if isinstance(value, bool):
        value = "true" if value else "false"
    elif isinstance(value, str):
        value = "'" + value.replace("'", "''") + "'"
    else:
        value = f"{value}D"
    return {"expr": {"Literal": {"Value": value}}}


# ----------------------------------------------------------------------------- visual builders

def visual(name: str, visual_type: str, box: tuple[int, int, int, int], roles: dict[str, list[dict]],
           title: str | None = None, sort: tuple[dict, str] | None = None, objects: dict | None = None) -> dict:
    x, y, w, h = box
    query: dict = {"queryState": {role: {"projections": fields} for role, fields in roles.items()}}
    if sort:
        query["sortDefinition"] = {"sort": [{"field": sort[0], "direction": sort[1]}], "isDefaultSort": False}
    body: dict = {"visualType": visual_type, "query": query, "drillFilterOtherVisuals": True}
    if objects:
        body["objects"] = objects
    if title:
        body["visualContainerObjects"] = {
            "title": [{"properties": {"show": literal(True), "text": literal(title)}}]}
    return {
        "$schema": f"{SCHEMAS}/item/report/definition/visualContainer/1.8.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "z": 0, "width": w, "height": h, "tabOrder": 0},
        "visual": body,
    }


def textbox(name: str, box: tuple[int, int, int, int], text: str, subtitle: str) -> dict:
    x, y, w, h = box
    return {
        "$schema": f"{SCHEMAS}/item/report/definition/visualContainer/1.8.0/schema.json",
        "name": name,
        "position": {"x": x, "y": y, "z": 0, "width": w, "height": h, "tabOrder": 0},
        "visual": {
            "visualType": "textbox",
            "objects": {"general": [{"properties": {"paragraphs": [
                {"textRuns": [{"value": text, "textStyle": {"fontSize": "20pt", "fontWeight": "bold"}}]},
                {"textRuns": [{"value": subtitle, "textStyle": {"fontSize": "10pt", "color": "#52514e"}}]},
            ]}}]},
        },
    }


def kpi_row(y: int, h: int, kpis: list[tuple[str, str]]) -> list[dict]:
    w = (RIGHT - LEFT - GUTTER * (len(kpis) - 1)) // len(kpis)
    return [visual(f"kpi{i}", "card", (LEFT + i * (w + GUTTER), y, w, h),
                   {"Values": [projection(measure(m))]}, title=label)
            for i, (m, label) in enumerate(kpis)]


def date_slicer(name: str) -> dict:
    return visual(name, "slicer", (RIGHT - 320, 12, 320, 56),
                  {"Values": [projection(column("Date", "dim_date"))]}, title="Service date")


# ----------------------------------------------------------------------------- pages

def network_page() -> tuple[dict, list[dict]]:
    visuals = [
        textbox("title", (LEFT, 12, 800, 56), "UK rail network performance",
                "Live National Rail Darwin data · passenger departures from public stops vs the public timetable"),
        date_slicer("dateSlicer"),
        *kpi_row(80, 104, [("Trains", "Trains tracked"), ("Departures", "Departures recorded"),
                           ("Within 5 min %", "Departed within 5 min"),
                           ("Avg departure delay", "Average departure delay")]),
        visual("withinFiveByHour", "lineChart", (LEFT, 200, HALF, 248),
               {"Category": [projection(column("Scheduled hour"))], "Y": [projection(measure("Within 5 min %"))]},
               title="Departed within 5 minutes, by scheduled hour",
               sort=(column("Scheduled hour"), "Ascending")),
        visual("delayByOperator", "clusteredBarChart", (LEFT + HALF + GUTTER, 200, HALF, 248),
               {"Category": [projection(column("Operator", "dim_operator"))],
                "Y": [projection(measure("Avg departure delay"))]},
               title="Average departure delay by operator",
               sort=(measure("Avg departure delay"), "Descending")),
        visual("stationTable", "tableEx", (LEFT, 464, RIGHT - LEFT, 232),
               {"Values": [projection(column("Station", "dim_location")),
                           projection(column("crs", "dim_location"), "CRS"),
                           projection(measure("Departures")), projection(measure("Delay minutes")),
                           projection(measure("Avg departure delay")), projection(measure("Within 5 min %"))]},
               title="Stations ranked by total delay minutes",
               sort=(measure("Delay minutes"), "Descending")),
    ]
    return {"name": "networkPerformance", "displayName": "Network performance"}, visuals


def causes_page() -> tuple[dict, list[dict]]:
    visuals = [
        textbox("title", (LEFT, 12, 800, 56), "Why trains are late",
                "Darwin late-running reasons and the distribution of departure delays"),
        date_slicer("dateSlicer"),
        visual("trainsByReason", "clusteredBarChart", (LEFT, 80, HALF, 300),
               {"Category": [projection(column("Reason", "dim_late_reason"))],
                "Y": [projection(measure("Delayed trains"))]},
               title="Trains delayed, by reason",
               sort=(measure("Delayed trains"), "Descending")),
        visual("delayDistribution", "clusteredColumnChart", (LEFT + HALF + GUTTER, 80, HALF, 300),
               {"Category": [projection(column("Delay band"))], "Y": [projection(measure("Departures"))]},
               title="Departures by delay band",
               sort=(column("Delay band"), "Ascending")),
        visual("worstDepartures", "tableEx", (LEFT, 396, RIGHT - LEFT, 300),
               {"Values": [projection(column("headcode"), "Headcode"),
                           projection(column("Operator", "dim_operator")),
                           projection(column("Journey")),
                           projection(column("Station", "dim_location")),
                           projection(column("scheduled_departure"), "Scheduled"),
                           projection(column("actual_departure"), "Departed"),
                           projection(column("departure_delay_min"), "Minutes late"),
                           projection(column("Reason", "dim_late_reason"))]},
               title="Most delayed departures",
               sort=(column("departure_delay_min"), "Descending")),
    ]
    return {"name": "delayCauses", "displayName": "Delay causes"}, visuals


# ----------------------------------------------------------------------------- write

def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


def main() -> None:
    definition = REPORT / "definition"
    shutil.rmtree(definition, ignore_errors=True)

    write_json(REPORT / "definition.pbir", {
        "$schema": f"{SCHEMAS}/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {"byPath": {"path": "../DarwinRail.SemanticModel"}},
    })
    write_json(definition / "version.json", {
        "$schema": f"{SCHEMAS}/item/report/definition/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    write_json(definition / "report.json", {
        "$schema": f"{SCHEMAS}/item/report/definition/report/1.2.0/schema.json",
        "themeCollection": {"customTheme": {"name": "DarwinTheme.json", "reportVersionAtImport": "5.61",
                                            "type": "RegisteredResources"}},
        "layoutOptimization": "None",
        "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources",
                              "items": [{"name": "DarwinTheme.json", "path": "DarwinTheme.json",
                                         "type": "CustomTheme"}]}],
    })
    write_json(REPORT / "StaticResources" / "RegisteredResources" / "DarwinTheme.json", THEME)

    pages = [network_page(), causes_page()]
    write_json(definition / "pages" / "pages.json", {
        "$schema": f"{SCHEMAS}/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p["name"] for p, _ in pages], "activePageName": pages[0][0]["name"]})
    for page, visuals in pages:
        folder = definition / "pages" / page["name"]
        write_json(folder / "page.json", {
            "$schema": f"{SCHEMAS}/item/report/definition/page/1.4.0/schema.json",
            **page, "displayOption": "FitToPage", "height": PAGE_H, "width": PAGE_W})
        for z, v in enumerate(visuals):
            v["position"]["z"] = v["position"]["tabOrder"] = z * 1000
            write_json(folder / "visuals" / v["name"] / "visual.json", v)

    write_json(HERE / "DarwinRail.pbip", {
        "$schema": f"{SCHEMAS}/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0",
        "artifacts": [{"report": {"path": "DarwinRail.Report"}}],
        "settings": {"enableAutoRecovery": True},
    })
    print(f"Wrote {sum(len(v) for _, v in pages)} visuals on {len(pages)} pages to {REPORT}")


if __name__ == "__main__":
    main()
