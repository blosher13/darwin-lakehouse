"""Renders static previews of the two Power BI report pages from live gold data.

Each query mirrors a DAX measure in the semantic model, so the images double as a check on
the measure logic. Output goes to docs/images/ for the README.

    pip install -e '.[preview]' && python powerbi/render_preview.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import PercentFormatter

from darwin_lakehouse.config import PROJECT_ROOT, Settings
from darwin_lakehouse.databricks import DatabricksClient

OUT = PROJECT_ROOT / "docs" / "images"

# Reference palette: one data hue on a light surface, ink tokens for all text
SURFACE, PANEL, BORDER = "#f3f2ef", "#fcfcfb", "#e4e3df"
INK, INK_2, INK_3, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e9e8e4"
BLUE = "#2a78d6"

DEPARTURES = ("call_type = 'Public stop' AND is_passenger_service AND departure_delay_minutes IS NOT NULL")
# "This train has been delayed by a fault..." -> "A fault..." (same shortening as the Power BI model)
_STRIPPED = "regexp_replace(reason_text, '^This (train|service) has been delayed by ', '')"
SHORT_REASON = f"concat(upper(left({_STRIPPED}, 1)), substr({_STRIPPED}, 2))"


def query(db: DatabricksClient, sql: str) -> list[list]:
    return db.sql(sql.replace("{F}", f"{SCHEMA}.fact_train_calls").replace("{DEP}", DEPARTURES))


SETTINGS = Settings()
SCHEMA = f"{SETTINGS.catalog}.{SETTINGS.schema}"


def load(db: DatabricksClient) -> dict:
    kpi = query(db, """
        SELECT count(DISTINCT train_service_id), count_if({DEP}),
               avg(IF({DEP}, IF(departure_delay_minutes <= 5, 1, 0), NULL)),
               avg(IF(call_type = 'Public stop' AND is_passenger_service, departure_delay_minutes, NULL)),
               min(scheduled_start_date), max(scheduled_start_date)
        FROM {F}""")[0]
    return {
        "kpi": kpi,
        "by_hour": query(db, """
            SELECT hour(scheduled_timestamp), avg(IF(departure_delay_minutes <= 5, 1, 0)), count(*)
            FROM {F} WHERE {DEP} GROUP BY 1 HAVING count(*) >= 50 ORDER BY 1"""),
        "by_operator": query(db, f"""
            SELECT o.operator_name, avg(f.departure_delay_minutes), count(*)
            FROM {{F}} f JOIN {SCHEMA}.operators o ON o.operator_code = f.operator_code
            WHERE {{DEP}} GROUP BY 1 HAVING count(*) >= 100 ORDER BY 2 DESC LIMIT 12"""),
        "stations": query(db, f"""
            SELECT l.location_name, l.station_code, count(*), sum(greatest(f.departure_delay_minutes, 0)),
                   avg(f.departure_delay_minutes), avg(IF(f.departure_delay_minutes <= 5, 1, 0))
            FROM {{F}} f JOIN {SCHEMA}.locations l ON l.location_code = f.location_code
            WHERE {{DEP}} GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 6"""),
        "reasons": query(db, f"""
            SELECT {SHORT_REASON}, count(DISTINCT f.train_service_id)
            FROM {{F}} f JOIN {SCHEMA}.late_reasons r ON r.reason_code = f.late_reason_code
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8"""),
        "bands": query(db, """
            SELECT CASE WHEN departure_delay_minutes <= 0 THEN 1 WHEN departure_delay_minutes <= 2 THEN 2
                        WHEN departure_delay_minutes <= 5 THEN 3 WHEN departure_delay_minutes <= 15 THEN 4
                        WHEN departure_delay_minutes <= 30 THEN 5 ELSE 6 END, count(*)
            FROM {F} WHERE {DEP} GROUP BY 1 ORDER BY 1"""),
        "worst": query(db, f"""
            SELECT f.headcode, o.operator_name, concat(f.origin_name, ' → ', f.destination_name), l.location_name,
                   f.scheduled_departure, f.actual_departure, f.departure_delay_minutes, {SHORT_REASON}
            FROM {{F}} f
            LEFT JOIN {SCHEMA}.operators o ON o.operator_code = f.operator_code
            LEFT JOIN {SCHEMA}.locations l ON l.location_code = f.location_code
            LEFT JOIN {SCHEMA}.late_reasons r ON r.reason_code = f.late_reason_code
            WHERE {{DEP}} ORDER BY f.departure_delay_minutes DESC LIMIT 7"""),
    }


# ----------------------------------------------------------------------------- drawing helpers

def page(title: str, subtitle: str, dates: str):
    fig = plt.figure(figsize=(12.8, 7.2), dpi=150, facecolor=SURFACE)
    fig.text(0.019, 0.955, title, fontsize=17, fontweight="bold", color=INK, va="top")
    fig.text(0.019, 0.905, subtitle, fontsize=9, color=INK_2, va="top")
    fig.text(0.981, 0.945, dates, fontsize=9, color=INK_2, va="top", ha="right")
    return fig


def panel(fig, rect, title):
    """A card with a title; returns the plotting axes inside it."""
    x, y, w, h = rect
    card = fig.add_axes(rect, facecolor=PANEL)
    card.set_xticks([]), card.set_yticks([])
    for spine in card.spines.values():
        spine.set_color(BORDER)
    card.set_zorder(0)
    fig.text(x + 0.012, y + h - 0.018, title, fontsize=10, fontweight="bold", color=INK, va="top")
    return card


def style_axes(ax, grid_axis="y"):
    ax.set_facecolor("none")
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BORDER)
    ax.tick_params(colors=INK_2, labelsize=8, length=0)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def table(fig, rect, header, rows, widths, align):
    x0, y0, w, h = rect
    line_h = h / (len(rows) + 1.4)
    y = y0 + h - line_h
    xs = [x0 + w * sum(widths[:i]) for i in range(len(widths))]
    def cell_x(xi, wi, a):
        return xi + w * wi - 0.012 if a == "right" else xi

    for xi, wi, label, a in zip(xs, widths, header, align, strict=True):
        fig.text(cell_x(xi, wi, a), y, label, fontsize=8, color=INK_3, fontweight="bold", ha=a, va="center")
    for row in rows:
        y -= line_h
        fig.add_artist(plt.Line2D([x0, x0 + w], [y + line_h / 2] * 2, color=GRID, linewidth=0.6,
                                  transform=fig.transFigure))
        for xi, wi, value, a in zip(xs, widths, row, align, strict=True):
            fig.text(cell_x(xi, wi, a), y, value, fontsize=8.5, color=INK, ha=a, va="center")


def truncate(text: str, n: int) -> str:
    return text if len(text) <= n else text[: n - 1] + "…"


# ----------------------------------------------------------------------------- pages

def network_page(d: dict, dates: str) -> plt.Figure:
    trains, departures, within5, avg_delay = d["kpi"][:4]
    fig = page("UK rail network performance",
               "Live National Rail Darwin data · passenger departures from public stops vs the public timetable",
               dates)

    kpis = [(f"{int(trains):,}", "Trains tracked"),
            (f"{int(departures):,}", "Departures recorded"),
            (f"{float(within5):.1%}", "Departed within 5 min"),
            (f"{float(avg_delay):.1f} min", "Average departure delay")]
    for i, (value, label) in enumerate(kpis):
        x = 0.019 + i * 0.2427
        panel(fig, (x, 0.70, 0.2302, 0.155), label)
        fig.text(x + 0.012, 0.725, value, fontsize=24, fontweight="bold", color=INK, va="bottom")

    panel(fig, (0.019, 0.355, 0.4685, 0.325), "Departed within 5 minutes, by scheduled hour")
    ax = fig.add_axes((0.06, 0.39, 0.415, 0.235))
    by_hour = {int(r[0]): float(r[1]) for r in d["by_hour"]}
    hours = list(range(24))
    share = [by_hour.get(h, float("nan")) for h in hours]  # NaN breaks the line where no data yet
    ax.plot(hours, share, color=BLUE, linewidth=2, marker="o", markersize=5,
            markeredgecolor=PANEL, markeredgewidth=1.5)
    worst = min(by_hour, key=by_hour.get)
    ax.annotate(f"{by_hour[worst]:.0%} at {worst:02d}:00", (worst, by_hour[worst]),
                textcoords="offset points", xytext=(10, 0), ha="left", va="center", fontsize=8, color=INK_2)
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    ax.set_ylim(max(0, min(by_hour.values()) - 0.1), 1.02)
    ax.set_xlim(-0.5, 23.5)
    ax.set_xticks(range(0, 24, 3), [f"{h:02d}" for h in range(0, 24, 3)])
    style_axes(ax)

    panel(fig, (0.5125, 0.355, 0.4685, 0.325), "Average departure delay by operator (minutes)")
    ax = fig.add_axes((0.64, 0.385, 0.33, 0.245))
    ops = d["by_operator"][::-1]
    values = [float(r[1]) for r in ops]
    bars = ax.barh([truncate(r[0], 26) for r in ops], values, color=BLUE, height=0.62)
    for bar, v in zip(bars, values, strict=True):
        ax.text(bar.get_width() + max(values) * 0.012, bar.get_y() + bar.get_height() / 2, f"{v:.1f}",
                va="center", fontsize=7.5, color=INK_2)
    style_axes(ax, grid_axis="x")
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(axis="y", labelsize=7.5)

    panel(fig, (0.019, 0.03, 0.962, 0.305), "Stations ranked by total delay minutes")
    rows = [[f"{r[0]}", r[1] or "", f"{int(r[2]):,}", f"{int(r[3]):,}", f"{float(r[4]):.1f}", f"{float(r[5]):.1%}"]
            for r in d["stations"]]
    table(fig, (0.031, 0.045, 0.938, 0.245), ["Station", "Station code", "Departures", "Delay minutes", "Average delay",
                                               "Within 5 minutes %"],
          rows, [0.40, 0.10, 0.125, 0.125, 0.125, 0.125], ["left", "left", "right", "right", "right", "right"])
    return fig


def causes_page(d: dict, dates: str) -> plt.Figure:
    fig = page("Why trains are late", "Darwin late-running reasons and the distribution of departure delays", dates)

    panel(fig, (0.019, 0.43, 0.4685, 0.43), "Trains delayed, by reason")
    ax = fig.add_axes((0.215, 0.46, 0.255, 0.35))
    reasons = d["reasons"][::-1]
    values = [int(r[1]) for r in reasons]
    bars = ax.barh([truncate(r[0], 38) for r in reasons], values, color=BLUE, height=0.62)
    for bar, v in zip(bars, values, strict=True):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2, f"{v:,}",
                va="center", fontsize=7.5, color=INK_2)
    style_axes(ax, grid_axis="x")
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(axis="y", labelsize=7.5)

    panel(fig, (0.5125, 0.43, 0.4685, 0.43), "Departures by delay band")
    ax = fig.add_axes((0.56, 0.48, 0.405, 0.32))
    labels = {1: "On time\nor early", 2: "1–2 min", 3: "3–5 min", 4: "6–15 min", 5: "16–30 min", 6: "Over 30"}
    bands = [(labels[int(r[0])], int(r[1])) for r in d["bands"]]
    bars = ax.bar([b[0] for b in bands], [b[1] for b in bands], color=BLUE, width=0.62)
    total = sum(b[1] for b in bands)
    for bar, (_, v) in zip(bars, bands, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{v / total:.0%}",
                ha="center", va="bottom", fontsize=8, color=INK_2)
    style_axes(ax)
    ax.yaxis.set_major_formatter(lambda v, _: f"{int(v):,}")

    panel(fig, (0.019, 0.03, 0.962, 0.38), "Most delayed departures")
    rows = [[r[0] or "", truncate(r[1] or "", 22), truncate(r[2] or "", 42), truncate(r[3] or "", 24), r[4] or "",
             r[5] or "", str(r[6]), truncate(r[7] or "—", 44)] for r in d["worst"]]
    table(fig, (0.031, 0.045, 0.938, 0.32),
          ["Headcode", "Operator", "Journey", "Station", "Scheduled", "Departed", "Min late", "Reason"],
          rows, [0.07, 0.14, 0.24, 0.13, 0.07, 0.07, 0.06, 0.22],
          ["left", "left", "left", "left", "left", "left", "right", "left"])
    return fig


def main() -> None:
    installed = {f.name for f in font_manager.fontManager.ttflist}
    # First available UI face, with DejaVu Sans as per-glyph fallback (e.g. for "→")
    preferred = next((f for f in ("Inter", "Helvetica Neue", "Arial") if f in installed), None)
    plt.rcParams["font.family"] = [f for f in (preferred, "DejaVu Sans") if f]
    data = load(DatabricksClient.from_settings(SETTINGS))
    first, last = data["kpi"][4], data["kpi"][5]
    dates = f"Service dates {first} – {last}" if first != last else f"Service date {first}"
    OUT.mkdir(parents=True, exist_ok=True)
    for name, fig in (("dashboard-network.png", network_page(data, dates)),
                      ("dashboard-causes.png", causes_page(data, dates))):
        fig.savefig(Path(OUT, name), facecolor=SURFACE)
        print("wrote", OUT / name)


if __name__ == "__main__":
    main()
