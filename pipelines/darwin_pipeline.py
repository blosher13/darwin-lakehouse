# Lakeflow (DLT) pipeline: Darwin bronze -> silver -> gold.
#
# silver  train_status_locations: one row per (TS message, TS element, location) parsed from
#         Darwin Train Status messages in workspace.darwin.pushport_bronze. Incremental: each
#         run only reads bronze rows appended since the last run.
# gold    fact_train_calls: one row per train call (train x location x scheduled time) with the
#         latest known actual/estimated times, delays and platform. Feeds the Power BI model.

import dlt
from pyspark.sql import functions as F

BRONZE = "workspace.darwin.pushport_bronze"

TIME_DATA = ("STRUCT<_et: STRING, _wet: STRING, _at: STRING, _atRemoved: BOOLEAN, _atClass: STRING, "
             "_etmin: STRING, _etUnknown: BOOLEAN, _delayed: BOOLEAN, _src: STRING, _srcInst: STRING>")

# Mirrors rttiPPTForecasts_v3.xsd (TS / TSLocation / TSTimeData / PlatformData).
# Times stay STRING: Darwin mixes HH:MM and HH:MM:SS, relative to the scheduled start date.
TS_SCHEMA = f"""STRUCT<
  _ts: TIMESTAMP,
  uR: STRUCT<_updateOrigin: STRING, _requestSource: STRING, _requestID: STRING, TS: ARRAY<STRUCT<
    _rid: STRING, _uid: STRING, _ssd: DATE, _isReverseFormation: BOOLEAN,
    LateReason: STRUCT<_VALUE: STRING, _tiploc: STRING, _near: BOOLEAN>,
    Location: ARRAY<STRUCT<
      _tpl: STRING, _wta: STRING, _wtd: STRING, _wtp: STRING, _pta: STRING, _ptd: STRING,
      arr: {TIME_DATA}, dep: {TIME_DATA}, pass: {TIME_DATA},
      plat: STRUCT<_VALUE: STRING, _platsup: BOOLEAN, _cisPlatsup: BOOLEAN, _platsrc: STRING, _conf: BOOLEAN>,
      suppr: BOOLEAN, length: INT, detachFront: BOOLEAN>>>>>
>"""

LINEAGE = ["kafka_partition", "kafka_offset", "sequence_number", "jms_timestamp", "ingested_at"]


def time_columns(event):
    """arr/dep/pass forecast columns, e.g. arr_et, arr_at, arr_delayed."""
    return [
        f"l.{event}._et AS {event}_et",
        f"l.{event}._wet AS {event}_wet",
        f"l.{event}._at AS {event}_at",
        f"l.{event}._atClass AS {event}_at_class",
        f"coalesce(l.{event}._atRemoved, false) AS {event}_at_removed",
        f"coalesce(l.{event}._delayed, false) AS {event}_delayed",
        f"l.{event}._src AS {event}_src",
    ]


@dlt.table(
    name="train_status_locations",
    comment="Darwin Train Status (TS) forecasts and actuals, one row per message/train/location. "
            "Deduplicated on (kafka_partition, kafka_offset).",
    cluster_by=["ssd", "rid"],
)
@dlt.expect_or_fail("rid_present", "rid IS NOT NULL")
@dlt.expect("has_location", "tiploc IS NOT NULL")
def train_status_locations():
    messages = (
        spark.readStream.table(BRONZE)
        .where("message_type = 'TS'")
        # Bronze is at-least-once: a replay after a crash re-sends rows within seconds
        .withWatermark("ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["kafka_partition", "kafka_offset"])
    )

    parsed = messages.select(
        *LINEAGE,
        F.expr(
            # Darwin's namespace prefixes (ns5: etc.) are not stable; drop them before parsing
            f"""from_xml(regexp_replace(xml, '<(/?)ns[0-9]+:', '<$1'), '{TS_SCHEMA}', map('mode', 'FAILFAST'))"""
        ).alias("p"),
    )

    trains = parsed.selectExpr(
        *LINEAGE,
        "p._ts AS darwin_ts",
        "p.uR._updateOrigin AS update_origin",
        "p.uR._requestSource AS request_source",
        "posexplode(p.uR.TS) AS (ts_pos, t)",
    )

    locations = trains.selectExpr("*", "posexplode_outer(t.Location) AS (loc_pos, l)")

    return locations.selectExpr(
        "kafka_partition", "kafka_offset", "ts_pos", "loc_pos",
        "sequence_number", "jms_timestamp", "darwin_ts", "ingested_at", "update_origin", "request_source",
        "t._rid AS rid", "t._uid AS uid", "t._ssd AS ssd",
        "t.LateReason._VALUE AS late_reason_code", "t.LateReason._tiploc AS late_reason_tiploc",
        "coalesce(t._isReverseFormation, false) AS is_reverse_formation",
        "l._tpl AS tiploc",
        "l._wta AS wta", "l._wtd AS wtd", "l._wtp AS wtp", "l._pta AS pta", "l._ptd AS ptd",
        *time_columns("arr"), *time_columns("dep"), *time_columns("pass"),
        "l.plat._VALUE AS platform",
        "coalesce(l.plat._platsup, false) AS platform_suppressed",
        "coalesce(l.plat._conf, false) AS platform_confirmed",
        "l.plat._platsrc AS platform_source",
        "coalesce(l.suppr, false) AS suppressed",
        "nullif(l.length, 0) AS train_length",
        "coalesce(l.detachFront, false) AS detach_front",
    )


GOLD_DAYS = 30  # fact_train_calls covers the last N days to bound recompute cost


def latest(column):
    """Most recent non-null value of a column across a train call's TS updates."""
    return f"max_by({column}, IF({column} IS NULL, NULL, sequence_number)) AS {column}"


def delay_minutes(actual, scheduled):
    """Minutes late for Darwin HH:MM[:SS] strings, wrapped past midnight into -720..719."""
    def minutes(t):
        return f"(int(split({t}, ':')[0]) * 60 + int(split({t}, ':')[1]))"
    return (f"CAST(pmod({minutes(actual)} - {minutes(scheduled)} + 720, 1440) - 720 AS INT)")


def scheduled_timestamp(time_col):
    """ssd + HH:MM as a UK-local timestamp, rolling over a day for calls after midnight."""
    return (f"to_timestamp(concat(c.ssd, ' ', left({time_col}, 5))) + "
            f"IF(left({time_col}, 5) < left(j.origin_departure, 5), INTERVAL 1 DAY, INTERVAL 0 DAY)")


@dlt.table(
    name="fact_train_calls",
    comment="Gold: one row per train call (rid x location x scheduled time) with the latest actual/"
            "estimated times and delays vs the public timetable (working timetable for passes). "
            f"Last {GOLD_DAYS} days.",
    cluster_by=["ssd", "toc"],
)
@dlt.expect("known_schedule", "toc IS NOT NULL")
def fact_train_calls():
    return spark.sql(f"""
      WITH calls AS (
        SELECT rid, ssd, tiploc, wta, wtd, wtp,
               max(pta) AS pta, max(ptd) AS ptd,
               {latest('arr_at')}, {latest('arr_et')},
               {latest('dep_at')}, {latest('dep_et')},
               {latest('pass_at')}, {latest('pass_et')},
               {latest('platform')}, {latest('train_length')},
               count(*) AS update_count,
               max(darwin_ts) AS last_update_ts
        FROM train_status_locations
        WHERE tiploc IS NOT NULL AND ssd >= current_date() - INTERVAL {GOLD_DAYS} DAYS
        GROUP BY rid, ssd, tiploc, wta, wtd, wtp
      ),
      reasons AS (
        SELECT rid, {latest('late_reason_code')}
        FROM train_status_locations
        WHERE ssd >= current_date() - INTERVAL {GOLD_DAYS} DAYS
        GROUP BY rid
      )
      SELECT
        sha2(concat_ws('|', c.rid, c.tiploc,
                       coalesce(c.wta, '-'), coalesce(c.wtd, '-'), coalesce(c.wtp, '-')), 256) AS call_id,
        c.rid, c.ssd, j.headcode, j.toc, j.train_category, coalesce(j.is_passenger, true) AS is_passenger,
        j.origin_tiploc, origin.location_name AS origin_name,
        j.destination_tiploc, dest.location_name AS destination_name,
        c.tiploc,
        CASE WHEN c.wtp IS NOT NULL THEN 'Pass'
             WHEN c.pta IS NOT NULL OR c.ptd IS NOT NULL THEN 'Public stop'
             ELSE 'Operational stop' END AS call_type,
        coalesce(c.pta, c.wta) AS scheduled_arrival,
        coalesce(c.ptd, c.wtd) AS scheduled_departure,
        c.wtp AS scheduled_pass,
        {scheduled_timestamp('coalesce(c.ptd, c.wtd, c.wtp, c.pta, c.wta)')} AS scheduled_ts,
        c.arr_at AS actual_arrival, c.dep_at AS actual_departure, c.pass_at AS actual_pass,
        c.arr_et AS estimated_arrival, c.dep_et AS estimated_departure, c.pass_et AS estimated_pass,
        {delay_minutes('c.arr_at', 'coalesce(c.pta, c.wta)')} AS arrival_delay_min,
        {delay_minutes('c.dep_at', 'coalesce(c.ptd, c.wtd)')} AS departure_delay_min,
        {delay_minutes('c.pass_at', 'c.wtp')} AS pass_delay_min,
        c.platform, c.train_length,
        r.late_reason_code,
        c.update_count, c.last_update_ts
      FROM calls c
      LEFT JOIN workspace.darwin.schedules j ON j.rid = c.rid
      LEFT JOIN reasons r ON r.rid = c.rid
      LEFT JOIN workspace.darwin.locations origin ON origin.tiploc = j.origin_tiploc
      LEFT JOIN workspace.darwin.locations dest ON dest.tiploc = j.destination_tiploc
    """)
