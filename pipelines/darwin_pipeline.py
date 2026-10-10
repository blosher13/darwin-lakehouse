# Lakeflow (DLT) pipeline: Darwin bronze -> silver -> gold.
#
# silver  train_status_locations: one row per (TS message, TS element, location) parsed from
#         Darwin Train Status messages in <catalog>.<schema>.pushport_bronze. Incremental: each
#         run only reads bronze rows appended since the last run.
# gold    fact_train_calls: one row per train call (train service x location x scheduled time) with the
#         latest known actual/estimated times, delays and platform. Feeds the Power BI model.

import dlt
from pyspark.sql import functions as F

# catalog.schema of bronze and the reference tables; set by darwin-deploy-pipeline
SCHEMA = spark.conf.get("darwin.schema", "workspace.darwin")
BRONZE = f"{SCHEMA}.pushport_bronze"

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

LINEAGE = ["message_id", "kafka_partition", "kafka_offset", "sequence_number", "published_timestamp", "ingested_at"]


def time_columns(element, event):
    """Forecast/actual columns for one Darwin time element, e.g. arr -> estimated_arrival."""
    return [
        f"l.{element}._et AS estimated_{event}",
        f"l.{element}._wet AS working_estimated_{event}",
        f"l.{element}._at AS actual_{event}",
        f"l.{element}._atClass AS actual_{event}_class",
        f"coalesce(l.{element}._atRemoved, false) AS actual_{event}_removed",
        f"coalesce(l.{element}._delayed, false) AS {event}_unknown_delay",
        f"l.{element}._src AS {event}_source",
    ]


@dlt.table(
    name="train_status_locations",
    comment="Darwin Train Status (TS) forecasts and actuals, one row per message/train/location. "
            "Deduplicated on message_id.",
    cluster_by=["scheduled_start_date", "train_service_id"],
)
@dlt.expect_or_fail("train_service_id_present", "train_service_id IS NOT NULL")
@dlt.expect("has_location", "location_code IS NOT NULL")
def train_status_locations():
    messages = (
        spark.readStream.table(BRONZE)
        .where("message_type = 'TS'")
        # Bronze is at-least-once: a replay after a crash re-sends rows within seconds. Dedupe on
        # the broker's message ID - Kafka offsets restart if the Kafka cluster is ever recreated.
        .withWatermark("ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["message_id"])
    )

    parsed = messages.select(
        *LINEAGE,
        F.expr(
            # Darwin's namespace prefixes (ns5: etc.) are not stable; drop them before parsing
            f"""from_xml(regexp_replace(message_xml, '<(/?)ns[0-9]+:', '<$1'),
                         '{TS_SCHEMA}', map('mode', 'FAILFAST'))"""
        ).alias("p"),
    )

    trains = parsed.selectExpr(
        *LINEAGE,
        "p._ts AS darwin_timestamp",
        "p.uR._updateOrigin AS update_origin",
        "p.uR._requestSource AS request_source",
        "posexplode(p.uR.TS) AS (train_status_index, t)",
    )

    locations = trains.selectExpr("*", "posexplode_outer(t.Location) AS (location_index, l)")

    return locations.selectExpr(
        "message_id", "kafka_partition", "kafka_offset", "train_status_index", "location_index",
        "sequence_number", "published_timestamp", "darwin_timestamp", "ingested_at",
        "update_origin", "request_source",
        "t._rid AS train_service_id", "t._uid AS schedule_id", "t._ssd AS scheduled_start_date",
        "t.LateReason._VALUE AS late_reason_code", "t.LateReason._tiploc AS late_reason_location_code",
        "coalesce(t._isReverseFormation, false) AS is_reverse_formation",
        "l._tpl AS location_code",
        "l._wta AS working_scheduled_arrival", "l._wtd AS working_scheduled_departure",
        "l._wtp AS working_scheduled_pass",
        "l._pta AS public_scheduled_arrival", "l._ptd AS public_scheduled_departure",
        *time_columns("arr", "arrival"), *time_columns("dep", "departure"), *time_columns("pass", "pass"),
        "l.plat._VALUE AS platform",
        "coalesce(l.plat._platsup, false) AS platform_suppressed",
        "coalesce(l.plat._conf, false) AS platform_confirmed",
        "l.plat._platsrc AS platform_source",
        "coalesce(l.suppr, false) AS service_suppressed",
        "nullif(l.length, 0) AS train_length",
        "coalesce(l.detachFront, false) AS detaches_from_front",
    )


GOLD_DAYS = 30  # fact_train_calls covers the last N days to bound recompute cost


def latest(column, alias=None):
    """Most recent non-null value of a column across a train call's TS updates."""
    return f"max_by({column}, IF({column} IS NULL, NULL, sequence_number)) AS {alias or column}"


def delay_minutes(actual, scheduled):
    """Minutes late for Darwin HH:MM[:SS] strings, wrapped past midnight into -720..719."""
    def minutes(t):
        return f"(int(split({t}, ':')[0]) * 60 + int(split({t}, ':')[1]))"
    return (f"CAST(pmod({minutes(actual)} - {minutes(scheduled)} + 720, 1440) - 720 AS INT)")


def scheduled_timestamp(time_col):
    """Start date + HH:MM as a UK-local timestamp, rolling over a day for calls after midnight."""
    return (f"to_timestamp(concat(c.scheduled_start_date, ' ', left({time_col}, 5))) + "
            f"IF(left({time_col}, 5) < left(j.origin_scheduled_departure, 5), INTERVAL 1 DAY, INTERVAL 0 DAY)")


@dlt.table(
    name="fact_train_calls",
    comment="Gold: one row per train call (train service x location x scheduled time) with the latest "
            "actual/estimated times and delays vs the public timetable (working timetable for passes). "
            f"Last {GOLD_DAYS} days.",
    cluster_by=["scheduled_start_date", "operator_code"],
)
@dlt.expect("known_schedule", "operator_code IS NOT NULL")
def fact_train_calls():
    return spark.sql(f"""
      WITH calls AS (
        SELECT train_service_id, scheduled_start_date, location_code,
               working_scheduled_arrival, working_scheduled_departure, working_scheduled_pass,
               max(public_scheduled_arrival) AS public_scheduled_arrival,
               max(public_scheduled_departure) AS public_scheduled_departure,
               {latest('actual_arrival')}, {latest('estimated_arrival')},
               {latest('actual_departure')}, {latest('estimated_departure')},
               {latest('actual_pass')}, {latest('estimated_pass')},
               {latest('platform')}, {latest('train_length')},
               count(*) AS update_count,
               max(darwin_timestamp) AS last_update_timestamp
        FROM train_status_locations
        WHERE location_code IS NOT NULL
          AND scheduled_start_date >= current_date() - INTERVAL {GOLD_DAYS} DAYS
        GROUP BY train_service_id, scheduled_start_date, location_code,
                 working_scheduled_arrival, working_scheduled_departure, working_scheduled_pass
      ),
      reasons AS (
        SELECT train_service_id, {latest('late_reason_code')}
        FROM train_status_locations
        WHERE scheduled_start_date >= current_date() - INTERVAL {GOLD_DAYS} DAYS
        GROUP BY train_service_id
      )
      SELECT
        sha2(concat_ws('|', c.train_service_id, c.location_code,
                       coalesce(c.working_scheduled_arrival, '-'), coalesce(c.working_scheduled_departure, '-'),
                       coalesce(c.working_scheduled_pass, '-')), 256) AS call_id,
        c.train_service_id, c.scheduled_start_date, j.headcode, j.operator_code, j.train_category,
        coalesce(j.is_passenger_service, true) AS is_passenger_service,
        j.origin_location_code, origin.location_name AS origin_name,
        j.destination_location_code, destination.location_name AS destination_name,
        c.location_code,
        CASE WHEN c.working_scheduled_pass IS NOT NULL THEN 'Pass'
             WHEN c.public_scheduled_arrival IS NOT NULL OR c.public_scheduled_departure IS NOT NULL
               THEN 'Public stop'
             ELSE 'Operational stop' END AS call_type,
        coalesce(c.public_scheduled_arrival, c.working_scheduled_arrival) AS scheduled_arrival,
        coalesce(c.public_scheduled_departure, c.working_scheduled_departure) AS scheduled_departure,
        c.working_scheduled_pass AS scheduled_pass,
        {scheduled_timestamp('coalesce(c.public_scheduled_departure, c.working_scheduled_departure, '
                             'c.working_scheduled_pass, c.public_scheduled_arrival, c.working_scheduled_arrival)')}
          AS scheduled_timestamp,
        c.actual_arrival, c.actual_departure, c.actual_pass,
        c.estimated_arrival, c.estimated_departure, c.estimated_pass,
        {delay_minutes('c.actual_arrival', 'coalesce(c.public_scheduled_arrival, c.working_scheduled_arrival)')}
          AS arrival_delay_minutes,
        {delay_minutes('c.actual_departure', 'coalesce(c.public_scheduled_departure, c.working_scheduled_departure)')}
          AS departure_delay_minutes,
        {delay_minutes('c.actual_pass', 'c.working_scheduled_pass')} AS pass_delay_minutes,
        c.platform, c.train_length,
        r.late_reason_code,
        c.update_count, c.last_update_timestamp
      FROM calls c
      LEFT JOIN {SCHEMA}.schedules j ON j.train_service_id = c.train_service_id
      LEFT JOIN reasons r ON r.train_service_id = c.train_service_id
      LEFT JOIN {SCHEMA}.locations origin ON origin.location_code = j.origin_location_code
      LEFT JOIN {SCHEMA}.locations destination ON destination.location_code = j.destination_location_code
    """)
