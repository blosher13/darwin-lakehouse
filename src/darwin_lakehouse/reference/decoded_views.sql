-- Applied by sync.py after every load (statements are split on ";\n").

CREATE OR REPLACE FUNCTION workspace.darwin.darwin_delay_minutes(actual STRING, scheduled STRING)
RETURNS INT
COMMENT 'Minutes from scheduled to actual for Darwin HH:MM[:SS] times, wrapped past midnight into -720..719'
RETURN CASE WHEN actual IS NULL OR scheduled IS NULL THEN NULL ELSE CAST(
  pmod((int(split(actual, ':')[0]) * 60 + int(split(actual, ':')[1]))
     - (int(split(scheduled, ':')[0]) * 60 + int(split(scheduled, ':')[1])) + 720, 1440) - 720 AS INT) END;

CREATE OR REPLACE VIEW workspace.darwin.train_status_decoded
COMMENT 'train_status_locations with TIPLOCs, operators, journeys and reason codes decoded via Darwin reference and timetable data. Delays are vs the public timetable (working timetable for passes and non-public calls); positive = late.'
AS SELECT
  s.darwin_ts,
  s.sequence_number,
  s.rid,
  s.ssd,
  j.headcode,
  j.toc,
  op.toc_name                                AS operator,
  j.train_category,
  j.is_passenger,
  j.origin_tiploc,
  origin.location_name                       AS origin,
  j.origin_departure,
  j.destination_tiploc,
  dest.location_name                         AS destination,
  j.destination_arrival,
  s.tiploc,
  loc.location_name                          AS location,
  loc.crs,
  coalesce(s.pta, s.wta)                     AS scheduled_arrival,
  coalesce(s.ptd, s.wtd)                     AS scheduled_departure,
  s.wtp                                      AS scheduled_pass,
  s.arr_et, s.arr_at,
  s.dep_et, s.dep_at,
  s.pass_et, s.pass_at,
  workspace.darwin.darwin_delay_minutes(coalesce(s.arr_at, s.arr_et), coalesce(s.pta, s.wta))   AS arrival_delay_min,
  workspace.darwin.darwin_delay_minutes(coalesce(s.dep_at, s.dep_et), coalesce(s.ptd, s.wtd))   AS departure_delay_min,
  workspace.darwin.darwin_delay_minutes(coalesce(s.pass_at, s.pass_et), s.wtp)                  AS pass_delay_min,
  s.arr_at IS NOT NULL OR s.dep_at IS NOT NULL OR s.pass_at IS NOT NULL                          AS is_actual,
  s.arr_delayed OR s.dep_delayed OR s.pass_delayed                                               AS is_delayed_unknown,
  s.platform,
  s.platform_suppressed,
  s.suppressed,
  s.train_length,
  s.late_reason_code,
  lr.reason_text                             AS late_reason,
  s.update_origin,
  s.kafka_partition, s.kafka_offset, s.ts_pos, s.loc_pos
FROM workspace.darwin.train_status_locations s
LEFT JOIN workspace.darwin.schedules j          ON j.rid = s.rid
LEFT JOIN workspace.darwin.operators op         ON op.toc = j.toc
LEFT JOIN workspace.darwin.locations origin     ON origin.tiploc = j.origin_tiploc
LEFT JOIN workspace.darwin.locations dest       ON dest.tiploc = j.destination_tiploc
LEFT JOIN workspace.darwin.locations loc        ON loc.tiploc = s.tiploc
LEFT JOIN workspace.darwin.late_reasons lr      ON lr.code = s.late_reason_code
