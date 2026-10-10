-- Applied by sync.py after every load (statements are split on ";\n").
-- {schema} is replaced with the configured catalog.schema, e.g. workspace.darwin.

CREATE OR REPLACE FUNCTION {schema}.darwin_delay_minutes(actual STRING, scheduled STRING)
RETURNS INT
COMMENT 'Minutes from scheduled to actual for Darwin HH:MM[:SS] times, wrapped past midnight into -720..719'
RETURN CASE WHEN actual IS NULL OR scheduled IS NULL THEN NULL ELSE CAST(
  pmod((int(split(actual, ':')[0]) * 60 + int(split(actual, ':')[1]))
     - (int(split(scheduled, ':')[0]) * 60 + int(split(scheduled, ':')[1])) + 720, 1440) - 720 AS INT) END;

CREATE OR REPLACE VIEW {schema}.train_status_decoded
COMMENT 'train_status_locations with location codes, operators, journeys and reason codes decoded via Darwin reference and timetable data. Delays are vs the public timetable (working timetable for passes and non-public calls); positive = late.'
AS SELECT
  s.darwin_timestamp,
  s.sequence_number,
  s.train_service_id,
  s.scheduled_start_date,
  j.headcode,
  j.operator_code,
  operator.operator_name,
  j.train_category,
  j.is_passenger_service,
  j.origin_location_code,
  origin.location_name                                AS origin_name,
  j.origin_scheduled_departure,
  j.destination_location_code,
  destination.location_name                           AS destination_name,
  j.destination_scheduled_arrival,
  s.location_code,
  location.location_name,
  location.station_code,
  coalesce(s.public_scheduled_arrival, s.working_scheduled_arrival)       AS scheduled_arrival,
  coalesce(s.public_scheduled_departure, s.working_scheduled_departure)   AS scheduled_departure,
  s.working_scheduled_pass                                                 AS scheduled_pass,
  s.estimated_arrival, s.actual_arrival,
  s.estimated_departure, s.actual_departure,
  s.estimated_pass, s.actual_pass,
  {schema}.darwin_delay_minutes(coalesce(s.actual_arrival, s.estimated_arrival),
                                coalesce(s.public_scheduled_arrival, s.working_scheduled_arrival))     AS arrival_delay_minutes,
  {schema}.darwin_delay_minutes(coalesce(s.actual_departure, s.estimated_departure),
                                coalesce(s.public_scheduled_departure, s.working_scheduled_departure)) AS departure_delay_minutes,
  {schema}.darwin_delay_minutes(coalesce(s.actual_pass, s.estimated_pass), s.working_scheduled_pass)    AS pass_delay_minutes,
  s.actual_arrival IS NOT NULL OR s.actual_departure IS NOT NULL OR s.actual_pass IS NOT NULL           AS is_actual,
  s.arrival_unknown_delay OR s.departure_unknown_delay OR s.pass_unknown_delay                          AS has_unknown_delay,
  s.platform,
  s.platform_suppressed,
  s.service_suppressed,
  s.train_length,
  s.late_reason_code,
  late_reason.reason_text                             AS late_reason,
  s.update_origin,
  s.message_id, s.train_status_index, s.location_index
FROM {schema}.train_status_locations s
LEFT JOIN {schema}.schedules j                   ON j.train_service_id = s.train_service_id
LEFT JOIN {schema}.operators operator            ON operator.operator_code = j.operator_code
LEFT JOIN {schema}.locations origin              ON origin.location_code = j.origin_location_code
LEFT JOIN {schema}.locations destination         ON destination.location_code = j.destination_location_code
LEFT JOIN {schema}.locations location            ON location.location_code = s.location_code
LEFT JOIN {schema}.late_reasons late_reason      ON late_reason.reason_code = s.late_reason_code
