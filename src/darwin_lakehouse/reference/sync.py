"""Darwin reference & timetable sync: S3 -> parse -> Unity Catalog Volume -> Delta tables.

Darwin publishes a reference file and a timetable file to S3 daily. This picks up the newest
pair, parses it, uploads gzip JSON-lines files to a Volume and loads them with the SQL
warehouse. Already-loaded timetables are skipped, so it is safe to run as often as you like.

    darwin-reference-sync --once     # single run
    darwin-reference-sync            # loop every SYNC_INTERVAL_HOURS (default 6)
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import tempfile
import time
from contextlib import ExitStack
from importlib import resources
from pathlib import Path

import boto3

from darwin_lakehouse.config import Settings
from darwin_lakehouse.databricks import DatabricksClient
from darwin_lakehouse.reference.parser import iter_timetable, parse_reference

log = logging.getLogger("darwin_lakehouse.reference")

BUCKET = "darwin.xmltimetable"
PREFIX = "PPTimetable/"
SCHEMA = "workspace.darwin"
VOLUME = "/Volumes/workspace/darwin/timetable_files"
KEEP_VOLUME_DAYS = 7

REFERENCE_TABLES = {
    "locations": ("tiploc STRING, crs STRING, toc STRING, location_name STRING",
                  "Darwin locations: TIPLOC -> CRS code and name"),
    "operators": ("toc STRING, toc_name STRING, url STRING", "Train operating companies"),
    "late_reasons": ("code STRING, reason_text STRING", "Darwin late-running reason codes"),
    "cancellation_reasons": ("code STRING, reason_text STRING", "Darwin cancellation reason codes"),
    "vias": ("at_crs STRING, destination_tiploc STRING, loc1_tiploc STRING, loc2_tiploc STRING, via_text STRING",
             "Via texts shown on departure boards"),
    "cis_sources": ("code STRING, name STRING", "Customer Information System sources"),
}

TIMETABLE_TABLES = {
    "schedules": (
        "rid STRING, uid STRING, headcode STRING, ssd DATE, toc STRING, train_category STRING, status STRING, "
        "is_passenger BOOLEAN, is_charter BOOLEAN, is_q_train BOOLEAN, is_deleted BOOLEAN, is_cancelled BOOLEAN, "
        "cancel_reason_code STRING, cancel_reason_tiploc STRING, origin_tiploc STRING, origin_departure STRING, "
        "destination_tiploc STRING, destination_arrival STRING, calling_points INT",
        "Darwin planned journeys, one row per rid"),
    "schedule_locations": (
        "rid STRING, ssd DATE, seq INT, tiploc STRING, location_type STRING, activity STRING, "
        "planned_activity STRING, platform STRING, pta STRING, ptd STRING, wta STRING, wtd STRING, wtp STRING, "
        "is_cancelled BOOLEAN, false_destination_tiploc STRING, route_delay INT",
        "Darwin planned calling points in route order (seq)"),
    "associations": (
        "main_rid STRING, assoc_rid STRING, category STRING, tiploc STRING, is_cancelled BOOLEAN, "
        "is_deleted BOOLEAN, main_time STRING, assoc_time STRING",
        "Darwin associations: joins (JJ), splits (VV) and next workings (NP)"),
}


def _source(folder: str, table: str, schema: str) -> str:
    return f"read_files('{folder}/{table}.json.gz', format => 'json', schema => '{schema}')"


class ReferenceSync:
    def __init__(self, settings: Settings):
        self.db = DatabricksClient.from_settings(settings)
        self.s3 = boto3.client(
            "s3", region_name="eu-west-1",
            aws_access_key_id=settings.require("darwin_s3_access_key"),
            aws_secret_access_key=settings.require("darwin_s3_secret_key"))

    def latest_timetable_id(self) -> str:
        keys = [o["Key"] for o in self.s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])]
        return max(k[len(PREFIX):len(PREFIX) + 14] for k in keys)

    def run(self) -> None:
        timetable_id = self.latest_timetable_id()
        self.db.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.timetable_files")
        self.db.sql(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.timetable_loads (timetable_id STRING, loaded_at TIMESTAMP)")
        if self.db.sql(f"SELECT 1 FROM {SCHEMA}.timetable_loads WHERE timetable_id = '{timetable_id}'"):
            log.info("Timetable %s already loaded", timetable_id)
            return

        log.info("Loading timetable %s", timetable_id)
        folder = f"{VOLUME}/timetable_id={timetable_id}"
        with tempfile.TemporaryDirectory() as tmp:
            self._extract(timetable_id, Path(tmp))
            for f in Path(tmp).glob("*.json.gz"):
                self.db.upload(f, f"{folder}/{f.name}")
        log.info("Uploaded to %s", folder)

        self._load(folder)
        self.db.sql_script(resources.files("darwin_lakehouse.reference").joinpath("decoded_views.sql").read_text())
        self.db.sql(f"INSERT INTO {SCHEMA}.timetable_loads VALUES ('{timetable_id}', current_timestamp())")
        self._prune_volume()
        log.info("Timetable %s loaded", timetable_id)

    def _extract(self, timetable_id: str, tmp: Path) -> None:
        """Downloads both files and writes one gzip JSON-lines file per target table."""
        ref, timetable = tmp / "ref.xml.gz", tmp / "timetable.xml.gz"
        self.s3.download_file(BUCKET, f"{PREFIX}{timetable_id}_ref_v4.xml.gz", str(ref))
        self.s3.download_file(BUCKET, f"{PREFIX}{timetable_id}_v8.xml.gz", str(timetable))

        for table, rows in parse_reference(ref).items():
            with gzip.open(tmp / f"{table}.json.gz", "wt") as f:
                f.writelines(json.dumps(row) + "\n" for row in rows)
            log.info("Parsed %s: %d rows", table, len(rows))

        counts = dict.fromkeys(TIMETABLE_TABLES, 0)
        with ExitStack() as stack:
            files = {t: stack.enter_context(gzip.open(tmp / f"{t}.json.gz", "wt")) for t in TIMETABLE_TABLES}
            for table, row in iter_timetable(timetable):
                files[table].write(json.dumps(row) + "\n")
                counts[table] += 1
        for table, count in counts.items():
            log.info("Parsed %s: %d rows", table, count)

    def _load(self, folder: str) -> None:
        for table, (schema, comment) in REFERENCE_TABLES.items():
            self.db.sql(f"CREATE OR REPLACE TABLE {SCHEMA}.{table} COMMENT '{comment}' AS "
                        f"SELECT * FROM {_source(folder, table, schema)}")

        # Timetables accumulate across days (a rid is unique to one day), so history stays joinable
        for table, (schema, comment) in TIMETABLE_TABLES.items():
            cluster = " CLUSTER BY (ssd, rid)" if table != "associations" else ""
            self.db.sql(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} ({schema}) COMMENT '{comment}'{cluster}")

        schedules = _source(folder, "schedules", TIMETABLE_TABLES["schedules"][0])
        self.db.sql(f"MERGE INTO {SCHEMA}.schedules t USING (SELECT * FROM {schedules}) s ON t.rid = s.rid "
                    f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        for table, key in (("schedule_locations", "rid"), ("associations", "main_rid")):
            self.db.sql(f"DELETE FROM {SCHEMA}.{table} WHERE {key} IN (SELECT rid FROM {schedules})")
            self.db.sql(f"INSERT INTO {SCHEMA}.{table} "
                        f"SELECT * FROM {_source(folder, table, TIMETABLE_TABLES[table][0])}")
        log.info("Loaded reference and timetable tables")

    def _prune_volume(self) -> None:
        folders = sorted(e["path"] for e in self.db.list_directory(VOLUME) if e.get("is_directory"))
        for folder in folders[:-KEEP_VOLUME_DAYS]:
            self.db.delete_directory(folder)
            log.info("Pruned %s", folder)


def main() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=logging.INFO)
    parser = argparse.ArgumentParser(description="Sync Darwin reference + timetable data into Databricks")
    parser.add_argument("--once", action="store_true", help="run a single sync and exit")
    args = parser.parse_args()

    sync = ReferenceSync(Settings())
    if args.once:
        sync.run()
        return
    interval = float(os.getenv("SYNC_INTERVAL_HOURS", "6")) * 3600
    while True:
        try:
            sync.run()
        except Exception:
            log.exception("Sync failed; retrying next interval")
        time.sleep(interval)


if __name__ == "__main__":
    main()
