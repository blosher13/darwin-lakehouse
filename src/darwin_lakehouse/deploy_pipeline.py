"""Deploys (or updates) the medallion pipeline and its schedule in Databricks.

* uploads pipelines/darwin_pipeline.py to /Workspace/Users/<you>/darwin/
* creates/updates serverless Lakeflow pipeline "darwin-medallion" (silver + gold, workspace.darwin)
* creates/updates job "darwin-medallion-refresh" running it hourly

    darwin-deploy-pipeline [--run-now]
"""

from __future__ import annotations

import argparse
import base64
import logging
import sys
import time

from darwin_lakehouse.config import PROJECT_ROOT, Settings
from darwin_lakehouse.databricks import DatabricksClient, DatabricksError

log = logging.getLogger("darwin_lakehouse.deploy")

PIPELINE_NAME = "darwin-medallion"
JOB_NAME = "darwin-medallion-refresh"
PIPELINE_SOURCE = PROJECT_ROOT / "pipelines" / "darwin_pipeline.py"
SCHEDULE_CRON = "0 0 * * * ?"  # hourly, Quartz syntax


def deploy(db: DatabricksClient) -> str:
    folder = f"/Workspace/Users/{db.current_user}/darwin"
    source_path = f"{folder}/{PIPELINE_SOURCE.name}"
    db.api("POST", "/api/2.0/workspace/mkdirs", {"path": folder})
    db.api("POST", "/api/2.0/workspace/import", {
        "path": source_path, "format": "AUTO", "overwrite": True,
        "content": base64.b64encode(PIPELINE_SOURCE.read_bytes()).decode()})
    log.info("Uploaded %s", source_path)

    settings = {
        "name": PIPELINE_NAME, "serverless": True, "catalog": "workspace", "schema": "darwin",
        "continuous": False, "development": False, "channel": "CURRENT",
        "libraries": [{"file": {"path": source_path}}],
    }
    pipelines = db.api("GET", "/api/2.0/pipelines?max_results=100").get("statuses", [])
    existing = next((p for p in pipelines if p["name"] == PIPELINE_NAME), None)
    if existing:
        pipeline_id = existing["pipeline_id"]
        db.api("PUT", f"/api/2.0/pipelines/{pipeline_id}", {"id": pipeline_id, **settings})
        log.info("Updated pipeline %s", pipeline_id)
    else:
        pipeline_id = db.api("POST", "/api/2.0/pipelines", settings)["pipeline_id"]
        log.info("Created pipeline %s", pipeline_id)

    job = {
        "name": JOB_NAME,
        "tasks": [{"task_key": "refresh", "pipeline_task": {"pipeline_id": pipeline_id}}],
        "schedule": {"quartz_cron_expression": SCHEDULE_CRON, "timezone_id": "UTC", "pause_status": "UNPAUSED"},
        "max_concurrent_runs": 1,
    }
    jobs = db.api("GET", f"/api/2.2/jobs/list?name={JOB_NAME}").get("jobs", [])
    if jobs:
        db.api("POST", "/api/2.2/jobs/reset", {"job_id": jobs[0]["job_id"], "new_settings": job})
        log.info("Updated job %s", jobs[0]["job_id"])
    else:
        log.info("Created job %s", db.api("POST", "/api/2.2/jobs/create", job)["job_id"])
    return pipeline_id


TERMINAL_STATES = ("COMPLETED", "FAILED", "CANCELED")


def wait_for_update(db: DatabricksClient, pipeline_id: str, update_id: str) -> str:
    while True:
        state = db.api("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]["state"]
        if state in TERMINAL_STATES:
            return state
        time.sleep(20)


def start_update(db: DatabricksClient, pipeline_id: str, attempts: int = 5) -> str:
    """Starts an update, first letting any running one (e.g. the hourly job) finish."""
    for _ in range(attempts):
        latest = db.api("GET", f"/api/2.0/pipelines/{pipeline_id}").get("latest_updates", [])
        if latest and latest[0]["state"] not in TERMINAL_STATES:
            log.info("Waiting for running update %s", latest[0]["update_id"])
            wait_for_update(db, pipeline_id, latest[0]["update_id"])
        try:
            return db.api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", {})["update_id"]
        except DatabricksError as e:
            if "ACTIVE_UPDATE_EXISTS" not in str(e):
                raise
    raise DatabricksError(f"Pipeline {pipeline_id} stayed busy; try again later")


def run_and_wait(db: DatabricksClient, pipeline_id: str) -> bool:
    update_id = start_update(db, pipeline_id)
    log.info("Started pipeline update %s", update_id)
    state = wait_for_update(db, pipeline_id, update_id)
    log.info("Pipeline update %s", state)
    if state != "COMPLETED":
        for event in db.api("GET", f"/api/2.0/pipelines/{pipeline_id}/events?max_results=100").get("events", []):
            if event.get("level") == "ERROR" and event.get("origin", {}).get("update_id") == update_id:
                exceptions = (event.get("error") or {}).get("exceptions") or [{}]
                log.error("%s %s", event.get("message"), exceptions[0].get("message", "")[:1000])
    return state == "COMPLETED"


def main() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=logging.INFO)
    parser = argparse.ArgumentParser(description="Deploy the Darwin medallion pipeline to Databricks")
    parser.add_argument("--run-now", action="store_true", help="run the pipeline once and wait for it")
    args = parser.parse_args()

    db = DatabricksClient.from_settings(Settings())
    pipeline_id = deploy(db)
    if args.run_now and not run_and_wait(db, pipeline_id):
        sys.exit(1)


if __name__ == "__main__":
    main()
