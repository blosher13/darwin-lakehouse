"""One-off, idempotent Databricks workspace setup.

* creates schema workspace.darwin and the bronze Delta table Zerobus writes to
* creates service principal "darwin-zerobus" with the grants Zerobus needs, plus an OAuth secret
* works out the workspace's Zerobus endpoint
* appends databricks_client_id / databricks_client_secret / zerobus_endpoint to .env

    darwin-setup
"""

from __future__ import annotations

import logging

from darwin_lakehouse.config import Settings
from darwin_lakehouse.databricks import DatabricksClient

log = logging.getLogger("darwin_lakehouse.setup")

CATALOG = "workspace"
SCHEMA = "darwin"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.pushport_bronze"
SERVICE_PRINCIPAL = "darwin-zerobus"

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  kafka_partition INT       COMMENT 'Kafka partition the record was read from',
  kafka_offset    BIGINT    COMMENT 'Kafka offset of the record',
  message_id      STRING    COMMENT 'JMS message ID (globally unique)',
  sequence_number BIGINT    COMMENT 'Darwin SequenceNumber header',
  message_type    STRING    COMMENT 'Darwin MessageType header, e.g. TS, SC, AS',
  jms_timestamp   TIMESTAMP COMMENT 'When Darwin published the message',
  ingested_at     TIMESTAMP COMMENT 'When the Kafka Streams app sent the row to Zerobus',
  xml             STRING    COMMENT 'Decompressed Darwin Push Port v16 XML'
) COMMENT 'Darwin Push Port v16 messages, one row per message (bronze)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""


def zerobus_endpoint(db: DatabricksClient) -> str:
    """<workspace-id>.zerobus.<region>.cloud.databricks.com (AWS workspaces)."""
    workspace_id = db.session.get(db.url + "/api/2.0/preview/scim/v2/Me").headers["x-databricks-org-id"]
    region = db.api("GET", "/api/2.1/unity-catalog/metastore_summary")["region"]
    return f"{workspace_id}.zerobus.{region}.cloud.databricks.com"


def ensure_service_principal(db: DatabricksClient) -> dict:
    query = f'/api/2.0/preview/scim/v2/ServicePrincipals?filter=displayName%20eq%20%22{SERVICE_PRINCIPAL}%22'
    existing = db.api("GET", query).get("Resources") or []
    if existing:
        return existing[0]
    return db.api("POST", "/api/2.0/preview/scim/v2/ServicePrincipals",
                  {"displayName": SERVICE_PRINCIPAL, "active": True})


def main() -> None:
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=logging.INFO)
    settings = Settings()
    db = DatabricksClient.from_settings(settings)

    db.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    db.sql(BRONZE_DDL)
    log.info("Schema %s.%s and table %s ready", CATALOG, SCHEMA, BRONZE_TABLE)

    sp = ensure_service_principal(db)
    app_id = sp["applicationId"]
    db.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{app_id}`")
    db.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{app_id}`")
    db.sql(f"GRANT SELECT, MODIFY ON TABLE {BRONZE_TABLE} TO `{app_id}`")
    log.info("Service principal %s (%s) granted access", SERVICE_PRINCIPAL, app_id)

    new_values = {}
    if not (settings.get("databricks_client_id") == app_id and settings.get("databricks_client_secret")):
        secret = db.api("POST", f"/api/2.0/accounts/servicePrincipals/{sp['id']}/credentials/secrets", {})
        new_values.update(databricks_client_id=app_id, databricks_client_secret=secret["secret"])
    if not settings.get("zerobus_endpoint"):
        new_values["zerobus_endpoint"] = zerobus_endpoint(db)
    if new_values:
        settings.append(new_values)
        log.info("Wrote %s to %s", ", ".join(new_values), settings.env_file)
    else:
        log.info("%s already complete", settings.env_file)


if __name__ == "__main__":
    main()
