from darwin_lakehouse.reference.sync import decoded_views_sql


def test_decoded_views_use_the_configured_schema():
    sql = decoded_views_sql("main.rail")
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))

    assert "{schema}" not in code
    assert "workspace.darwin" not in code
    assert "CREATE OR REPLACE VIEW main.rail.train_status_decoded" in sql
    assert "FROM main.rail.train_status_locations" in sql
