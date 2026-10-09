from darwin_lakehouse.databricks import DatabricksClient


def test_sql_script_runs_each_statement_and_skips_comment_only_chunks(monkeypatch):
    client = DatabricksClient("https://example.cloud.databricks.com", "token")
    executed = []
    monkeypatch.setattr(client, "sql", executed.append)

    client.sql_script("-- header comment\nCREATE FUNCTION f() RETURNS INT RETURN 1;\n"
                      "CREATE VIEW v AS SELECT 1;\n-- trailing comment only\n")

    assert executed == ["-- header comment\nCREATE FUNCTION f() RETURNS INT RETURN 1",
                        "CREATE VIEW v AS SELECT 1"]
