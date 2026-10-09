import pytest

from darwin_lakehouse.config import Settings


@pytest.fixture
def env_file(tmp_path):
    path = tmp_path / ".env"
    path.write_text("username = DARWINabc\n"
                    "databricks_host=https://dbc-1.cloud.databricks.com/settings/workspace?o=42\n"
                    "empty=\n")
    return path


def test_values_are_trimmed(env_file):
    assert Settings(env_file).require("username") == "DARWINabc"


def test_databricks_url_drops_path_and_query(env_file):
    assert Settings(env_file).databricks_url == "https://dbc-1.cloud.databricks.com"


def test_missing_and_empty_keys(env_file):
    settings = Settings(env_file)
    assert settings.get("empty", "fallback") == "fallback"
    with pytest.raises(KeyError, match="empty"):
        settings.require("empty")
    with pytest.raises(KeyError, match="nope"):
        settings.require("nope")


def test_missing_file_explains_what_to_do(tmp_path):
    with pytest.raises(FileNotFoundError, match=".env.example"):
        Settings(tmp_path / ".env")


def test_append_persists_and_updates(env_file):
    settings = Settings(env_file)
    settings.append({"zerobus_endpoint": "1.zerobus.us-east-2.cloud.databricks.com"})

    assert settings.require("zerobus_endpoint").startswith("1.zerobus")
    assert Settings(env_file).require("zerobus_endpoint").startswith("1.zerobus")
