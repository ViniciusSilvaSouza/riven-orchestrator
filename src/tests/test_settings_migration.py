import json
import os

import program.settings as settings_module
from program.settings import SettingsManager
from program.settings.models import get_version


def test_load_and_migrate_legacy_settings_file(tmp_path, monkeypatch):
    settings_file = tmp_path / "settings.json"
    legacy_settings = {
        "version": "0.7.5",
        "log_level": True,
        "tracemalloc": False,
        "downloaders": {
            "real_debrid": {
                "enabled": False,
                "api_key": "",
            },
            "all_debrid": {
                "enabled": True,
                "api_key": "12345678",
                "proxy_url": "https://no_proxy.com",
            },
        },
    }
    settings_file.write_text(json.dumps(legacy_settings), encoding="utf-8")
    monkeypatch.setattr(settings_module, "data_dir_path", tmp_path)
    for key in list(os.environ):
        if key == "API_KEY" or key.startswith("RIVEN_"):
            monkeypatch.delenv(key, raising=False)

    settings_manager = SettingsManager()

    assert settings_manager.settings.log_level == "DEBUG"
    assert settings_manager.settings.tracemalloc is False
    assert settings_manager.settings.downloaders.real_debrid.enabled is False
    assert settings_manager.settings.downloaders.all_debrid.enabled is True
    assert settings_manager.settings.downloaders.all_debrid.api_key == "12345678"
    assert not hasattr(settings_manager.settings.downloaders.all_debrid, "proxy_url")
    assert str(settings_manager.settings.database.host) == (
        "postgresql+psycopg2://postgres:postgres@localhost/riven"
    )
    assert settings_manager.settings.version == get_version()

    persisted_settings = json.loads(settings_file.read_text(encoding="utf-8"))
    assert persisted_settings["version"] == get_version()
