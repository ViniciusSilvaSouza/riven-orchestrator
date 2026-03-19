import contextvars
import json
import os
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, cast

from loguru import logger
from pydantic import ValidationError
from RTN.models import SettingsModel

from program.settings.models import AppModel, Observable
from program.utils import data_dir_path


class SettingsManager:
    """Class that handles settings, ensuring they are validated against a Pydantic schema."""

    def __init__(self):
        self.observers = list[Callable[[], Any]]()
        self.filename = os.environ.get("SETTINGS_FILENAME", "settings.json")
        self.settings_file = data_dir_path / self.filename
        self._overrides_ctx: contextvars.ContextVar[dict[str, Any] | None] = (
            contextvars.ContextVar("settings_overrides", default=None)
        )

        Observable.set_notify_observers(self.notify_observers)

        if not self.settings_file.exists():
            logger.info(f"Settings filename: {self.filename}")

            self.settings = AppModel()
            self.settings = AppModel.model_validate(
                self.check_environment(
                    self.settings.model_dump(),
                    "RIVEN",
                )
            )

            self.notify_observers()
        else:
            self.load()

    def register_observer(self, observer: Callable[[], None]):
        self.observers.append(observer)

    def notify_observers(self):
        for observer in self.observers:
            observer()

    def _environment_candidates(self, prefix: str, key: str) -> list[str]:
        candidates = [f"{prefix}_{key}".upper()]
        if prefix == "RIVEN" and key == "api_key":
            candidates.append("API_KEY")
        return candidates

    def _environment_override(self, prefix: str, key: str) -> tuple[str, str] | None:
        for candidate in self._environment_candidates(prefix, key):
            override = os.getenv(candidate)
            if override is not None:
                return candidate, override
        return None

    def _coerce_environment_value(
        self,
        current_value: Any,
        override_value: str,
        environment_variable: str,
    ) -> Any:
        if isinstance(current_value, bool):
            return override_value.lower() == "true" or override_value == "1"
        if isinstance(current_value, int):
            return int(override_value)
        if isinstance(current_value, float):
            return float(override_value)
        if isinstance(current_value, list):
            if override_value.startswith("["):
                return json.loads(override_value)

            logger.error(
                f"Environment variable {environment_variable} for list type must be a JSON array string. Got {override_value}."
            )
            return current_value

        return override_value

    def check_environment(
        self,
        settings: dict[str, Any],
        prefix: str = "",
        separator: str = "_",
    ) -> dict[str, Any]:
        checked_settings = dict[str, Any]()

        for key, value in settings.items():
            if isinstance(value, dict):
                checked_settings[key] = self.check_environment(
                    settings=cast(dict[str, Any], value),
                    prefix=f"{prefix}{separator}{key}",
                    separator=separator,
                )
                continue

            override = self._environment_override(prefix, key)
            checked_settings[key] = (
                value
                if override is None
                else self._coerce_environment_value(value, override[1], override[0])
            )

        return checked_settings

    def load(self, settings_dict: dict[str, Any] | None = None):
        """Load settings from file, validating against the AppModel schema."""

        try:
            if not settings_dict:
                with open(self.settings_file, "r", encoding="utf-8") as file:
                    settings_dict = json.loads(file.read())

                    if (
                        settings_dict
                        and os.environ.get("RIVEN_FORCE_ENV", "false").lower() == "true"
                    ):
                        settings_dict = self.check_environment(
                            settings_dict,
                            "RIVEN",
                        )

            self.settings = AppModel.model_validate(settings_dict)
            self.save()
        except ValidationError as e:
            formatted_error = format_validation_error(e)
            logger.error(f"Settings validation failed:\n{formatted_error}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing settings file: {e}")
            raise
        except FileNotFoundError:
            logger.warning(
                f"Error loading settings: {self.settings_file} does not exist"
            )
            raise
        self.notify_observers()

    def save(self):
        """Save settings to file, using Pydantic model for JSON serialization."""
        with open(self.settings_file, "w", encoding="utf-8") as file:
            file.write(self.settings.model_dump_json(indent=4, exclude_none=True))

    @contextmanager
    def override(self, **overrides: Any) -> Generator[None, None, None]:
        """Context manager to temporarily override settings."""
        old_overrides = self._overrides_ctx.get() or {}
        token = self._overrides_ctx.set({**old_overrides, **overrides})
        try:
            yield
        finally:
            try:
                self._overrides_ctx.reset(token)
            except ValueError:
                # Handle cases where the context has changed (e.g., across thread/task boundaries)
                logger.trace("Context mismatch during override reset, manually restoring old overrides")
                self._overrides_ctx.set(old_overrides)

    def get_setting(self, key: str, default: Any) -> Any:
        """Get a setting value, respecting any active overrides."""
        overrides = self._overrides_ctx.get() or {}
        if overrides and key in overrides:
            return overrides[key]
        return default

    def get_effective_rtn_model(self):
        """Get the effective RTN settings, merging global settings with active overrides."""
        # Start with global settings
        ranking_settings = self.settings.ranking.model_dump()

        # Apply overrides
        overrides = self._overrides_ctx.get() or {}
        if overrides:
            valid_keys = SettingsModel.model_fields.keys()
            filtered_overrides = {k: v for k, v in overrides.items() if k in valid_keys}
            ranking_settings.update(filtered_overrides)

        return SettingsModel(**ranking_settings)



def format_validation_error(e: ValidationError) -> str:
    """Format validation errors in a user-friendly way"""

    messages = list[str]()

    for error in e.errors():
        field = ".".join(str(x) for x in error["loc"])
        message = error.get("msg")
        messages.append(f"• {field}: {message}")

    return "\n".join(messages)


settings_manager = SettingsManager()
