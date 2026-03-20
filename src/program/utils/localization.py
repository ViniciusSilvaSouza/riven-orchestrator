"""Localization helpers shared across metadata and subtitle services."""

from __future__ import annotations

from babelfish import Error as BabelfishError
from babelfish import Language
from loguru import logger


def normalize_language_tag(language: str | None, default: str = "en-US") -> str:
    """Normalize a BCP-47-like language tag such as ``pt-BR``."""

    raw = str(language or "").strip().replace("_", "-")

    if not raw:
        return default

    parts = [part for part in raw.split("-") if part]

    if not parts:
        return default

    base_language = parts[0].lower()

    if len(parts) == 1:
        return base_language

    region = parts[1].upper()

    return f"{base_language}-{region}"


def normalize_region_code(region: str | None, default: str = "US") -> str:
    """Normalize a 2-letter region code."""

    raw = str(region or "").strip()

    if not raw:
        return default

    return raw.upper()


def extract_language_code(language: str | None, default: str = "en") -> str:
    """Extract the base language code from a language tag."""

    normalized = normalize_language_tag(language, default)
    return normalized.split("-")[0]


def language_tag_to_alpha3(language: str | None, default: str = "eng") -> str:
    """Convert language tags such as ``pt-BR`` into ISO 639-3 codes."""

    try:
        language_str = normalize_language_tag(language, default)
        lang_part = extract_language_code(language_str, default[:2])

        if len(lang_part) == 3:
            try:
                return str(Language(lang_part).alpha3)
            except (BabelfishError, ValueError):
                try:
                    return str(Language.fromcode(lang_part, "alpha3b").alpha3)
                except (BabelfishError, ValueError, KeyError):
                    pass

        if len(lang_part) == 2:
            try:
                return str(Language.fromcode(lang_part, "alpha2").alpha3)
            except (BabelfishError, ValueError, KeyError):
                pass

        logger.warning(
            f"Could not parse language '{language}', defaulting to '{default}'"
        )
        return default
    except Exception as exc:
        logger.error(
            f"Error normalizing language '{language}': {exc}, defaulting to '{default}'"
        )
        return default
