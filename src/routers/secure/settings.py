from copy import copy
from typing import Annotated, Any, cast, get_args, get_origin

from fastapi import APIRouter, Body, HTTPException, Path, Query
from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic.fields import FieldInfo

from program.settings import settings_manager
from program.settings.models import AppModel

from ..models.shared import MessageResponse

router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    responses={404: {"description": "Not found"}},
)

FieldPath = list[tuple[str, FieldInfo]]


def _resolve_path(current_settings: dict[str, Any], path: str) -> Any:
    current_obj: Any = current_settings
    for key in path.split("."):
        if not isinstance(current_obj, dict) or key not in current_obj:
            return None
        current_obj = current_obj[key]
    return current_obj


def _compat_value(current_settings: dict[str, Any], path: str) -> Any:
    filesystem = cast(dict[str, Any], current_settings.get("filesystem", {}))
    downloaders = cast(dict[str, Any], current_settings.get("downloaders", {}))
    post_processing = cast(dict[str, Any], current_settings.get("post_processing", {}))
    subtitle = cast(dict[str, Any], post_processing.get("subtitle", {}))
    subtitle_providers = cast(dict[str, Any], subtitle.get("providers", {}))
    scraping = cast(dict[str, Any], current_settings.get("scraping", {}))
    indexer = cast(dict[str, Any], current_settings.get("indexer", {}))
    logging = cast(dict[str, Any], current_settings.get("logging", {}))
    updaters = cast(dict[str, Any], current_settings.get("updaters", {}))

    anime_profile = cast(
        dict[str, Any],
        cast(dict[str, Any], filesystem.get("library_profiles", {})).get("anime", {}),
    )

    compat: dict[str, Any] = {
        "debug": current_settings.get("log_level") == "DEBUG",
        "debug_database": False,
        "log": logging.get("enabled", True),
        "force_refresh": False,
        "map_metadata": False,
        "symlink": {
            "rclone_path": filesystem.get("mount_path", ""),
            "library_path": anime_profile.get("library_path", "/library"),
            "separate_anime_dirs": anime_profile.get("enabled", False),
            "repair_symlinks": False,
            "repair_interval": 6,
        },
        "downloaders": {
            **downloaders,
            "prefer_speed_over_quality": False,
            "torbox": {"enabled": False, "api_key": ""},
        },
        "indexer": {
            **indexer,
            "update_interval": indexer.get("schedule_offset_minutes", 30) * 60,
        },
        "post_processing": {
            **post_processing,
            "subliminal": {
                "enabled": subtitle.get("enabled", False),
                "languages": subtitle.get("languages", []),
                "providers": {
                    "opensubtitles": subtitle_providers.get(
                        "opensubtitles", {"enabled": False}
                    ),
                    "opensubtitlescom": {
                        "enabled": False,
                        "username": "",
                        "password": "",
                    },
                },
            },
        },
        "scraping": {
            **scraping,
            "parse_debug": False,
            "knightcrawler": {
                "enabled": False,
                "url": "",
                "filter": "",
                "timeout": 30,
                "ratelimit": True,
            },
        },
        "updaters": {
            **updaters,
            "updater_interval": updaters.get("updater_interval", 120),
        },
    }

    return compat.get(path)


def _parse_requested_keys(keys: str) -> list[str]:
    requested_keys = [key.strip() for key in keys.split(",") if key.strip()]
    if requested_keys:
        return requested_keys

    raise HTTPException(
        status_code=400,
        detail="At least one key must be provided",
    )


def _unwrap_model_annotation(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation

    origin = get_origin(annotation)
    if origin is None:
        return None

    for arg in get_args(annotation):
        if arg is type(None):
            continue
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg

    return None


def _resolve_model_field_path(path: str) -> FieldPath:
    parts = path.split(".")
    current_model: type[BaseModel] = AppModel
    resolved_fields: FieldPath = []

    for index, part in enumerate(parts):
        model_fields = current_model.model_fields
        field_info = model_fields.get(part)
        if field_info is None:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid path: {path}",
            )

        resolved_fields.append((part, field_info))

        if index == len(parts) - 1:
            continue

        next_model = _unwrap_model_annotation(field_info.annotation)
        if next_model is None:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid nested path: {path}",
            )
        current_model = next_model

    return resolved_fields


def _ensure_container_schema(
    target_properties: dict[str, Any],
    target_required: list[str],
    field_name: str,
    field_info: FieldInfo,
) -> dict[str, Any]:
    container_schema = target_properties.get(field_name)
    if not isinstance(container_schema, dict):
        title_value = field_info.title or field_name.replace("_", " ").title()
        container_schema = {
            "title": title_value,
            "type": "object",
            "properties": {},
            "required": [],
        }
        target_properties[field_name] = container_schema

    if field_info.is_required() and field_name not in target_required:
        target_required.append(field_name)

    return container_schema


def _field_json_schema(
    field_info: FieldInfo,
) -> tuple[dict[str, Any], dict[str, Any]]:
    adapter: TypeAdapter[Any] = TypeAdapter(field_info.annotation)
    field_schema = cast(
        dict[str, Any],
        adapter.json_schema(ref_template="#/$defs/{model}"),
    )
    defs = cast(dict[str, Any], field_schema.pop("$defs", {}))
    return field_schema, defs


def _add_requested_field_schema(
    properties: dict[str, Any],
    required: list[str],
    all_defs: dict[str, Any],
    path: str,
) -> None:
    resolved_fields = _resolve_model_field_path(path)
    field_name, target_field_info = resolved_fields[-1]
    field_schema, defs = _field_json_schema(target_field_info)
    all_defs.update(defs)

    current_properties = properties
    current_required = required

    for parent_name, parent_field_info in resolved_fields[:-1]:
        container_schema = _ensure_container_schema(
            current_properties,
            current_required,
            parent_name,
            parent_field_info,
        )
        current_properties = cast(dict[str, Any], container_schema["properties"])
        current_required = cast(list[str], container_schema["required"])

    current_properties[field_name] = field_schema
    if target_field_info.is_required() and field_name not in current_required:
        current_required.append(field_name)


def _build_filtered_schema(requested_keys: list[str], title: str) -> dict[str, Any]:
    all_defs: dict[str, Any] = {}
    properties: dict[str, Any] = {}
    required: list[str] = []

    for key in requested_keys:
        _add_requested_field_schema(properties, required, all_defs, key)

    filtered_schema: dict[str, Any] = {
        "properties": properties,
        "required": required,
        "title": title,
        "type": "object",
    }

    if all_defs:
        filtered_schema["$defs"] = all_defs

    return filtered_schema


@router.get(
    "/schema",
    operation_id="get_settings_schema",
    response_model=dict[str, Any],
)
async def get_settings_schema() -> dict[str, Any]:
    """Get the JSON schema for the settings."""

    return settings_manager.settings.model_json_schema()


@router.get(
    "/schema/keys",
    operation_id="get_settings_schema_for_keys",
    response_model=dict[str, Any],
)
async def get_settings_schema_for_keys(
    keys: Annotated[
        str,
        Query(
            description="Comma-separated list of settings keys or nested paths to get schema for (e.g., 'api_key,updaters,downloaders.orchestrator')",
            min_length=1,
        ),
    ],
    title: Annotated[
        str,
        Query(
            description="Title of the schema",
        ),
    ] = "FilteredSettings",
) -> dict[str, Any]:
    requested_keys = _parse_requested_keys(keys)
    return _build_filtered_schema(requested_keys, title)


@router.get(
    "/load",
    operation_id="load_settings",
    response_model=MessageResponse,
)
async def load_settings() -> MessageResponse:
    settings_manager.load()

    return MessageResponse(message="Settings loaded!")


@router.post(
    "/save",
    operation_id="save_settings",
    response_model=MessageResponse,
)
async def save_settings() -> MessageResponse:
    settings_manager.save()

    return MessageResponse(message="Settings saved!")


@router.get(
    "/get/all",
    operation_id="get_all_settings",
    response_model=AppModel,
)
async def get_all_settings() -> AppModel:
    return copy(settings_manager.settings)


@router.get(
    "/get/{paths}",
    operation_id="get_settings",
    response_model=dict[str, Any],
)
async def get_settings(
    paths: Annotated[
        str,
        Path(
            description="Comma-separated list of settings paths",
            min_length=1,
        ),
    ],
) -> dict[str, Any]:
    current_settings = settings_manager.settings.model_dump()
    data = dict[str, Any]()

    for path in paths.split(","):
        compat_value = _compat_value(current_settings, path)
        if compat_value is not None:
            data[path] = compat_value
            continue

        data[path] = _resolve_path(current_settings, path)

    return data


@router.post(
    "/set/all",
    operation_id="set_all_settings",
    response_model=MessageResponse,
)
async def set_all_settings(
    new_settings: Annotated[
        dict[str, Any],
        Body(description="New settings to apply"),
    ],
) -> MessageResponse:
    current_settings = settings_manager.settings.model_dump()

    def update_settings(current_obj: dict[str, Any], new_obj: dict[str, Any]):
        for key, value in new_obj.items():
            if isinstance(value, dict) and key in current_obj:
                update_settings(current_obj[key], cast(dict[str, Any], value))
            else:
                current_obj[key] = value

    update_settings(current_settings, new_settings)

    # Validate and save the updated settings
    try:
        updated_settings = settings_manager.settings.model_validate(current_settings)
        settings_manager.load(settings_dict=updated_settings.model_dump())
        settings_manager.save()  # Ensure the changes are persisted
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return MessageResponse(message="All settings updated successfully!")


@router.post(
    "/set/{paths}",
    operation_id="set_settings",
    response_model=MessageResponse,
)
async def set_settings(
    paths: Annotated[
        str,
        Path(
            description="Comma-separated list of settings paths to update",
            min_length=1,
        ),
    ],
    values: Annotated[
        dict[str, Any],
        Body(description="Dictionary mapping paths to their new values"),
    ],
) -> MessageResponse:
    current_settings = settings_manager.settings.model_dump()
    requested_paths = [p.strip() for p in paths.split(",") if p.strip()]

    missing_values = [p for p in requested_paths if p not in values]
    if missing_values:
        raise HTTPException(
            status_code=400,
            detail=f"Missing values for paths: {', '.join(missing_values)}",
        )

    for path in requested_paths:
        keys = path.split(".")
        current_obj: Any = current_settings

        # Navigate to the parent object
        for k in keys[:-1]:
            if not isinstance(current_obj, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot traverse path '{path}': intermediate value is not an object.",
                )
            if k not in current_obj:
                raise HTTPException(
                    status_code=400,
                    detail=f"Path '{path}' does not exist.",
                )
            current_obj = cast(Any, current_obj[k])

        if not isinstance(current_obj, dict):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot set value at '{path}': parent is not an object.",
            )
        if keys[-1] not in current_obj:
            raise HTTPException(
                status_code=400,
                detail=f"Key '{keys[-1]}' does not exist in path '{'.'.join(keys[:-1]) or 'root'}'.",
            )
        current_obj[keys[-1]] = values[path]

    try:
        updated_settings = settings_manager.settings.__class__(**current_settings)
        settings_manager.load(settings_dict=updated_settings.model_dump())
        settings_manager.save()
    except ValidationError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to update settings: {str(e)}",
        ) from e

    return MessageResponse(message="Settings updated successfully.")
