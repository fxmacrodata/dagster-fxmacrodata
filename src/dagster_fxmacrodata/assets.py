"""Native Dagster definitions with operation-specific configuration."""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any

import dagster as dg
from fxmacrodata_public import Operation

from .resource import RESULT_TYPE, FXMacroDataResource, FXMacroDataResult, operation_catalogue


def _operation(name: str) -> Operation:
    for operation in operation_catalogue():
        if operation.name == name:
            return operation
    raise ValueError("Unknown FXMacroData operation. Use operation_catalogue().")


def _config_type(schema: dict[str, Any]) -> Any:
    kind = schema.get("type")
    if kind == "string":
        return dg.String
    if kind == "integer":
        return dg.Int
    if kind == "number":
        return dg.Float
    if kind == "boolean":
        return dg.Bool
    if kind == "array":
        return dg.Array(_config_type(schema.get("items", {})))
    if kind == "object":
        return _object_shape(schema, {})
    # JSON Schema unions and constraints remain authoritative in the client.
    # Dagster Any preserves their values instead of rewriting the API contract.
    return dg.Any


def _object_shape(schema: dict[str, Any], defaults: Mapping[str, Any]) -> Any:
    required = schema.get("required", [])
    fields: dict[str, dg.Field] = {}
    for name, value in schema.get("properties", {}).items():
        kwargs: dict[str, Any] = {"description": value.get("description", "")}
        if name in defaults:
            kwargs["default_value"] = deepcopy(defaults[name])
            kwargs["is_required"] = False
        else:
            kwargs["is_required"] = name in required
        fields[name] = dg.Field(_config_type(value), **kwargs)
    shape = dg.Permissive if schema.get("additionalProperties", True) is not False else dg.Shape
    return shape(fields)


def _config(operation: Operation, parameters: Mapping[str, Any] | None) -> dict[str, dg.Field]:
    values = deepcopy(dict(parameters or {}))
    if any(
        name.lower() in {"api_key", "authorization", "access_token", "url", "base_url"} for name in values
    ):
        raise ValueError("Credentials and endpoint overrides are not operation parameters.")
    properties = operation.input_schema.get("properties", {})
    if set(values) - set(properties):
        raise ValueError("Unknown parameter in FXMacroData definition defaults.")
    missing = set(operation.input_schema.get("required", [])) - set(values)
    shape = _object_shape(operation.input_schema, values)
    field = dg.Field(shape, is_required=True) if missing else dg.Field(shape, default_value=values)
    return {"parameters": field}


def _definition_metadata(operation: Operation) -> dict[str, dg.MetadataValue]:
    return {"fxmacrodata/input_schema": dg.MetadataValue.json(deepcopy(operation.input_schema))}


def build_fxmacrodata_asset(
    operation: str,
    *,
    parameters: Mapping[str, Any] | None = None,
    name: str | None = None,
    key_prefix: Sequence[str] = ("fxmacrodata",),
    resource_key: str = "fxmacrodata",
    group_name: str = "fxmacrodata",
    io_manager_key: str = "io_manager",
    complete_history: bool = False,
    max_pages: int = 10000,
) -> dg.AssetsDefinition:
    """Create a materializable asset for any packaged REST or MCP operation.

    Parameters become operation-specific Dagster config and can be changed at
    launch. The original JSON Schema is included in definition metadata. Response
    data is returned to the configured IO manager, never attached to event metadata.
    """
    selected = _operation(operation)

    @dg.asset(
        name=name or operation,
        key_prefix=list(key_prefix),
        group_name=group_name,
        required_resource_keys={resource_key},
        config_schema=_config(selected, parameters),
        description=selected.description,
        metadata=_definition_metadata(selected),
        dagster_type=RESULT_TYPE,
        io_manager_key=io_manager_key,
        kinds={"fxmacrodata"},
    )
    def fxmacrodata_asset(context: dg.AssetExecutionContext) -> dg.Output[FXMacroDataResult]:
        resource: FXMacroDataResource = getattr(context.resources, resource_key)
        result = resource.execute(
            selected.name,
            context.op_execution_context.op_config.get("parameters", {}),
            complete_history=complete_history,
            max_pages=max_pages,
        )
        return dg.Output(result, metadata=result.metadata())

    return fxmacrodata_asset


def build_fxmacrodata_assets(
    operations: Sequence[str] | None = None,
    *,
    parameters: Mapping[str, Mapping[str, Any]] | None = None,
    **kwargs: Any,
) -> list[dg.AssetsDefinition]:
    """Build separately selectable assets; omission exposes the complete inventory."""
    names = list(operations) if operations is not None else [op.name for op in operation_catalogue()]
    if len(set(names)) != len(names):
        raise ValueError("FXMacroData asset operation names must be unique.")
    if parameters and set(parameters) - set(names):
        raise ValueError("Parameters were supplied for an unselected FXMacroData operation.")
    return [
        build_fxmacrodata_asset(name, parameters=(parameters or {}).get(name), **kwargs) for name in names
    ]


def build_fxmacrodata_op(
    operation: str,
    *,
    parameters: Mapping[str, Any] | None = None,
    name: str | None = None,
    resource_key: str = "fxmacrodata",
    complete_history: bool = False,
    max_pages: int = 10000,
) -> dg.OpDefinition:
    """Create an ordinary Dagster op whose output can feed a graph or job."""
    selected = _operation(operation)

    @dg.op(
        name=name or "fxmd_" + operation,
        required_resource_keys={resource_key},
        config_schema=_config(selected, parameters),
        description=selected.description,
        out=dg.Out(FXMacroDataResult),
    )
    def fxmacrodata_op(context: dg.OpExecutionContext) -> dg.Output[FXMacroDataResult]:
        resource: FXMacroDataResource = getattr(context.resources, resource_key)
        result = resource.execute(
            selected.name,
            context.op_config.get("parameters", {}),
            complete_history=complete_history,
            max_pages=max_pages,
        )
        return dg.Output(result, metadata=result.metadata())

    return fxmacrodata_op
