import json
from copy import deepcopy
from typing import ClassVar
from urllib.parse import parse_qs, quote, urlsplit

import dagster as dg
import pytest
from fxmacrodata_public import FXMacroDataError, Operation, Result, list_operations
from jsonschema import Draft202012Validator

import dagster_fxmacrodata.resource as resource_module
from dagster_fxmacrodata import (
    FXMacroDataResource,
    FXMacroDataResult,
    build_fxmacrodata_asset,
    build_fxmacrodata_assets,
    build_fxmacrodata_op,
    operation_catalogue,
)

OPERATIONS = list_operations()
PAYLOAD = {
    "data": [{"value": 1.25, "unit": "percent", "announcement_datetime": None}],
    "source": "https://www.federalreserve.gov/",
    "publication_time_status": "unknown",
    "forecast_type": "market_consensus",
    "pagination": {"offset": 0, "total": 1},
}


def sample(schema, name=""):
    if "default" in schema and schema["default"] is not None:
        return deepcopy(schema["default"])
    if "enum" in schema:
        return schema["enum"][0]
    if "anyOf" in schema:
        return sample(next(item for item in schema["anyOf"] if item.get("type") != "null"), name)
    if "oneOf" in schema:
        return sample(schema["oneOf"][0], name)
    kind = schema.get("type")
    if isinstance(kind, list):
        return sample({**schema, "type": next(item for item in kind if item != "null")}, name)
    if kind == "object" or "properties" in schema:
        return {
            key: sample(value, key)
            for key, value in schema.get("properties", {}).items()
            if key in schema.get("required", [])
        }
    if kind == "array":
        return [sample(schema.get("items", {}), name)] * max(1, schema.get("minItems", 1))
    if kind in {"integer", "number"}:
        value = max(schema.get("minimum", 1), 1)
        return float(value) if kind == "number" else int(value)
    if kind == "boolean":
        return False
    if schema.get("format") == "date" or name in {"start_date", "end_date", "date"}:
        return "2026-01-01"
    if name in {"decision_times", "as_of"}:
        return "2026-01-01T00:00:00Z"
    if name in {"currency", "base", "base_currency", "quote", "quote_currency"}:
        return "USD"
    if name == "indicator":
        return "policy_rate"
    if name == "dataset_version":
        return "0" * 64
    return schema.get("example", "usd")


@pytest.fixture
def fake_client(monkeypatch):
    class Client:
        instances: ClassVar[list] = []
        response = PAYLOAD
        error = None
        close_error = None

        def __init__(self, api_key=None, timeout=30):
            self.api_key = api_key
            self.timeout = timeout
            self.calls = []
            self.closed = False
            self.refreshed = False
            self.instances.append(self)

        def execute(self, operation, parameters):
            if self.error:
                raise self.error
            spec = next(item for item in OPERATIONS if item.name == operation)
            Draft202012Validator(spec.input_schema).validate(parameters or {})
            self.calls.append((operation, deepcopy(parameters)))
            return Result(operation, deepcopy(self.response), "https://api.fxmacrodata.com/v1/test")

        def history(self, operation, parameters, *, max_pages):
            result = self.execute(operation, parameters)
            self.history_limit = max_pages
            return result

        def close(self):
            self.closed = True
            if self.close_error:
                raise self.close_error

        def discover_mcp_tools(self):
            self.refreshed = True

        def list_operations(self):
            return OPERATIONS + (Operation("mcp_future", "Future", {"type": "object"}, "/mcp", "MCP"),)

    monkeypatch.setattr(resource_module, "FXMacroDataClient", Client)
    monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
    monkeypatch.delenv("FXMD_API_KEY", raising=False)
    return Client


@pytest.mark.parametrize("operation", OPERATIONS, ids=lambda item: item.name)
def test_every_operation_native_asset(operation, fake_client):
    parameters = sample(operation.input_schema)
    Draft202012Validator(operation.input_schema).validate(parameters)
    asset = build_fxmacrodata_asset(operation.name, parameters=parameters)
    schema = asset.metadata_by_key[asset.key]["fxmacrodata/input_schema"].value
    assert schema == operation.input_schema
    result = dg.materialize_to_memory([asset], resources={"fxmacrodata": FXMacroDataResource()})
    assert result.success
    output = result.output_for_node(asset.node_def.name)
    assert isinstance(output, FXMacroDataResult)
    assert output.payload == PAYLOAD
    assert fake_client.instances[-1].calls == [(operation.name, parameters)]
    assert fake_client.instances[-1].closed
    materialization = result.asset_materializations_for_node(asset.node_def.name)[0]
    metadata = materialization.metadata
    assert metadata["dagster/row_count"].value == 1
    assert set(metadata) == {
        "fxmacrodata/operation",
        "dagster/row_count",
        "fxmacrodata/provider",
        "fxmacrodata/documentation",
    }
    assert "announcement_datetime" not in str(metadata)


@pytest.mark.parametrize("operation", OPERATIONS, ids=lambda item: item.name)
def test_every_operation_native_op(operation, fake_client):
    parameters = {
        name: sample(schema, name) for name, schema in operation.input_schema.get("properties", {}).items()
    }
    computation = build_fxmacrodata_op(operation.name, parameters=parameters)

    @dg.job(resource_defs={"fxmacrodata": FXMacroDataResource(api_key_env_var="")})
    def research_job():
        computation()

    with dg.instance_for_test() as instance:
        result = research_job.execute_in_process(instance=instance)
    assert result.success
    assert result.output_for_node(computation.name).payload == PAYLOAD
    assert fake_client.instances[-1].calls == [(operation.name, parameters)]
    assert fake_client.instances[-1].closed


def test_all_definitions_load_and_discovery_is_detached():
    definitions = dg.Definitions(
        assets=build_fxmacrodata_assets(), resources={"fxmacrodata": FXMacroDataResource()}
    )
    dg.Definitions.validate_loadable(definitions)
    assert len(definitions.assets) == len(OPERATIONS) == 79
    assert sum(item.method == "MCP" for item in operation_catalogue()) == 50
    copied = operation_catalogue()
    copied[0].input_schema["injected"] = True
    assert "injected" not in operation_catalogue()[0].input_schema


def test_op_downstream_consumption_and_launch_override(fake_client):
    read = build_fxmacrodata_op("release_calendar", parameters={"currency": "USD"})

    @dg.op
    def row_count(data: FXMacroDataResult) -> int:
        assert data.payload == PAYLOAD
        return len(data.records())

    @dg.job(resource_defs={"fxmacrodata": FXMacroDataResource()})
    def pipeline():
        row_count(read())

    result = pipeline.execute_in_process(
        run_config={"ops": {"fxmd_release_calendar": {"config": {"parameters": {"currency": "EUR"}}}}}
    )
    assert result.success and result.output_for_node("row_count") == 1
    assert fake_client.instances[-1].calls == [("release_calendar", {"currency": "EUR"})]


def test_asset_downstream_and_custom_resource(fake_client):
    read = build_fxmacrodata_asset("health", resource_key="macro", name="provider_health")

    @dg.asset(ins={"data": dg.AssetIn(key=read.key)})
    def extracted(data: FXMacroDataResult):
        return data.payload["data"]

    result = dg.materialize_to_memory([read, extracted], resources={"macro": FXMacroDataResource()})
    assert result.success
    assert result.output_for_node("extracted") == PAYLOAD["data"]


def test_credentials_resolve_at_run_and_do_not_enter_config(fake_client, monkeypatch):
    secret = "unit-test-private-value"
    resource = FXMacroDataResource(api_key_env_var="DATA_SERVICE_CREDENTIAL")
    assert "DATA_SERVICE_CREDENTIAL" in str(resource.model_dump())
    monkeypatch.setenv("DATA_SERVICE_CREDENTIAL", secret)
    asset = build_fxmacrodata_asset("health")
    result = dg.materialize_to_memory([asset], resources={"fxmacrodata": resource})
    assert fake_client.instances[-1].api_key == secret
    assert secret not in repr(resource)
    assert secret not in str(resource.model_dump())
    assert secret not in str(result.all_events)
    assert resource._credential is None


def test_anonymous_explicit_and_fallback_env(fake_client, monkeypatch):
    monkeypatch.setenv("FXMD_API_KEY", "unit-test-fallback")
    FXMacroDataResource().execute("health")
    assert fake_client.instances[-1].api_key == "unit-test-fallback"
    FXMacroDataResource(api_key_env_var="").execute("health")
    assert fake_client.instances[-1].api_key == ""


def test_missing_explicit_env_fails_without_echo(fake_client):
    with pytest.raises(dg.Failure, match="environment variable is empty"):
        FXMacroDataResource(api_key_env_var="UNSET_RESEARCH_CREDENTIAL").execute("health")
    assert not fake_client.instances


@pytest.mark.parametrize("value", ["private-credential-value", "has space", "lowercase", 12])
def test_rejects_credential_in_env_name_without_echo(value):
    with pytest.raises(ValueError) as caught:
        FXMacroDataResource(api_key_env_var=value)
    assert str(value) not in str(caught.value)


def test_number_config_accepts_integer_without_coercion():
    from dagster._config import validate_config

    from dagster_fxmacrodata.assets import _config_type

    for value in [1, 1.5]:
        evaluated = validate_config(_config_type({"type": "number"}), value)
        assert evaluated.success
        assert evaluated.value == value
        assert type(evaluated.value) is type(value)


@pytest.mark.parametrize("known_error", [True, False])
def test_exception_and_cleanup_privacy(fake_client, monkeypatch, known_error):
    secret = "unit-test-private/value"
    monkeypatch.setenv("FXMD_API_KEY", secret)
    message = f"service failed {secret} https://service.invalid?api_key={quote(secret, safe='')}"
    fake_client.error = FXMacroDataError(message) if known_error else RuntimeError(message)
    fake_client.close_error = RuntimeError(message)
    with pytest.raises(dg.Failure) as caught:
        FXMacroDataResource().execute("health")
    assert secret not in str(caught.value)
    assert quote(secret, safe="") not in str(caught.value)
    assert fake_client.instances[-1].closed


def test_native_failure_events_do_not_leak_exception_context(fake_client, monkeypatch):
    secret = "unit-test-hidden-credential"
    monkeypatch.setenv("FXMD_API_KEY", secret)
    fake_client.error = RuntimeError(f"failed with private auth {secret}")
    result = dg.materialize_to_memory(
        [build_fxmacrodata_asset("health")],
        resources={"fxmacrodata": FXMacroDataResource()},
        raise_on_error=False,
    )
    assert not result.success
    assert secret not in str(result.all_events)
    assert fake_client.instances[-1].closed


def test_response_echo_redacted_but_original_data_preserved(fake_client, monkeypatch):
    secret = "unit-test-private/value"
    monkeypatch.setenv("FXMD_API_KEY", secret)
    fake_client.response = {**PAYLOAD, "debug": [secret, quote(secret, safe="")], "api_key": "other-value"}
    output = FXMacroDataResource().execute("health")
    assert output.payload["data"] == PAYLOAD["data"]
    assert output.payload["publication_time_status"] == "unknown"
    assert output.payload["forecast_type"] == "market_consensus"
    assert secret not in json.dumps(output.as_dict())
    assert quote(secret, safe="") not in json.dumps(output.as_dict())
    assert output.payload["api_key"] == "[redacted]"
    assert "1.25" not in repr(output)
    records = output.records()
    records[0]["value"] = 9
    assert output.payload["data"][0]["value"] == 1.25


def test_each_call_owns_a_session_and_history_closes(fake_client):
    resource = FXMacroDataResource()
    resource.execute("health")
    resource.execute("health")
    resource.execute(
        "indicator_history",
        {"currency": "USD", "indicator": "policy_rate"},
        complete_history=True,
        max_pages=7,
    )
    assert len(fake_client.instances) == 3
    assert all(client.closed for client in fake_client.instances)
    assert fake_client.instances[-1].history_limit == 7


@pytest.mark.parametrize("pagination", [{"offset": 100}, {"offset": -1}, {"offset": "0"}, {"page": 2}])
def test_complete_history_rejects_partial_start_before_client_creation(fake_client, pagination):
    with pytest.raises(dg.Failure, match="Complete history"):
        FXMacroDataResource().execute(
            "indicator_history",
            {"currency": "USD", "indicator": "policy_rate", **pagination},
            complete_history=True,
        )
    assert not fake_client.instances


def test_native_complete_history_nonzero_offset_fails_before_request(fake_client):
    asset = build_fxmacrodata_asset(
        "indicator_history",
        complete_history=True,
        parameters={"currency": "USD", "indicator": "policy_rate", "offset": 100},
    )
    result = dg.materialize_to_memory(
        [asset],
        resources={"fxmacrodata": FXMacroDataResource()},
        raise_on_error=False,
    )
    assert not result.success
    assert not fake_client.instances
    assert "Complete history must start at offset 0" in str(result.all_events)


def test_complete_history_zero_offset_preserves_budget_and_source(fake_client):
    output = FXMacroDataResource().execute(
        "indicator_history",
        {"currency": "USD", "indicator": "policy_rate", "offset": 0},
        complete_history=True,
        max_pages=7,
    )
    assert output.payload == PAYLOAD
    assert output.source_url == "https://api.fxmacrodata.com/v1/test"
    assert fake_client.instances[-1].history_limit == 7
    assert fake_client.instances[-1].calls[0][1]["offset"] == 0


def test_all_materialization_site_links_are_attributed():
    result = FXMacroDataResult("health", {}, "https://api.fxmacrodata.com/v1/test")
    for name in ["fxmacrodata/provider", "fxmacrodata/documentation"]:
        url = result.metadata()[name].value
        query = parse_qs(urlsplit(url).query)
        assert query == {
            "utm_source": ["github"],
            "utm_medium": ["referral"],
            "utm_campaign": ["dagster"],
            "utm_content": ["docs"],
        }


def test_refresh_discovery_is_explicit_and_closes(fake_client):
    resource = FXMacroDataResource()
    assert resource.discover_operations() == operation_catalogue()
    assert not fake_client.instances
    refreshed = resource.discover_operations(refresh_mcp=True)
    assert refreshed[-1].name == "mcp_future"
    assert fake_client.instances[-1].refreshed and fake_client.instances[-1].closed


@pytest.mark.parametrize("factory", [build_fxmacrodata_asset, build_fxmacrodata_op])
def test_unknown_and_credentials_not_definition_arguments(factory):
    with pytest.raises(ValueError, match="Unknown FXMacroData operation"):
        factory("unrecognized")
    with pytest.raises(ValueError, match="Credentials"):
        factory("health", parameters={"api_key": "unit-test-value"})
    with pytest.raises(ValueError, match="Unknown parameter"):
        factory("health", parameters={"random": 1})


def test_duplicate_and_unselected_batch_arguments():
    with pytest.raises(ValueError, match="unique"):
        build_fxmacrodata_assets(["health", "health"])
    with pytest.raises(ValueError, match="unselected"):
        build_fxmacrodata_assets(["health"], parameters={"ping": {}})


def test_schema_rejects_bad_type_before_request(fake_client):
    asset = build_fxmacrodata_asset("release_calendar")
    with pytest.raises(dg.DagsterInvalidConfigError):
        dg.materialize_to_memory(
            [asset],
            resources={"fxmacrodata": FXMacroDataResource()},
            run_config={"ops": {asset.node_def.name: {"config": {"parameters": {"currency": 9}}}}},
        )
    assert not fake_client.instances


def test_actual_client_rejects_invalid_schema_and_secrets(monkeypatch):
    monkeypatch.delenv("FXMACRODATA_API_KEY", raising=False)
    monkeypatch.delenv("FXMD_API_KEY", raising=False)
    resource = FXMacroDataResource(api_key_env_var="")
    for parameters in [{"currency": 9}, {"api_key": "unit-test-value"}]:
        with pytest.raises(dg.Failure):
            resource.execute("release_calendar", parameters)


@pytest.mark.parametrize("payload", [None, {}, [], {"data": []}])
def test_empty_results_remain_empty(fake_client, payload):
    fake_client.response = payload
    result = FXMacroDataResource().execute("health")
    assert result.payload == payload
    assert result.records() == []
