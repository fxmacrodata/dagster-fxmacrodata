# FXMacroData for Dagster

Bring your FXMacroData subscription into Dagster for repeatable macro research, cross-currency history and release-aware data pipelines. The resource and asset/op factories make FXMacroData data available to ordinary Dagster jobs, schedules and downstream assets.

**[Subscribe to FXMacroData](https://fxmacrodata.com/subscribe?utm_source=github&utm_medium=referral&utm_campaign=dagster&utm_content=subscribe)** for broader indicator coverage and full available history. Public USD catalogue, recent history and release-calendar access let you evaluate the integration before connecting your subscription.

## Install

Use Python 3.10 or later. Install the two supplied wheels together; these commands do not require either package version to be published on PyPI:

```sh
python -m pip install ./fxmacrodata_public_client-0.1.1-py3-none-any.whl ./dagster_fxmacrodata-0.1.0-py3-none-any.whl
```

The package supports Dagster 1.13.21 through the 1.13 release series. Include it in the Python environment used by your code location and run workers.

## Connect your subscription

Set `FXMACRODATA_API_KEY` in your worker environment or secret manager. The resource resolves it when a run starts. You can choose a different variable with `FXMacroDataResource(api_key_env_var="RESEARCH_DATA_KEY")`; only that variable's name enters Dagster configuration. The default also recognizes `FXMD_API_KEY`. Use `api_key_env_var=""` to explicitly evaluate public access.

Do not put a key in asset parameters, job configuration, notebooks or source files. Missing standard variables select public access; an explicitly selected missing variable fails clearly.

## Materialize macro data

```python
import dagster as dg
from dagster_fxmacrodata import FXMacroDataResource, build_fxmacrodata_asset

history = build_fxmacrodata_asset(
    "indicator_history",
    name="usd_policy_history",
    parameters={"currency": "USD", "indicator": "policy_rate"},
)
calendar = build_fxmacrodata_asset(
    "release_calendar",
    name="usd_release_calendar",
    parameters={"currency": "USD"},
)

defs = dg.Definitions(
    assets=[history, calendar],
    resources={"fxmacrodata": FXMacroDataResource()},
)
```

Select either asset in Dagster and materialize it. Each asset has operation-specific `parameters` configuration, so you can change a currency or date window in the Launchpad. The full JSON Schema remains attached to the asset definition; constraints and union types are checked by the client before a request.

Every result is an `FXMacroDataResult`. Its `payload` preserves the original response, `source_url` identifies the source endpoint, and `records()` provides a detached table view. Downstream assets receive the same typed result through your chosen IO manager:

```python
from dagster_fxmacrodata import FXMacroDataResult

@dg.asset(
    ins={"history": dg.AssetIn(key=["fxmacrodata", "usd_policy_history"])}
)
def policy_records(history: FXMacroDataResult) -> list[dict]:
    return history.records()
```

Add `policy_records` to `Definitions.assets` to materialize the dependency together. Keep units, publication status and source timestamps when transforming observations. Missing data stays unavailable; FX reference rates are not executable broker quotes.

The default IO manager follows your Dagster deployment's storage configuration. Supply `io_manager_key` when your data needs a dedicated encrypted or access-controlled destination. Materialization metadata includes the operation, record count and public links; it does not include request parameters or response rows. Your own downstream code and IO manager control any additional logging or storage.

## Discover operations and create jobs

```python
from dagster_fxmacrodata import (
    build_fxmacrodata_assets,
    build_fxmacrodata_op,
    operation_catalogue,
)

schemas = {item.name: item.input_schema for item in operation_catalogue()}
all_assets = build_fxmacrodata_assets()
calendar_op = build_fxmacrodata_op(
    "release_calendar", parameters={"currency": "USD"}
)

@dg.job(resource_defs={"fxmacrodata": FXMacroDataResource()})
def release_job():
    calendar_op()
```

All 29 REST operations and 50 hosted MCP tools have both native asset and op paths. MCP names start with `mcp_`. Unconfigured required parameters are requested at launch rather than guessed. Select the operations needed for a pipeline; materializing the whole catalogue is usually unnecessary.

Use `complete_history=True` with offset-paginated REST operations to gather every page while retaining page metadata. Complete history starts at offset zero; nonzero offsets and page-number parameters are rejected before a request. `max_pages` bounds the retrieval and incomplete histories raise an error. Other operations preserve their documented pagination arguments. `stream_events` captures a bounded number of events over a bounded period; it is not a background listener. MCP visual resources remain available in the payload, but Dagster does not render MCP Apps.

See the [operation matrix](CAPABILITIES.md) and [API reference](https://fxmacrodata.com/documentation/reference?utm_source=github&utm_medium=referral&utm_campaign=dagster&utm_content=docs). [FXMacroData](https://fxmacrodata.com/?utm_source=github&utm_medium=referral&utm_campaign=dagster&utm_content=readme) website links carry static referral tags; API and MCP requests carry no tracking tags, and the integration sends no analytics events.

## Development

```sh
python -m pip install -e ".[test]"
python -m pytest tests -q
python -m ruff check src tests
python -m ruff format --check src tests
python -m pyright src
python -m build
```

Tests use installed Dagster definitions and execution with a simulated data client. They require no subscription credentials or external data services.
