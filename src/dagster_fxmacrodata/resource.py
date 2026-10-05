"""Run-scoped credentials and lossless data for Dagster computations."""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
from fxmacrodata_public import FXMacroDataClient, FXMacroDataError, Operation, Result, list_operations
from fxmacrodata_public.transport import redact_text
from pydantic import Field, PrivateAttr, SecretStr

PROVIDER_URL = (
    "https://fxmacrodata.com/?utm_source=dagster&utm_medium=integration"
    "&utm_campaign=dagster-fxmacrodata&utm_content=homepage"
)
DOCUMENTATION_URL = (
    "https://fxmacrodata.com/documentation/reference?utm_source=dagster&utm_medium=integration"
    "&utm_campaign=dagster-fxmacrodata&utm_content=docs"
)


def operation_catalogue() -> tuple[Operation, ...]:
    """Return independent copies of every packaged REST and MCP input schema."""
    return deepcopy(list_operations())


def _redact(value: Any, credential: str) -> Any:
    if isinstance(value, str):
        return redact_text(value, credential)
    if isinstance(value, list):
        return [_redact(item, credential) for item in value]
    if isinstance(value, dict):
        return {
            _redact(key, credential): (
                "[redacted]"
                if str(key).lower().replace("_", "").replace("-", "")
                in {"apikey", "authorization", "accesstoken", "password", "token", "secret"}
                else _redact(item, credential)
            )
            for key, item in value.items()
        }
    return deepcopy(value)


@dataclass(frozen=True)
class FXMacroDataResult:
    """Original response and source identity passed to downstream assets or ops.

    Response data is excluded from repr and Dagster event metadata. The selected
    IO manager controls storage and access for the actual output object.
    """

    operation: str
    payload: Any = field(repr=False)
    source_url: str = field(repr=False)

    def records(self) -> list[dict[str, Any]]:
        """Return the client's additive table view without changing the response."""
        return Result(self.operation, self.payload, self.source_url).records()

    def as_dict(self) -> dict[str, Any]:
        """Return a detached copy suitable for downstream Python consumers."""
        return Result(self.operation, self.payload, self.source_url).as_dict()

    def metadata(self) -> dict[str, dg.MetadataValue]:
        """Describe the materialization without serializing response rows or inputs."""
        return {
            "fxmacrodata/operation": dg.MetadataValue.text(self.operation),
            "dagster/row_count": dg.MetadataValue.int(len(self.records())),
            "fxmacrodata/provider": dg.MetadataValue.url(PROVIDER_URL),
            "fxmacrodata/documentation": dg.MetadataValue.url(DOCUMENTATION_URL),
        }


RESULT_TYPE = dg.PythonObjectDagsterType(FXMacroDataResult)
dg.make_python_type_usable_as_dagster_type(FXMacroDataResult, RESULT_TYPE)


class FXMacroDataResource(dg.ConfigurableResource):
    """Use subscriber data in Dagster without putting a key in run configuration.

    Only the environment-variable name belongs in this resource. An empty name
    explicitly selects public evaluation access. None checks FXMACRODATA_API_KEY
    and FXMD_API_KEY when execution starts, using public access if both are absent.
    """

    api_key_env_var: str | None = Field(
        default=None,
        description="Credential environment-variable name; empty selects public USD evaluation.",
    )
    timeout: float = Field(default=30, ge=1, le=120, description="HTTP request timeout in seconds.")
    _credential: SecretStr | None = PrivateAttr(default=None)

    def __init__(self, **data: Any) -> None:
        name = data.get("api_key_env_var")
        if name is not None and (
            not isinstance(name, str) or (name and not re.fullmatch(r"[A-Z_][A-Z0-9_]{0,127}", name))
        ):
            raise ValueError("Use an uppercase environment-variable name, not a credential value.")
        super().__init__(**data)

    def _resolve_credential(self) -> SecretStr:
        if self.api_key_env_var == "":
            return SecretStr("")
        names = (
            [self.api_key_env_var]
            if self.api_key_env_var is not None
            else ["FXMACRODATA_API_KEY", "FXMD_API_KEY"]
        )
        for name in names:
            value = dg.EnvVar(name).get_value()
            if value:
                return SecretStr(value)
        if self.api_key_env_var is not None:
            raise dg.Failure("The configured FXMacroData credential environment variable is empty.")
        return SecretStr("")

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._credential = self._resolve_credential()

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        self._credential = None

    @contextmanager
    def _client(self) -> Iterator[tuple[FXMacroDataClient, str]]:
        secret = self._credential or self._resolve_credential()
        credential = secret.get_secret_value()
        client = None
        try:
            client = FXMacroDataClient(api_key=credential, timeout=self.timeout)
            yield client, credential
        except Exception as error:  # noqa: BLE001 - sanitize the external client boundary
            detail = (
                redact_text(str(error), credential)
                if isinstance(error, FXMacroDataError)
                else "FXMacroData request failed. Check the operation parameters and subscription access."
            )
            raise dg.Failure(description=detail) from None
        finally:
            if client is not None:
                # Cleanup must neither replace an operation error nor leak SDK diagnostics.
                try:
                    client.close()
                except Exception:  # noqa: BLE001, S110 - cleanup diagnostics can contain credentials
                    pass

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | None = None,
        *,
        complete_history: bool = False,
        max_pages: int = 10000,
    ) -> FXMacroDataResult:
        """Read one operation, or every page of an offset-paginated REST operation.

        API schemas and authentication are enforced by the public client. Empty
        responses remain empty. No values, timestamps or future events are inferred.
        """
        if complete_history and parameters is not None:
            offset = parameters.get("offset")
            if offset is not None and (type(offset) is not int or offset != 0):
                raise dg.Failure("Complete history must start at offset 0. Remove offset or set it to 0.")
            if "page" in parameters:
                raise dg.Failure("Complete history uses offset pagination. Remove the page parameter.")
        with self._client() as (client, credential):
            if complete_history:
                result = client.history(operation, deepcopy(parameters), max_pages=max_pages)
            else:
                result = client.execute(operation, deepcopy(parameters))
            return FXMacroDataResult(
                operation=result.operation,
                payload=_redact(result.payload, credential),
                source_url=_redact(result.source_url, credential),
            )

    def discover_operations(self, *, refresh_mcp: bool = False) -> tuple[Operation, ...]:
        """Inspect packaged schemas, optionally refreshing MCP discovery for this call.

        Factories use the packaged inventory for stable Definitions. New server tools
        become factory operations after updating the public-client dependency.
        """
        if not refresh_mcp:
            return operation_catalogue()
        with self._client() as (client, _credential):
            client.discover_mcp_tools()
            return deepcopy(client.list_operations())
