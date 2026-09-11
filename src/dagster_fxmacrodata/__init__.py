"""FXMacroData resources, assets and ops for Dagster."""

from .assets import build_fxmacrodata_asset, build_fxmacrodata_assets, build_fxmacrodata_op
from .resource import FXMacroDataResource, FXMacroDataResult, operation_catalogue

__version__ = "0.1.0"
__all__ = [
    "FXMacroDataResource",
    "FXMacroDataResult",
    "build_fxmacrodata_asset",
    "build_fxmacrodata_assets",
    "build_fxmacrodata_op",
    "operation_catalogue",
]
