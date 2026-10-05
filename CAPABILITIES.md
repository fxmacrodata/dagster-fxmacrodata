# Dagster operation coverage

The packaged public-client 0.1.1 inventory contains 29 REST operations and 50 MCP
tools. Every row below has an independent native Dagster asset and op, using
`build_fxmacrodata_asset(name)` or `build_fxmacrodata_op(name)`. Required parameters
are supplied in the definition or launch configuration. `operation_catalogue()`
returns every original JSON Schema; each asset carries that schema in definition
metadata. Field descriptions and primitive/nested object types appear in Dagster
configuration. JSON Schema unions, patterns, bounds and other constraints are
validated by the public client without changing their meaning.

The `FXMacroDataResult` output preserves the service payload and source identity.
Its records projection is additive. The IO manager receives the whole object;
event metadata contains only operation, count and static public provider/docs links.

| Operation | Contract | Native consumers |
| --- | --- | --- |
| `health` | `GET /v1/health` | Asset, op, resource |
| `ping` | `GET /v1/ping` | Asset, op, resource |
| `forex` | `GET /v1/forex/{base}/{quote}` | Asset, op, resource |
| `intraday_reference_rates` | `GET /v1/fx/intraday-reference-rates/{base}/{quote}` | Asset, op, resource |
| `fx_sources` | `GET /v1/fx/sources` | Asset, op, resource |
| `fx_source_universe` | `GET /v1/fx/source-universe` | Asset, op, resource |
| `data_catalogue` | `GET /v1/data_catalogue/{currency}` | Asset, op, resource |
| `release_calendar` | `GET /v1/calendar/{currency}` | Asset, op, resource |
| `market_sessions` | `GET /v1/market_sessions` | Asset, op, resource |
| `rate_differentials` | `GET /v1/rate_differentials/{base}/{quote}` | Asset, op, resource |
| `curves` | `GET /v1/curves/{currency}` | Asset, op, resource |
| `financial_prices` | `GET /v1/financial_prices/{currency}` | Asset, op, resource |
| `press_releases` | `GET /v1/press-releases/{currency}` | Asset, op, resource |
| `risk_sentiment` | `GET /v1/risk_sentiment` | Asset, op, resource |
| `factors` | `GET /v1/factors/{currency}/{factor}` | Asset, op, resource |
| `event_predictions` | `GET /v1/predictions/{currency}/{indicator}` | Asset, op, resource |
| `latest_announcements` | `GET /v1/announcements/{currency}/latest` | Asset, op, resource |
| `indicator_history` | `GET /v1/announcements/{currency}/{indicator}` | Asset, op, resource |
| `cot` | `GET /v1/cot/{currency}` | Asset, op, resource |
| `latest_commodities` | `GET /v1/commodities/latest` | Asset, op, resource |
| `commodities` | `GET /v1/commodities/{indicator}` | Asset, op, resource |
| `announcement_changes` | `GET /v1/announcements/changes` | Asset, op, resource |
| `stream_events` | `GET /v1/stream/events` | Asset, op, resource |
| `reference_rates` | `GET /v1/fx/reference-rates/{base}/{quote}` | Asset, op, resource |
| `session_rates` | `GET /v1/fx/session-rates/{base}/{quote}` | Asset, op, resource |
| `prediction_coverage` | `GET /v1/predictions/coverage` | Asset, op, resource |
| `currency_prediction_coverage` | `GET /v1/predictions/coverage/{currency}` | Asset, op, resource |
| `currency_predictions` | `GET /v1/predictions/{currency}` | Asset, op, resource |
| `research_panel` | `POST /v1/research/panel` | Asset, op, resource |
| `mcp_ping` | Hosted MCP `ping` | Asset, op, resource |
| `mcp_mcp_capabilities` | Hosted MCP `mcp_capabilities` | Asset, op, resource |
| `mcp_mcp_auth_guide` | Hosted MCP `mcp_auth_guide` | Asset, op, resource |
| `mcp_subscribe_for_mcp_access` | Hosted MCP `subscribe_for_mcp_access` | Asset, op, resource |
| `mcp_data_catalogue` | Hosted MCP `data_catalogue` | Asset, op, resource |
| `mcp_risk_sentiment` | Hosted MCP `risk_sentiment` | Asset, op, resource |
| `mcp_macro_news` | Hosted MCP `macro_news` | Asset, op, resource |
| `mcp_release_calendar` | Hosted MCP `release_calendar` | Asset, op, resource |
| `mcp_release_calendar_visual_artifact` | Hosted MCP `release_calendar_visual_artifact` | Asset, op, resource |
| `mcp_event_predictions` | Hosted MCP `event_predictions` | Asset, op, resource |
| `mcp_latest_announcements` | Hosted MCP `latest_announcements` | Asset, op, resource |
| `mcp_announcement_changes` | Hosted MCP `announcement_changes` | Asset, op, resource |
| `mcp_press_releases` | Hosted MCP `press_releases` | Asset, op, resource |
| `mcp_macro_factor` | Hosted MCP `macro_factor` | Asset, op, resource |
| `mcp_fx_reference_sources` | Hosted MCP `fx_reference_sources` | Asset, op, resource |
| `mcp_fx_reference_universe` | Hosted MCP `fx_reference_universe` | Asset, op, resource |
| `mcp_fx_intraday_reference_rates` | Hosted MCP `fx_intraday_reference_rates` | Asset, op, resource |
| `mcp_rate_curve` | Hosted MCP `rate_curve` | Asset, op, resource |
| `mcp_rate_differentials` | Hosted MCP `rate_differentials` | Asset, op, resource |
| `mcp_latest_commodities` | Hosted MCP `latest_commodities` | Asset, op, resource |
| `mcp_forex` | Hosted MCP `forex` | Asset, op, resource |
| `mcp_seasonality` | Hosted MCP `seasonality` | Asset, op, resource |
| `mcp_indicator_query` | Hosted MCP `indicator_query` | Asset, op, resource |
| `mcp_plot_visual_artifact` | Hosted MCP `plot_visual_artifact` | Asset, op, resource |
| `mcp_indicator_visual_artifact` | Hosted MCP `indicator_visual_artifact` | Asset, op, resource |
| `mcp_forex_visual_artifact` | Hosted MCP `forex_visual_artifact` | Asset, op, resource |
| `mcp_commodities_visual_artifact` | Hosted MCP `commodities_visual_artifact` | Asset, op, resource |
| `mcp_cot_visual_artifact` | Hosted MCP `cot_visual_artifact` | Asset, op, resource |
| `mcp_policy_rate_differential_visual_artifact` | Hosted MCP `policy_rate_differential_visual_artifact` | Asset, op, resource |
| `mcp_macro_briefing_task` | Hosted MCP `macro_briefing_task` | Asset, op, resource |
| `mcp_indicator_intel_task` | Hosted MCP `indicator_intel_task` | Asset, op, resource |
| `mcp_pair_intel_task` | Hosted MCP `pair_intel_task` | Asset, op, resource |
| `mcp_macro_heatmap_task` | Hosted MCP `macro_heatmap_task` | Asset, op, resource |
| `mcp_policy_scenario_modeler_task` | Hosted MCP `policy_scenario_modeler_task` | Asset, op, resource |
| `mcp_macro_war_room_task` | Hosted MCP `macro_war_room_task` | Asset, op, resource |
| `mcp_event_impact_replay_task` | Hosted MCP `event_impact_replay_task` | Asset, op, resource |
| `mcp_quant_scenario_lab_task` | Hosted MCP `quant_scenario_lab_task` | Asset, op, resource |
| `mcp_known_at_time_task` | Hosted MCP `known_at_time_task` | Asset, op, resource |
| `mcp_macro_regime_classifier_task` | Hosted MCP `macro_regime_classifier_task` | Asset, op, resource |
| `mcp_release_risk_score_task` | Hosted MCP `release_risk_score_task` | Asset, op, resource |
| `mcp_portfolio_risk_engine_task` | Hosted MCP `portfolio_risk_engine_task` | Asset, op, resource |
| `mcp_fx_trade_setup_task` | Hosted MCP `fx_trade_setup_task` | Asset, op, resource |
| `mcp_fx_backtest_task` | Hosted MCP `fx_backtest_task` | Asset, op, resource |
| `mcp_macro_research_pack_task` | Hosted MCP `macro_research_pack_task` | Asset, op, resource |
| `mcp_prediction_coverage` | Hosted MCP `prediction_coverage` | Asset, op, resource |
| `mcp_market_sessions` | Hosted MCP `market_sessions` | Asset, op, resource |
| `mcp_cot_data` | Hosted MCP `cot_data` | Asset, op, resource |
| `mcp_commodities` | Hosted MCP `commodities` | Asset, op, resource |
| `mcp_financial_prices` | Hosted MCP `financial_prices` | Asset, op, resource |
| `mcp_official_dataset_family` | Hosted MCP `official_dataset_family` | Asset, op, resource |

## Execution limits

- Authentication uses a subscriber's environment variable, not asset parameters.
  Public USD access is available for evaluation; protected operations enforce the
  subscription's permissions. An unavailable result remains unavailable.
- Original timestamps, publication status, units and forecast types are retained.
  The adapter does not infer future release dates or turn reference FX into trading quotes.
- `complete_history=True` gathers every offset-paginated REST page through the
  client's bounded history reader, starting at offset zero. Nonzero initial offsets
  and page-number parameters are rejected before a request. Other pagination arguments are passed through.
  The output retains pagination metadata; an incomplete full-history read fails.
- `stream_events` is a bounded event capture with the client's event/time limits.
  Dagster jobs do not become persistent webhook or streaming listeners.
- MCP resources, images and rich content remain in the original payload. Dagster
  does not render MCP Apps. Discovery refresh can inspect new server tools, but
  stable generated Definitions use the packaged inventory until the client is updated.
- Dagster's IO manager and downstream code determine storage, retention and access
  for subscriber data. The adapter adds no sample rows or input values to event metadata.

[FXMacroData documentation](https://fxmacrodata.com/documentation/reference?utm_source=github&utm_medium=referral&utm_campaign=dagster-fxmacrodata&utm_content=docs)
