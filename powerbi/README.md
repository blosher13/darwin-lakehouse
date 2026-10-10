# Power BI: DarwinRail

A Power BI Project (PBIP) over the Databricks gold layer. Everything is plain text, so it diffs
and reviews like code.

```
DarwinRail.pbip                 open this in Power BI Desktop
DarwinRail.SemanticModel/       TMDL: tables, DAX measures, relationships, Databricks parameters
DarwinRail.Report/              PBIR: 2 pages, 14 visuals (generated - do not hand-edit)
generate_report.py              report layout as code -> DarwinRail.Report/
validate_pbir.py                schema + model-reference validation (runs in CI)
render_preview.py               PNG previews from live data -> ../docs/images/
```

## Model

Star schema, import mode:

| Table | Source | Key |
|---|---|---|
| `fact_train_calls` | `<catalog>.<schema>.fact_train_calls` | `call_id` |
| `dim_location` | `locations` | `location_code` |
| `dim_operator` | `operators` | `operator_code` |
| `dim_late_reason` | `late_reasons` (text shortened in Power Query) | `reason_code` |
| `dim_date` | DAX `CALENDAR` over `fact_train_calls[scheduled_start_date]` | `Date` |

Measures: `Trains`, `Departures`, `On time %`, `Within 5 minutes %`, `Average departure delay`,
`Delay minutes` and `Delayed trains`. Every measure carries a description in the model.
Departure measures count **passenger trains at public stops** against the **public timetable**.

Connection parameters: `DatabricksHost`, `DatabricksHttpPath`, `DatabricksCatalog` (default
`workspace`) and `DatabricksSchema` (default `darwin`).

## Pages

1. **Network performance:** KPI cards, within-5-minutes share by scheduled hour, average delay by
   operator, stations ranked by delay minutes.
2. **Delay causes:** trains delayed by Darwin reason, the distribution of departure delays, and
   the most delayed departures.

Both pages have a service-date slicer. The visuals use one hue (`#2a78d6`), so nothing depends on
telling colours apart.

## Opening it

1. Open `DarwinRail.pbip` in Power BI Desktop (Windows). If prompted, enable *Power BI Project
   (.pbip) save option* and *Store reports using enhanced metadata format (PBIR)* under
   Options > Preview features.
2. **Transform data > Edit parameters:** replace the placeholders in `DatabricksHost` and
   `DatabricksHttpPath`. You'll find both under SQL Warehouses > *your warehouse* > Connection
   details. Change `DatabricksCatalog` and `DatabricksSchema` only if you changed
   `databricks_catalog` / `databricks_schema` in `.env`.
3. **Refresh,** and authenticate with **Personal Access Token** using your Databricks token.
4. **Publish** to a Power BI workspace if you want to share it. Alternatively, in Databricks you
   can use *Catalog Explorer > Use with BI tools > Publish to Power BI workspace* to create the
   semantic model directly in the service.

## Validation status

- **Model:** the TMDL model loads in Microsoft's Tabular Object Model (TOM) with 5 tables,
  7 measures and 4 relationships, all references resolving.
- **Report:** every report file validates against Microsoft's published PBIR schemas, and every
  visual field exists in the model.
- **Not yet done:** the project hasn't been opened in Power BI Desktop. Expect Desktop to upgrade
  the file versions on first save. Commit what it writes back.

## Regenerating

```bash
python powerbi/generate_report.py     # after editing the layout
python powerbi/validate_pbir.py       # schemas + field references
pip install -e '.[preview]' && python powerbi/render_preview.py   # refresh README images (needs .env)
```
