# Verification Checklist

## Fixed in this rebuilt project

- Runtime notebook errors removed: no `display(df)` bug, no invalid `dropDuplicates` call, no `df_Date` casing mismatch, no broken `listen_duraion` typo, no unclosed Jinja block.
- `checkpointLocation` spelling corrected everywhere.
- `trigger(availableNow=True)` replaces deprecated `once=True`.
- CDC redesign implemented with `metadata.pipeline_watermarks` and ADF Lookup + per-table execution.
- Real SCD2 merge logic implemented for `dim_user_scd2` and `dim_artist_scd2`.
- Gold Lakeflow pipeline uses `from pyspark import pipelines as dp`.
- `durationFlag` moved from silver to gold.
- Data quality notebook covers PK null checks, referential integrity checks, and row-count drift warnings.
- Structured Delta-backed logging tables implemented.
- Azure Monitor ARM template included.
- The Databricks bundle now includes an explicit `gold_modeling_job` that runs the SCD2 notebook before refreshing the Lakeflow pipeline.
- The ADF SQL linked service now reads the connection string from Azure Key Vault instead of pipeline parameters.
- CI workflow added.
- Static tests expanded to cover orchestration, Key Vault secret usage, and merge metrics handling.

## Remaining limits that still require a live cloud validation

- Actual Databricks workspace deployment and `databricks bundle validate` against a real host and token.
- Actual Lakeflow pipeline compilation in the target workspace/runtime.
- Actual ADF import and execution against Azure SQL, ADLS, Key Vault, and Logic App resources.
- Actual Azure Monitor alert firing.

## Brutally honest conclusion

This repo is materially stronger and the previously identified high-risk design defects have been reduced. However, it is only fair to call it **code-and-config validated locally** until the Databricks, ADF, Key Vault, and Azure Monitor assets are executed in a real Azure/Databricks environment.


## v3 final hardening pass

- Added `.gitignore` and removed committed Python cache artifacts (`__pycache__`, `.pytest_cache`).
- Removed `logicAppWebhookUrl` from ADF pipeline parameters. The child pipeline now retrieves the Logic App webhook URL from Azure Key Vault at runtime using a `WebActivity` authenticated with the factory managed identity, with secure input/output enabled.
- Clarified ADLS Gen2 authentication in `LS_ADLS_Spotify` as the Azure Data Factory system-assigned managed identity pattern and documented the required RBAC/ACL expectation directly in the linked service metadata.
- Expanded static tests to assert: no committed cache directories, no Logic App secret parameters, Key Vault-backed secret retrieval for notifications, and explicit ADLS managed identity documentation.
