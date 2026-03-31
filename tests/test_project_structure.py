from pathlib import Path
import json

import yaml

ROOT = Path(__file__).resolve().parents[1]

required_files = [
    ROOT / "README.md",
    ROOT / "databricks.yml",
    ROOT / ".gitignore",
    ROOT / "sitecustomize.py",
    ROOT / "pytest.ini",
    ROOT / "notebooks" / "silver" / "silver_ingestion.py",
    ROOT / "notebooks" / "gold" / "gold_scd2_merge.py",
    ROOT / "notebooks" / "gold" / "gold_dlt_pipeline.py",
    ROOT / "notebooks" / "quality" / "data_quality_validation.py",
    ROOT / "utils" / "transformations.py",
    ROOT / "sql" / "001_create_source_schema.sql",
    ROOT / "infra" / "azure_monitor_alerts.json",
    ROOT / "adf" / "linkedServices" / "ls_key_vault.json",
    ROOT / ".github" / "workflows" / "ci.yml",
]


def test_required_files_exist():
    missing = [str(p) for p in required_files if not p.exists()]
    assert not missing, f"Missing files: {missing}"


def test_repo_has_no_committed_python_cache_or_pytest_cache():
    import shutil

    # Remove any transient cache dirs created by the local interpreter/test runner,
    # then assert the shipped project tree is clean.
    for cache_dir in [p for p in ROOT.rglob('*') if p.is_dir() and p.name in {'__pycache__', '.pytest_cache'}]:
        shutil.rmtree(cache_dir, ignore_errors=True)

    bad_dirs = [
        str(p.relative_to(ROOT))
        for p in ROOT.rglob('*')
        if p.is_dir() and p.name in {'__pycache__', '.pytest_cache'}
    ]
    assert not bad_dirs, f"Remove committed cache directories: {bad_dirs}"


def test_table_config_json_is_valid():
    data = json.loads((ROOT / "conf" / "table_configs.json").read_text(encoding="utf-8"))
    assert "tables" in data
    assert len(data["tables"]) == 5


def test_databricks_bundle_has_gold_orchestration_job():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    jobs = bundle["resources"]["jobs"]
    assert "gold_modeling_job" in jobs
    tasks = jobs["gold_modeling_job"]["tasks"]
    task_keys = {task["task_key"] for task in tasks}
    assert {"gold_scd2_merge", "gold_pipeline_refresh"}.issubset(task_keys)
    pipeline_tasks = [task for task in tasks if task["task_key"] == "gold_pipeline_refresh"]
    assert pipeline_tasks[0]["pipeline_task"]["pipeline_id"] == "${resources.pipelines.gold_dlt_pipeline.id}"


def test_gold_scd2_is_materialized_before_dual_use():
    text = (ROOT / "notebooks" / "gold" / "gold_scd2_merge.py").read_text(encoding="utf-8")
    assert ".cache()" in text
    assert "changed_or_new.count()" in text
    assert "F.lit(run_ts)" in text


def test_adf_sql_linked_service_uses_key_vault_secret_reference():
    linked_service = json.loads((ROOT / "adf" / "linkedServices" / "ls_sql_source.json").read_text(encoding="utf-8"))
    connection_string = linked_service["properties"]["typeProperties"]["connectionString"]
    assert connection_string["type"] == "AzureKeyVaultSecret"
    assert connection_string["store"]["referenceName"] == "LS_AzureKeyVault"


def test_adls_linked_service_is_documented_as_system_assigned_managed_identity():
    linked_service = json.loads((ROOT / "adf" / "linkedServices" / "ls_adls.json").read_text(encoding="utf-8"))
    description = linked_service["properties"]["description"]
    annotations = linked_service["properties"]["annotations"]
    assert "system-assigned managed identity" in description.lower()
    assert "auth:system-assigned-managed-identity" in annotations


def test_adf_pipelines_do_not_expose_sql_or_logicapp_secret_parameters():
    master = json.loads((ROOT / "adf" / "pipelines" / "pl_spotify_ingest_master.json").read_text(encoding="utf-8"))
    child = json.loads((ROOT / "adf" / "pipelines" / "pl_spotify_copy_table.json").read_text(encoding="utf-8"))
    master_params = master["properties"].get("parameters", {})
    child_params = child["properties"].get("parameters", {})
    for forbidden in ["sqlPassword", "sqlUsername", "logicAppWebhookUrl"]:
        assert forbidden not in master_params
        assert forbidden not in child_params


def test_adf_copy_pipeline_uses_key_vault_web_activity_for_logicapp_secret():
    child = json.loads((ROOT / "adf" / "pipelines" / "pl_spotify_copy_table.json").read_text(encoding="utf-8"))
    activities = {activity["name"]: activity for activity in child["properties"]["activities"]}
    get_secret = activities["Get_LogicApp_Webhook_Secret"]
    assert get_secret["type"] == "WebActivity"
    assert get_secret["typeProperties"]["authentication"]["type"] == "MSI"
    assert "vault.azure.net" in get_secret["typeProperties"]["authentication"]["resource"]
    assert get_secret["policy"]["secureOutput"] is True
    notify = activities["Notify_On_Failure"]
    assert "Get_LogicApp_Webhook_Secret" in notify["typeProperties"]["url"]["value"]
    assert notify["policy"]["secureInput"] is True


def test_merge_metrics_helper_exists():
    text = (ROOT / "utils" / "transformations.py").read_text(encoding="utf-8")
    assert "count_rows_written_from_operation_metrics" in text
    assert "get_delta_operation_metrics" in text
    assert "numTargetRowsInserted" in text


def test_ci_workflow_runs_pytest_and_optional_bundle_validate():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "pytest -q" in workflow
    assert "databricks bundle validate" in workflow


def test_gold_pipeline_uses_current_lakeflow_import():
    text = (ROOT / "notebooks" / "gold" / "gold_dlt_pipeline.py").read_text(encoding="utf-8")
    assert "from pyspark import pipelines as dp" in text
