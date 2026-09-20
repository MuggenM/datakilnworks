import os
import sys

# Add repo to sys.path
sys.path.insert(0, "/home/martin/volumes/datakilnworks")

def run_tests():
    print("=== TEST 1: parse_model_namespace ===")
    from web.serving import parse_model_namespace
    
    cat, sch, name, alias = parse_model_namespace("marketing.analytics.churn_risk@champion")
    assert (cat, sch, name, alias) == ("marketing", "analytics", "churn_risk", "champion"), f"Failed: {(cat, sch, name, alias)}"
    
    cat, sch, name, alias = parse_model_namespace("sales.revenue_forecast")
    assert (cat, sch, name, alias) == ("warehouse", "sales", "revenue_forecast", None), f"Failed: {(cat, sch, name, alias)}"

    cat, sch, name, alias = parse_model_namespace("equipment_failure")
    assert (cat, sch, name, alias) == ("warehouse", "dbo", "equipment_failure", None), f"Failed: {(cat, sch, name, alias)}"

    cat, sch, name, alias = parse_model_namespace("credit_score@v2", default_catalog="prod_cat", default_schema="finance")
    assert (cat, sch, name, alias) == ("prod_cat", "finance", "credit_score", "v2"), f"Failed: {(cat, sch, name, alias)}"
    print("PASS: parse_model_namespace tests passed.")

    print("\n=== TEST 2: create_registered_model with 3-part dotted name ===")
    from web.serving import (
        create_registered_model,
        get_registered_model,
        create_model_version,
        get_model_version,
        set_model_alias,
        get_model_aliases,
        delete_model_alias,
        resolve_model_and_version,
        delete_registered_model
    )

    # Clean up previous test run if any
    delete_registered_model("customer_churn_risk")

    m = create_registered_model(
        name="marketing.analytics.customer_churn_risk",
        description="Predicts customer churn probability within 30 days",
        tags={"tier": "enterprise", "framework": "xgboost"}
    )
    assert m is not None, "Failed to create registered model"
    assert m["name"] == "customer_churn_risk", f"Unexpected name: {m['name']}"
    assert m["catalog_name"] == "marketing", f"Unexpected catalog: {m['catalog_name']}"
    assert m["schema_name"] == "analytics", f"Unexpected schema: {m['schema_name']}"
    assert m["full_name"] == "marketing.analytics.customer_churn_risk", f"Unexpected full_name: {m['full_name']}"
    print(f"PASS: Created 3-level model '{m['full_name']}'")

    print("\n=== TEST 3: create_model_version & auto-creation ===")
    v1 = create_model_version(
        name="marketing.analytics.customer_churn_risk",
        algorithm="XGBClassifier",
        metrics={"auc": 0.89, "accuracy": 0.85},
        description="Initial baseline model"
    )
    assert v1["version"] == 1, f"Expected version 1, got {v1['version']}"
    assert v1["algorithm"] == "XGBClassifier"

    v2 = create_model_version(
        name="marketing.analytics.customer_churn_risk",
        algorithm="XGBClassifier_Tuned",
        metrics={"auc": 0.94, "accuracy": 0.91},
        description="Hyperparameter tuned model"
    )
    assert v2["version"] == 2, f"Expected version 2, got {v2['version']}"
    print("PASS: Model versions 1 and 2 created successfully.")

    print("\n=== TEST 4: Model Aliases with 3-level names ===")
    # Assign champion to v2, challenger to v1
    m_alias = set_model_alias("marketing.analytics.customer_churn_risk", "champion", 2)
    assert m_alias["champion_version"] == 2, f"Champion alias not set: {m_alias.get('champion_version')}"
    
    set_model_alias("marketing.analytics.customer_churn_risk", "challenger", 1)
    aliases = get_model_aliases("marketing.analytics.customer_churn_risk")
    assert aliases.get("champion") == 2 and aliases.get("challenger") == 1, f"Aliases mismatch: {aliases}"
    print(f"PASS: Aliases assigned successfully: {aliases}")

    print("\n=== TEST 5: resolve_model_and_version 3-level resolution ===")
    mod_champ, ver_champ = resolve_model_and_version("marketing.analytics.customer_churn_risk@champion")
    assert mod_champ is not None and ver_champ is not None, "Failed to resolve champion"
    assert ver_champ["version"] == 2, f"Expected v2 for champion, got {ver_champ['version']}"

    mod_chal, ver_chal = resolve_model_and_version("marketing.analytics.customer_churn_risk@challenger")
    assert mod_chal is not None and ver_chal is not None, "Failed to resolve challenger"
    assert ver_chal["version"] == 1, f"Expected v1 for challenger, got {ver_chal['version']}"

    # Default to champion without alias
    mod_def, ver_def = resolve_model_and_version("marketing.analytics.customer_churn_risk")
    assert ver_def["version"] == 2, f"Expected default to resolve champion (v2), got {ver_def['version']}"
    print("PASS: 3-level model namespace and alias resolution verified.")

    print("\n=== TEST 6: Python MLflow Shim Parity ===")
    import web.mlflow_shim as mlflow
    client = mlflow.tracking.MlflowClient()

    reg_info = client.get_registered_model("marketing.analytics.customer_churn_risk")
    assert reg_info["name"] == "customer_churn_risk"
    assert reg_info["catalog_name"] == "marketing"
    assert reg_info["schema_name"] == "analytics"

    mv_champ = client.get_model_version_by_alias("marketing.analytics.customer_churn_risk", "champion")
    assert mv_champ.version == 2, f"Expected version 2, got {mv_champ.version}"
    assert mv_champ.name == "customer_churn_risk"

    # Test top-level mlflow.register_model
    auto_mv = mlflow.register_model(
        model_uri="runs:/fake_run_12345/model",
        name="sales.forecasting.quarterly_revenue_forecaster",
        tags={"tier": "financial", "domain": "sales"}
    )
    assert auto_mv.name == "quarterly_revenue_forecaster", f"Unexpected auto_mv name: {auto_mv.name}"
    auto_reg = client.get_registered_model("sales.forecasting.quarterly_revenue_forecaster")
    assert auto_reg["catalog_name"] == "sales"
    assert auto_reg["schema_name"] == "forecasting"
    print(f"PASS: mlflow.register_model created '{auto_reg['full_name']}' version {auto_mv.version}")

    # Clean up test models
    delete_registered_model("customer_churn_risk")
    delete_registered_model("quarterly_revenue_forecaster")

    print("\n=======================================================")
    print("ALL MODEL REGISTRY NAMESPACE TESTS PASSED 100%!")
    print("=======================================================")

if __name__ == "__main__":
    run_tests()
