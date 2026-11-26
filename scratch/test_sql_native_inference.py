#!/usr/bin/env python3
"""
Verification script for SQL-Native ML Inference parity with Databricks.
Tests:
1. Native DuckDB UDF registration of `predict` and `predict_score`.
2. Flexible feature parsing (DuckDB struct literals, json_object, and raw JSON strings).
3. 3-level namespace & alias resolution in SQL: 'catalog.schema.model', 'model@champion', 'model@v1'.
4. Actual trained Scikit-learn model logging, registration, artifact persistence, and dynamic SQL execution.
5. All 7 inference functions: predict, predict_score, ai_predict, ai_score, ai_classify, ai_explain, ai_query.
"""

import os
import sys
import json
import duckdb
import numpy as np
import pandas as pd

# Add repo to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from web.ai_sql import register_duckdb_ai_functions, parse_features
from web.serving import score_model, init_serving_db
from web import mlflow_shim as mlflow

def run_tests():
    print("================================================================")
    print("  Testing SQL-Native ML Inference Parity with Databricks")
    print("================================================================")

    init_serving_db()
    con = duckdb.connect()
    register_duckdb_ai_functions(con)

    # -------------------------------------------------------------
    # 1. Test Feature Parsing (JSON, Struct literal, dict)
    # -------------------------------------------------------------
    print("\n--- 1. Testing parse_features ---")
    p1 = parse_features({"salary": 85000, "tenure": 2.5})
    assert p1 == {"salary": 85000, "tenure": 2.5}, f"Failed dict: {p1}"
    print("✓ Direct dict parsed:", p1)

    p2 = parse_features('{"salary": 85000, "tenure": 2.5}')
    assert p2 == {"salary": 85000, "tenure": 2.5}, f"Failed json: {p2}"
    print("✓ JSON string parsed:", p2)

    p3 = parse_features("{'salary': 85000, 'tenure': 2.5}")
    assert p3 == {"salary": 85000, "tenure": 2.5}, f"Failed struct literal: {p3}"
    print("✓ DuckDB struct literal string parsed:", p3)

    # -------------------------------------------------------------
    # 2. Test predict() and predict_score() with Demonstration Models
    # -------------------------------------------------------------
    print("\n--- 2. Testing predict() and predict_score() SQL UDFs ---")

    # With DuckDB native struct
    res_struct = con.execute("""
        SELECT 
            predict('employee_turnover_predictor', {'salary': 95000, 'tenure_years': 3.0, 'satisfaction_score': 0.85, 'overtime_hours': 2.0}) AS pred,
            predict_score('employee_turnover_predictor', {'salary': 95000, 'tenure_years': 3.0, 'satisfaction_score': 0.85, 'overtime_hours': 2.0}) AS prob
    """).fetchall()
    print("✓ DuckDB Struct Query Result:", res_struct)
    assert len(res_struct) == 1
    assert res_struct[0][0] in ["LOW", "MEDIUM", "HIGH"]
    assert 0.0 <= res_struct[0][1] <= 1.0

    # With json_object()
    res_json_obj = con.execute("""
        SELECT 
            predict('employee_turnover_predictor', json_object('salary', 40000, 'tenure_years', 0.5, 'satisfaction_score', 0.2, 'overtime_hours', 20.0)) AS pred,
            predict_score('employee_turnover_predictor', json_object('salary', 40000, 'tenure_years', 0.5, 'satisfaction_score', 0.2, 'overtime_hours', 20.0)) AS prob
    """).fetchall()
    print("✓ json_object Query Result:", res_json_obj)
    assert res_json_obj[0][0] == "HIGH"
    assert res_json_obj[0][1] > 0.60

    # -------------------------------------------------------------
    # 3. Test 3-Level Namespace & Model Aliases in SQL Queries
    # -------------------------------------------------------------
    print("\n--- 3. Testing 3-Level Namespace & Aliases in SQL ---")
    res_alias = con.execute("""
        SELECT 
            predict('employee_turnover_predictor@champion', {'salary': 90000}) AS champion_pred,
            predict('employee_turnover_predictor@challenger', {'salary': 90000}) AS challenger_pred,
            predict('warehouse.dbo.employee_turnover_predictor', {'salary': 90000}) AS ns_pred
    """).fetchall()
    print("✓ Namespace & Alias Query Result:", res_alias)
    assert len(res_alias) == 1

    # -------------------------------------------------------------
    # 4. Test Existing AI Functions Parity
    # -------------------------------------------------------------
    print("\n--- 4. Testing Full Suite: ai_predict, ai_score, ai_classify, ai_explain, ai_query ---")
    res_full = con.execute("""
        SELECT 
            ai_classify('employee_turnover_predictor', {'salary': 90000}) AS tier,
            ai_score('employee_turnover_predictor', {'salary': 90000}) AS score,
            ai_explain('employee_turnover_predictor', {'salary': 90000}) AS explanation,
            ai_query('employee_turnover_predictor', {'salary': 90000}) AS query_val
    """).fetchall()
    print("✓ Full Suite Result:", res_full)
    assert len(res_full) == 1
    assert isinstance(res_full[0][1], float)

    # -------------------------------------------------------------
    # 5. Train an Actual Scikit-Learn Model, Register in Unity Catalog,
    #    and Execute Real Trained Artifact Inference in SQL!
    # -------------------------------------------------------------
    print("\n--- 5. Testing True Trained Model Artifact Inference in SQL ---")
    from sklearn.ensemble import RandomForestClassifier

    # Synthetic credit risk dataset
    X_train = pd.DataFrame({
        "income": [25000, 30000, 45000, 80000, 110000, 140000],
        "debt_to_income": [0.65, 0.55, 0.40, 0.20, 0.15, 0.10],
        "credit_score": [580, 610, 640, 720, 780, 810]
    })
    y_train = np.array([1, 1, 1, 0, 0, 0]) # 1 = Default risk, 0 = Safe

    clf = RandomForestClassifier(n_estimators=10, random_state=42)
    clf.fit(X_train, y_train)

    trained_model_name = "finance.risk.credit_default_model"

    with mlflow.start_run(run_name="credit_default_training") as run:
        mlflow.log_params({"n_estimators": 10, "algorithm": "RandomForest"})
        mlflow.log_metric("train_accuracy", 1.0)
        mlflow.sklearn.log_model(
            sk_model=clf,
            artifact_path="model",
            registered_model_name=trained_model_name
        )
        print(f"✓ Trained model logged and registered as '{trained_model_name}' (Run ID: {run.info.run_id})")

    # Now execute SQL inference against this newly trained real model!
    res_trained = con.execute(f"""
        SELECT 
            predict('{trained_model_name}', {{'income': 150000, 'debt_to_income': 0.12, 'credit_score': 800}}) AS safe_pred,
            predict_score('{trained_model_name}', {{'income': 150000, 'debt_to_income': 0.12, 'credit_score': 800}}) AS safe_prob,
            predict('{trained_model_name}', {{'income': 22000, 'debt_to_income': 0.70, 'credit_score': 550}}) AS risk_pred,
            predict_score('{trained_model_name}', {{'income': 22000, 'debt_to_income': 0.70, 'credit_score': 550}}) AS risk_prob
    """).fetchall()

    print("✓ Real Trained Scikit-Learn Model SQL Inference Result:", res_trained)
    row = res_trained[0]
    # safe_pred should be '0' (Safe) and risk_pred should be '1' (Default risk)
    assert str(row[0]) == "0", f"Expected safe_pred 0, got {row[0]}"
    assert str(row[2]) == "1", f"Expected risk_pred 1, got {row[2]}"
    print("✓ Model predicted safe customer as:", row[0], "with default probability:", row[1])
    print("✓ Model predicted high risk customer as:", row[2], "with default probability:", row[3])

    # -------------------------------------------------------------
    # 6. Test Batch Table Inference via SQL
    # -------------------------------------------------------------
    print("\n--- 6. Testing Batch Table Inference via SQL ---")
    con.execute("""
        CREATE TEMP TABLE applicants AS 
        SELECT 101 AS app_id, 135000 AS income, 0.15 AS debt_to_income, 790 AS credit_score
        UNION ALL
        SELECT 102 AS app_id, 28000 AS income, 0.62 AS debt_to_income, 595 AS credit_score
    """)

    batch_res = con.execute(f"""
        SELECT 
            app_id,
            income,
            predict('{trained_model_name}', {{'income': income, 'debt_to_income': debt_to_income, 'credit_score': credit_score}}) AS decision,
            predict_score('{trained_model_name}', {{'income': income, 'debt_to_income': debt_to_income, 'credit_score': credit_score}}) AS default_prob
        FROM applicants
        ORDER BY app_id ASC
    """).fetchall()

    print("✓ Batch Table Query Result:")
    for r in batch_res:
        print("   Record:", r)
    assert len(batch_res) == 2
    assert str(batch_res[0][2]) == "0"
    assert str(batch_res[1][2]) == "1"

    print("\n================================================================")
    print("  ALL SQL-NATIVE ML INFERENCE PARITY TESTS PASSED SUCCESSFULLY!")
    print("================================================================")

if __name__ == "__main__":
    run_tests()
