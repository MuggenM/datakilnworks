"""
SQL-Native AI & ML Model Inference Engine for Data Kiln Works.
Provides Databricks-compatible SQL functions:
- ai_predict(model_name, features_json) -> JSON string with full prediction metadata
- ai_score(model_name, features_json)   -> DOUBLE primary score / probability
- ai_classify(model_name, features_json)-> VARCHAR predicted class / risk tier / alert level
- ai_query(model_or_prompt, input_val)  -> VARCHAR Databricks ai_query() emulation
- ai_explain(model_name, features_json) -> VARCHAR human-readable recommendation & reasoning

Works natively across local DuckDB queries, clustered compute nodes, and Monaco SQL Editor.
"""

import os
import json
import math
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("localspark.ai_sql")


def parse_features(features_input: Any) -> Dict[str, Any]:
    """Safely parses dict, stringified JSON, or DuckDB struct representation into a features dictionary."""
    if isinstance(features_input, dict):
        return features_input
    if not features_input:
        return {}
    if isinstance(features_input, str):
        trimmed = features_input.strip()
        if trimmed.startswith("{") and trimmed.endswith("}"):
            # 1. Try standard JSON
            try:
                return json.loads(trimmed)
            except Exception:
                pass
            # 2. Try Python/DuckDB struct literal representation (e.g. {'salary': 85000, 'tenure': 2.0})
            try:
                import ast
                parsed = ast.literal_eval(trimmed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        # Handle simple key=val or scalar fallback
        try:
            return json.loads(trimmed)
        except Exception:
            return {"input": trimmed}
    return {"value": features_input}


def predict_scalar(model_name: str, features_input: Any) -> str:
    """
    Databricks-compatible predict(model, features) scalar UDF.
    Evaluates model against feature record and returns primary prediction
    (predicted class, label, tier, or continuous value) as VARCHAR.
    Usage:
        SELECT predict('employee_turnover_predictor', {'salary': 85000, 'tenure_years': 2.0});
        SELECT predict('finance.risk.credit_evaluator', json_object('income', 75000, 'debt', 12000));
        SELECT predict('employee_turnover_predictor@champion', {'salary': salary, 'tenure_years': tenure});
    """
    try:
        from web.serving import score_model
        payload = parse_features(features_input)
        res = score_model(str(model_name).strip(), payload)
        if res.get("predictions") and len(res["predictions"]) > 0:
            p = res["predictions"][0]
            for key in ["prediction", "class", "risk_tier", "alert_level", "label", "turnover_probability", "failure_probability", "score"]:
                if key in p and p[key] is not None:
                    return str(p[key])
            return json.dumps(p)
        return "UNKNOWN"
    except Exception as e:
        logger.error(f"Error in predict('{model_name}'): {e}")
        return f"ERROR: {e}"


def predict_score_scalar(model_name: str, features_input: Any) -> float:
    """
    Databricks-compatible predict_score(model, features) scalar UDF.
    Returns primary continuous probability or regression prediction score as a DOUBLE.
    Usage:
        SELECT * FROM warehouse.dbo.silver_employees
        WHERE predict_score('employee_turnover_predictor', {'salary': salary, 'tenure_years': tenure}) > 0.60;
    """
    try:
        from web.serving import score_model
        payload = parse_features(features_input)
        res = score_model(str(model_name).strip(), payload)
        if res.get("predictions") and len(res["predictions"]) > 0:
            p = res["predictions"][0]
            for key in ["probability", "turnover_probability", "failure_probability", "score", "confidence"]:
                if key in p and p[key] is not None:
                    return float(p[key])
        return 0.0
    except Exception as e:
        logger.error(f"Error in predict_score('{model_name}'): {e}")
        return 0.0


def ai_predict_scalar(model_name: str, features_input: Any) -> str:
    """
    Evaluates model against feature record and returns complete prediction object as JSON.
    Usage:
        SELECT ai_predict('employee_turnover_predictor', json_object('salary', 85000, 'tenure_years', 2.0))
        SELECT ai_predict('employee_turnover_predictor', {'salary': 85000, 'tenure_years': 2.0}) ->> '$.recommendation'
    """
    try:
        from web.serving import score_model
        payload = parse_features(features_input)
        res = score_model(str(model_name).strip(), payload)
        if res.get("predictions") and len(res["predictions"]) > 0:
            pred = dict(res["predictions"][0])
            pred["model_name"] = res.get("model_name")
            pred["model_version"] = res.get("model_version")
            pred["stage"] = res.get("stage")
            return json.dumps(pred)
        return json.dumps({"error": "No prediction generated", "model": model_name})
    except Exception as e:
        logger.error(f"Error in ai_predict('{model_name}'): {e}")
        return json.dumps({"error": str(e), "model": model_name})


def ai_score_scalar(model_name: str, features_input: Any) -> float:
    """
    Returns the primary continuous probability or prediction score as a DOUBLE.
    Usage:
        SELECT * FROM warehouse.dbo.silver_employees
        WHERE ai_score('employee_turnover_predictor', {'salary': salary, 'tenure_years': tenure}) > 0.60;
    """
    return predict_score_scalar(model_name, features_input)


def ai_classify_scalar(model_name: str, features_input: Any) -> str:
    """
    Returns the predicted category, risk tier, or alert label as a VARCHAR.
    Usage:
        SELECT 
            department,
            ai_classify('employee_turnover_predictor', {'salary': salary, 'tenure_years': tenure}) AS tier,
            COUNT(*)
        FROM warehouse.dbo.silver_employees
        GROUP BY 1, 2;
    """
    try:
        from web.serving import score_model
        payload = parse_features(features_input)
        res = score_model(str(model_name).strip(), payload)
        if res.get("predictions") and len(res["predictions"]) > 0:
            p = res["predictions"][0]
            for key in ["risk_tier", "alert_level", "label", "tier", "class", "prediction"]:
                if key in p and p[key] is not None:
                    return str(p[key])
        return "UNKNOWN"
    except Exception as e:
        logger.error(f"Error in ai_classify('{model_name}'): {e}")
        return f"ERROR: {e}"


def ai_explain_scalar(model_name: str, features_input: Any) -> str:
    """
    Returns actionable recommendation or explanation string for the prediction.
    Usage:
        SELECT employee_id, ai_explain('employee_turnover_predictor', {'salary': salary, 'tenure_years': tenure}) AS next_steps
        FROM warehouse.dbo.silver_employees;
    """
    try:
        from web.serving import score_model
        payload = parse_features(features_input)
        res = score_model(str(model_name).strip(), payload)
        if res.get("predictions") and len(res["predictions"]) > 0:
            p = res["predictions"][0]
            for key in ["recommendation", "explanation", "action", "alert_level"]:
                if key in p and p[key]:
                    return str(p[key])
        return "No action required."
    except Exception as e:
        return f"Explanation error: {e}"


def ai_query_scalar(model_or_prompt: str, input_val: Any) -> str:
    """
    Databricks-compatible ai_query() emulation.
    Can query an ML model (if registered) or an AI prompt / LLM completion.
    Usage:
        SELECT ai_query('employee_turnover_predictor', {'salary': 75000, 'tenure_years': 3})
    """
    target = str(model_or_prompt).strip()
    # 1. Try ML model scoring first
    try:
        from web.serving import get_registered_model, score_model
        model = get_registered_model(target)
        if model:
            payload = parse_features(input_val)
            res = score_model(target, payload)
            if res.get("predictions") and len(res["predictions"]) > 0:
                p = res["predictions"][0]
                # Return string representation of primary prediction
                for key in ["risk_tier", "alert_level", "turnover_probability", "failure_probability", "score", "prediction"]:
                    if key in p:
                        return str(p[key])
                return json.dumps(p)
    except Exception as e_m:
        logger.debug(f"Model lookup fallback in ai_query: {e_m}")

    # 2. Heuristic / LLM fallback
    input_clean = str(input_val).strip()
    return f"AI[{target}]: {input_clean[:100]}"


def register_duckdb_ai_functions(duckdb_py_conn):
    """
    Registers the AI/ML SQL UDFs onto the provided DuckDB Python connection.
    Safe to call repeatedly; uses DuckDB create_function with native bindings.
    Supports both native DuckDB struct arguments and stringified JSON payloads.
    """
    if duckdb_py_conn is None:
        return

    # Check if conn has .con (e.g. DuckSession) or is already DuckDBPyConnection
    target_conn = getattr(duckdb_py_conn, "con", duckdb_py_conn)

    udf_specs = [
        ("predict", predict_scalar, "VARCHAR"),
        ("predict_score", predict_score_scalar, "DOUBLE"),
        ("ai_predict", ai_predict_scalar, "VARCHAR"),
        ("ai_score", ai_score_scalar, "DOUBLE"),
        ("ai_classify", ai_classify_scalar, "VARCHAR"),
        ("ai_explain", ai_explain_scalar, "VARCHAR"),
        ("ai_query", ai_query_scalar, "VARCHAR"),
    ]

    for name, fn, return_type in udf_specs:
        try:
            try:
                target_conn.remove_function(name)
            except Exception:
                pass
            # Register with dynamic parameter type inference so both STRUCT and VARCHAR are accepted
            target_conn.create_function(name, fn, return_type=return_type)
        except Exception as e:
            # Fallback for DuckDB versions requiring explicit parameter types
            try:
                target_conn.create_function(name, fn, ["VARCHAR", "VARCHAR"], return_type)
            except Exception as e2:
                logger.warning(f"Could not register DuckDB UDF '{name}': {e2}")

    logger.info("Successfully registered DuckDB SQL AI UDFs: predict, predict_score, ai_predict, ai_score, ai_classify, ai_explain, ai_query")
