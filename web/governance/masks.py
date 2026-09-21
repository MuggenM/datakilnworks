"""
Mask expression builders: type families, per-type behaviour, custom-expression validation and a live tester.

Every builder returns a SQL expression that (a) references only the column and the mask primitives from macros.py and
(b) is wrapped in CAST(... AS <column type>), so a masked result never changes a column's type. When a mask does not fit
a type (hash on a number, partial on a date, ...) the result is NULL: a policy can never leak because its mask did not
apply.
"""

import re
from typing import Any, Dict, List, Optional

import sqlglot
from sqlglot import exp

from web.governance.macros import HASH_FUNCTION, install_governance_macros, qualified

MASK_TYPES = ("redact", "hash", "partial", "email", "null", "generalize", "custom")
FAMILIES = ("string", "numeric", "temporal", "boolean", "other")

# Restrictiveness for tie-breaking between equal-priority policies (higher = more restrictive).
RESTRICTIVENESS = {"null": 5, "redact": 4, "hash": 3, "custom": 3, "partial": 2, "email": 2, "generalize": 1}

_NUMERIC = re.compile(r"^(TINYINT|SMALLINT|INTEGER|INT|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT|"
                      r"DECIMAL|NUMERIC|DOUBLE|FLOAT|REAL|INT[0-9]+|FLOAT[0-9]+)\b", re.IGNORECASE)
_STRING = re.compile(r"^(VARCHAR|CHAR|BPCHAR|TEXT|STRING|NVARCHAR)\b", re.IGNORECASE)
_TEMPORAL = re.compile(r"^(DATE|DATETIME|TIMESTAMP\w*|TIME\w*)\b", re.IGNORECASE)
_TIME_ONLY = re.compile(r"^TIME(?!STAMP)\w*\b", re.IGNORECASE)
PLACEHOLDER = "{col}"
_DUMMY = "__gov_col__"


def type_family(data_type: str) -> str:
    """Maps a DuckDB type name to string / numeric / temporal / boolean / other (arrays and structs are 'other')."""
    t = (data_type or "").strip()
    if t.endswith("]") or t.upper().startswith(("STRUCT", "MAP", "LIST", "UNION", "ENUM")):
        return "other"
    if t.upper() == "BOOLEAN" or t.upper() == "BOOL":
        return "boolean"
    if _STRING.match(t):
        return "string"
    if _NUMERIC.match(t):
        return "numeric"
    if _TEMPORAL.match(t):
        return "temporal"
    return "other"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _null(data_type: str) -> str:
    return f"CAST(NULL AS {data_type})"


def mask_expression(mask_type: str, data_type: str, column: str, custom_expr: Optional[str] = None) -> str:
    """The SQL expression that replaces `column` (of `data_type`) for a masked principal."""
    fam = type_family(data_type)
    col = quote_ident(column)
    T = data_type
    if mask_type == "null":
        return _null(T)
    if mask_type == "redact":
        return f"CAST('****' AS {T})" if fam == "string" else _null(T)
    if mask_type == "hash":
        return f"CAST({HASH_FUNCTION}(CAST({col} AS VARCHAR)) AS {T})" if fam == "string" else _null(T)
    if mask_type == "partial":
        return f"CAST({qualified('gov_mask_partial')}({col}) AS {T})" if fam == "string" else _null(T)
    if mask_type == "email":
        return f"CAST({qualified('gov_mask_email')}({col}) AS {T})" if fam == "string" else _null(T)
    if mask_type == "generalize":
        if fam == "string":
            return f"CAST({qualified('gov_mask_generalize_str')}({col}) AS {T})"
        if fam == "numeric":
            return f"CAST({qualified('gov_mask_generalize_num')}({col}) AS {T})"
        if fam == "temporal" and not _TIME_ONLY.match(T):
            return f"CAST(date_trunc('year', {col}) AS {T})"
        return _null(T)
    if mask_type == "custom":
        if not custom_expr or PLACEHOLDER not in custom_expr:
            return _null(T)
        return f"CAST(({custom_expr.replace(PLACEHOLDER, col)}) AS {T})"
    raise ValueError(f"Unknown mask type '{mask_type}'.")


def behaviour_table(mask_type: str) -> Dict[str, str]:
    """Human-readable per-family behaviour of a built-in mask, for the policy editor."""
    table = {
        "redact": {"string": "'****'", "numeric": "NULL", "temporal": "NULL", "boolean": "NULL", "other": "NULL"},
        "null": {f: "NULL" for f in FAMILIES},
        "hash": {"string": "keyed hash (stable pseudonym, joins still work)", "numeric": "NULL", "temporal": "NULL",
                 "boolean": "NULL", "other": "NULL"},
        "partial": {"string": "keeps up to the last 4 characters", "numeric": "NULL", "temporal": "NULL", "boolean": "NULL", "other": "NULL"},
        "email": {"string": "a***@domain.com", "numeric": "NULL", "temporal": "NULL", "boolean": "NULL", "other": "NULL"},
        "generalize": {"string": "first 3 characters + ***", "numeric": "one significant figure", "temporal": "truncated to the year",
                       "boolean": "NULL", "other": "NULL"},
        "custom": {f: "your expression" for f in FAMILIES},
    }
    return table[mask_type]


# ----------------------------------------------------------------------------
# Custom expression validation
# ----------------------------------------------------------------------------

_ALLOWED_FUNC_CLASSES = {
    "Left", "Right", "Concat", "ConcatWs", "RegexpReplace", "Substring", "Length", "Lower", "Upper", "Coalesce", "Nullif",
    "Round", "Floor", "Ceil", "Abs", "Trim", "SHA2", "MD5", "Repeat", "SplitPart", "Case", "If", "Cast", "TryCast", "DateTrunc",
    "Least", "Greatest", "Extract", "Anonymous", "Mod", "Sign", "Pow", "Sqrt", "Initcap", "Replace", "Reverse", "Lpad", "Rpad",
    "Hex", "StrPosition", "Ln", "Log", "Exp",
}
_ALLOWED_ANONYMOUS = {"repeat", "left", "right", "sha256", "md5", "regexp_replace", "split_part", "date_trunc", "instr",
                      "lpad", "rpad", "strpos", "concat_ws", "substr", "trim", "ltrim", "rtrim", "sign", "ceil", "floor",
                      "gov_mask_email", "gov_mask_partial", "gov_mask_generalize_str", "gov_mask_generalize_num"}
_FORBIDDEN_NODES = (exp.Select, exp.Subquery, exp.Window, exp.AggFunc, exp.Star, exp.Placeholder, exp.Parameter, exp.Table,
                    exp.Command, exp.Lambda, exp.Union, exp.With, exp.Query)
_SAMPLES = {
    "string": ("VARCHAR", "alice@example.com"), "numeric": ("DECIMAL(12,2)", "12345.67"),
    "temporal": ("TIMESTAMP", "1990-05-17 08:30:00"), "boolean": ("BOOLEAN", "true"),
}


def _ast_errors(template: str) -> List[str]:
    if PLACEHOLDER not in template:
        return [f"The expression must reference the column with the {PLACEHOLDER} placeholder."]
    if template.replace(PLACEHOLDER, "").count("{") or template.replace(PLACEHOLDER, "").count("}"):
        return ["Only the {col} placeholder is allowed between braces."]
    if ";" in template:
        return ["Multiple statements are not allowed."]
    try:
        tree = sqlglot.parse_one(template.replace(PLACEHOLDER, _DUMMY), dialect="duckdb")
    except Exception as exc:
        return [f"Not a valid scalar SQL expression: {str(exc).splitlines()[0][:160]}"]
    errors: List[str] = []
    for node in tree.walk():
        node = node[0] if isinstance(node, tuple) else node
        if isinstance(node, _FORBIDDEN_NODES):
            errors.append(f"{type(node).__name__} is not allowed in a mask expression.")
        elif isinstance(node, exp.Column) and node.name != _DUMMY:
            errors.append(f"Unknown column '{node.name}': use {PLACEHOLDER} to refer to the masked column.")
        elif isinstance(node, exp.Func):
            kind = type(node).__name__
            if kind == "Anonymous":
                if node.name.lower() not in _ALLOWED_ANONYMOUS:
                    errors.append(f"Function '{node.name}' is not allowed.")
            elif kind not in _ALLOWED_FUNC_CLASSES:
                errors.append(f"Function '{node.sql_name()}' is not allowed.")
    if not tree.find(exp.Column):
        errors.append(f"The expression never uses {PLACEHOLDER}, so it would not depend on the data.")
    return sorted(set(errors))


_tester_con = None


def _tester():
    """An isolated in-memory connection with the mask primitives, used for dry runs (never sees real data)."""
    global _tester_con
    if _tester_con is None:
        import duckdb
        con = duckdb.connect(":memory:")
        install_governance_macros(con)
        _tester_con = con
    return _tester_con


def run_mask(mask_type: str, data_type: str, value: Any, custom_expr: Optional[str] = None) -> Dict[str, Any]:
    """Applies the real mask expression to one sample value in a sandbox connection."""
    if not re.fullmatch(r"[A-Za-z0-9_ (),\[\]]{1,64}", data_type or ""):
        raise ValueError("Unsupported data type.")
    con = _tester().cursor()
    expression = mask_expression(mask_type, data_type, "v", custom_expr)
    sql = f'SELECT {expression} AS masked FROM (SELECT CAST(? AS {data_type}) AS "v")'
    try:
        row = con.execute(sql, [value]).fetchone()
        return {"ok": True, "masked": None if row[0] is None else str(row[0]), "sql": expression}
    except Exception as exc:
        return {"ok": False, "error": str(exc).splitlines()[0][:200], "sql": expression}
    finally:
        con.close()


def validate_custom_expression(template: str, families: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Static AST checks plus a dry run on synthetic values (and NULL) for each family.
    `families` limits the dry run to the types the policy applies to; a policy without a type filter must survive all.
    """
    errors = _ast_errors(template or "")
    results: Dict[str, Any] = {}
    if not errors:
        for family in (families or [f for f in FAMILIES if f in _SAMPLES]):
            if family not in _SAMPLES:
                continue
            data_type, sample = _SAMPLES[family]
            res = run_mask("custom", data_type, sample, template)
            null_res = run_mask("custom", data_type, None, template)
            results[family] = {"input": sample, "output": res.get("masked"), "ok": res["ok"] and null_res["ok"],
                               "error": res.get("error") or null_res.get("error")}
            if not results[family]["ok"]:
                errors.append(f"Fails for {family} columns: {results[family]['error']}. "
                              f"Restrict the policy to the types it supports (applies_to_types).")
    return {"ok": not errors, "errors": errors, "results": results}
