"""
Mask primitives installed on every DuckDB connection that can execute governed SQL.

* Secret-free masks (email, partial, generalize) are plain SQL macros in `memory.main`. Macros must be called by that
  qualified name (verified: temp macros are not visible to cursors and unqualified names only resolve in the current
  catalog), so masks.py always emits `memory.main.gov_mask_*`.
* The keyed hash is a vectorised **Python UDF**, not a macro: a macro body would expose the key through EXPLAIN and
  duckdb_functions(), while a UDF keeps it in process memory. The key is a per-install secret shared over the
  warehouse volume so the studio, workers and Ray actors produce identical pseudonyms.

Because `gov_mask_hash` doubles as a hashing oracle for anyone who can call it, the enforcement gateway (phase 3)
rejects user SQL that references any `gov_*` function; only the rewriter may emit them.
"""

import hashlib
import logging
from typing import List

import pyarrow as pa

from web.secrets_store import load_or_create_secret

logger = logging.getLogger("localspark.governance")

MACRO_CATALOG = "memory.main"
HASH_FUNCTION = "gov_mask_hash"
MACRO_NAMES = ("gov_mask_email", "gov_mask_partial", "gov_mask_generalize_str", "gov_mask_generalize_num")
ALL_FUNCTION_NAMES = (HASH_FUNCTION,) + MACRO_NAMES

_MACRO_SQL: List[str] = [
    # keep the first character of the local part and the domain
    f"""CREATE OR REPLACE MACRO {MACRO_CATALOG}.gov_mask_email(v) AS
        CASE WHEN v IS NULL THEN NULL
             WHEN instr(v, '@') > 1 THEN concat(left(v, 1), '***@', substr(v, instr(v, '@') + 1))
             ELSE '****' END""",
    # keep at most the last 4 characters and never more than 40% of the value (short values stay fully masked)
    f"""CREATE OR REPLACE MACRO {MACRO_CATALOG}.gov_mask_partial(v) AS
        CASE WHEN v IS NULL THEN NULL
             WHEN length(v) < 3 THEN repeat('*', length(v))
             ELSE concat(repeat('*', length(v) - least(4, (length(v) * 2) // 5)), right(v, least(4, (length(v) * 2) // 5))) END""",
    f"""CREATE OR REPLACE MACRO {MACRO_CATALOG}.gov_mask_generalize_str(v) AS
        CASE WHEN v IS NULL THEN NULL
             WHEN length(v) <= 3 THEN '***'
             ELSE concat(left(v, 3), '***') END""",
    # truncate to one significant figure; rounding down so the result never exceeds the original's magnitude
    f"""CREATE OR REPLACE MACRO {MACRO_CATALOG}.gov_mask_generalize_num(v) AS
        CASE WHEN v IS NULL THEN NULL
             WHEN CAST(v AS DOUBLE) = 0 THEN 0.0
             ELSE sign(CAST(v AS DOUBLE)) * floor(abs(CAST(v AS DOUBLE)) / pow(10, floor(log10(abs(CAST(v AS DOUBLE)))))) *
                  pow(10, floor(log10(abs(CAST(v AS DOUBLE))))) END""",
]


def _hash_key() -> bytes:
    return bytes.fromhex(load_or_create_secret("governance_salt"))[:32]


def make_hash_function():
    """Vectorised keyed BLAKE2b (a MAC): deterministic per install, not reversible or dictionary-attackable without the key."""
    key = _hash_key()

    def gov_mask_hash(values: pa.Array) -> pa.Array:
        out = [None if v is None else hashlib.blake2b(v.encode("utf-8"), key=key, digest_size=16).hexdigest()
               for v in values.to_pylist()]
        return pa.array(out, type=pa.string())

    return gov_mask_hash


def install_governance_macros(con) -> None:
    """Idempotently installs every mask primitive on `con` (a raw DuckDB connection)."""
    from duckdb.sqltypes import VARCHAR
    for sql in _MACRO_SQL:
        con.execute(sql)
    try:
        con.remove_function(HASH_FUNCTION)
    except Exception:
        pass
    con.create_function(HASH_FUNCTION, make_hash_function(), [VARCHAR], VARCHAR, type="arrow", side_effects=False)


def macros_installed(con) -> bool:
    """True when every mask primitive resolves on `con`. Workers report this in health checks."""
    try:
        names = {r[0] for r in con.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'gov_mask_%'").fetchall()}
        return set(ALL_FUNCTION_NAMES) <= names
    except Exception:
        return False


def qualified(name: str) -> str:
    """The name to emit in rewritten SQL (the Python UDF is global, macros live in memory.main)."""
    return name if name == HASH_FUNCTION else f"{MACRO_CATALOG}.{name}"
