"""
Glue between the application and the enforcement engine: one small API every data-egress path uses.

    principal_for(user)                 dict from auth (or None) -> Principal; unknown means least privilege
    govern_sql(sql, user, ...)          rewrite + audit; returns a RewriteResult
    governed_sql_or_raise(sql, user)    the SQL to run, or raises GovernanceBlocked
    masked_columns_payload(result)      what the UI shows next to a result
    fingerprint(result)                 cache-key component: results are only shareable between equal mask sets
    mask_arrow(table, catalog, ...)     masks an Arrow table that Python code already holds (version diffs, previews)
    deny_if_subject(user, what)         for features that cannot be masked (they are refused for masked principals)
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional

import pyarrow as pa

from web.governance import enforce, policies, tags
from web.governance.enforce import GovernanceBlocked, RewriteResult
from web.governance.policies import Principal

logger = logging.getLogger("localspark.governance")

_provider = None
_mask_con = None

ANONYMOUS = Principal(username="anonymous", role="user", user_id="anonymous")
# Sample rows that end up in LLM prompts are computed as this principal: its role matches no policy exemption, so tagged
# values never reach a language model (local or hosted), whoever asked the question.
LLM_CONTEXT = Principal(username="llm-context", role="llm-context", user_id="llm-context")


def set_connection_provider(provider) -> None:
    """app.py injects a callable returning a DuckDB cursor on the studio connection (avoids importing app from here)."""
    global _provider
    _provider = provider


def _cursor():
    if _provider is None:
        raise RuntimeError("Governance gateway is not connected to the query engine.")
    return _provider()


def principal_for(user: Optional[Dict[str, Any]]) -> Principal:
    """Missing identity is *least privilege*, never admin."""
    if not user:
        return ANONYMOUS
    return Principal.from_user(user)


def principal_for_username(username: Optional[str]) -> Principal:
    """For work that runs later on someone's behalf (alerts, scheduled exports, jobs): resolve the owner's role now."""
    if not username:
        return ANONYMOUS
    if username == policies.SYSTEM_USERNAME:
        return Principal.system()
    try:
        from web.auth import get_user_by_username
        user = get_user_by_username(username)
    except Exception:
        user = None
    if not user or user.get("is_active", 1) != 1:
        return ANONYMOUS
    return Principal.from_user(user)


def govern_sql(sql: str, user, *, catalog: Optional[str] = None, client: str = "sql", trusted: bool = False,
               con=None) -> RewriteResult:
    """Rewrites `sql` for `user` (a user dict or a Principal). `con` overrides the metadata cursor."""
    principal = user if isinstance(user, Principal) else principal_for(user)
    own = con is None
    cur = con if con is not None else _cursor()
    try:
        return enforce.govern(sql, principal, cur, default_catalog=catalog or "warehouse", default_schema="main",
                              client=client, trusted=trusted)
    finally:
        if own:
            try:
                cur.close()
            except Exception:
                pass


def propagate_tags(sql: str, user, result: RewriteResult, *, catalog: Optional[str] = None, con=None) -> List[Dict[str, str]]:
    """After a successful CREATE TABLE AS / INSERT ... SELECT by an exempt principal: tag the new table like its sources."""
    from web.governance import propagate
    principal = user if isinstance(user, Principal) else principal_for(user)
    own = con is None
    cur = con if con is not None else _cursor()
    try:
        return propagate.propagate_after(sql, principal, result, cur, default_catalog=catalog or "warehouse")
    finally:
        if own:
            try:
                cur.close()
            except Exception:
                pass


def governed_sql_or_raise(sql: str, user, **kw) -> str:
    result = govern_sql(sql, user, **kw)
    if result.blocked:
        raise GovernanceBlocked(result.blocked)
    return result.sql


def masked_columns_payload(result: RewriteResult) -> List[Dict[str, str]]:
    seen, out = set(), []
    for m in result.masked:
        key = (m.table, m.column)
        if key not in seen:
            seen.add(key)
            out.append({"table": m.table, "column": m.column, "policy": m.policy_name, "mask": m.mask_type})
    return out


def fingerprint(result: RewriteResult) -> str:
    """Stable id of the set of masks a result was computed under; part of every result-cache key."""
    if not result.masked:
        return ""
    parts = sorted(f"{m.table}.{m.column}:{m.policy_id}:{m.mask_type}" for m in result.masked)
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]


def is_subject(user) -> bool:
    """True when at least one masking policy applies to this user (and tagged data exists)."""
    principal = user if isinstance(user, Principal) else principal_for(user)
    if principal.is_system or not tags.has_any_tags():
        return False
    return any(not policies.is_exempt(p, principal) for p in policies.enabled_policies())


def deny_if_subject(user, what: str) -> None:
    """Refuses features that read data outside the governed catalogs (they cannot be masked)."""
    if is_subject(user):
        raise GovernanceBlocked(f"{what} is not available while masking policies apply to you, because it reads data "
                                "that cannot be masked. Use the SQL editor instead.")


_provisioned: "weakref.WeakSet" = None


def ensure_masks(conn) -> None:
    """Installs the mask primitives on a connection some module opened itself (duckrun.connect(...)); idempotent."""
    import weakref
    global _provisioned
    if _provisioned is None:
        _provisioned = weakref.WeakSet()
    raw = getattr(conn, "con", conn)
    try:
        if raw in _provisioned:
            return
    except TypeError:
        pass
    from web.governance.macros import install_governance_macros, macros_installed
    if not macros_installed(raw):
        install_governance_macros(raw)
    try:
        _provisioned.add(raw)
    except TypeError:
        pass


def _tester():
    global _mask_con
    if _mask_con is None:
        import duckdb
        from web.governance.macros import install_governance_macros
        con = duckdb.connect(":memory:")
        install_governance_macros(con)
        _mask_con = con
    return _mask_con


def mask_arrow(table: pa.Table, catalog: str, schema_name: str, table_name: str, user) -> pa.Table:
    """Applies the masks that `user` is subject to for (catalog.schema.table) to an Arrow table Python code already holds."""
    principal = user if isinstance(user, Principal) else principal_for(user)
    if principal.is_system or not tags.has_any_tags() or table.num_columns == 0:
        return table
    cur = _tester().cursor()
    try:
        cur.register("__gov_in", table)
        described = cur.execute("DESCRIBE SELECT * FROM __gov_in").fetchall()
        columns = [{"column": r[0], "type": r[1]} for r in described]
        specs = policies.masks_for_table(catalog, schema_name, table_name, columns, principal)
        if not specs:
            return table
        replace = ", ".join(f'{s.expression} AS "{s.column}"' for s in specs)
        return cur.execute(f"SELECT * REPLACE ({replace}) FROM __gov_in").arrow().read_all()
    finally:
        try:
            cur.unregister("__gov_in")
        except Exception:
            pass
        cur.close()
