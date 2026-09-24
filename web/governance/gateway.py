"""
Glue between the application and the enforcement engine: one small API every data-egress path uses.

    principal_for(user)                 dict from auth (or None) -> Principal; unknown means least privilege
    govern_sql(sql, user, ...)          rewrite + audit; returns a RewriteResult
    governed_sql_or_raise(sql, user)    the SQL to run, or raises GovernanceBlocked
    masked_columns_payload(result)      what the UI shows next to a result
    row_filter_payload(result)          which tables had row filters applied, for the same banner
    fingerprint(result)                 cache-key component: results are only shareable between equal mask/filter sets
    mask_arrow(table, catalog, ...)     masks + row-filters an Arrow table Python code already holds (version diffs, previews)
    deny_if_subject(user, what)         for features that cannot be masked or row-filtered (refused for governed principals)
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional

import pyarrow as pa

from web.governance import enforce, policies, row_filters, tags
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


def row_filter_payload(result: RewriteResult) -> List[Dict[str, str]]:
    """Which tables had a row filter applied, for the same UI banner masked_columns_payload feeds."""
    seen, out = set(), []
    for r in result.row_filtered:
        if r.table not in seen:
            seen.add(r.table)
            out.append({"table": r.table, "column": r.filter_column, "policy": r.policy_name})
    return out


def fingerprint(result: RewriteResult) -> str:
    """Stable id of the masks and row filters a result was computed under; part of every result-cache key."""
    if not result.masked and not result.row_filtered:
        return ""
    parts = sorted(f"{m.table}.{m.column}:{m.policy_id}:{m.mask_type}" for m in result.masked)
    # predicate_digest, not the policy id alone: two users under the same policy can resolve to different predicates
    # (different username, different assigned attribute values), and must never share a cached result.
    parts += sorted(f"{r.table}:{r.policy_id}:{r.predicate_digest}" for r in result.row_filtered)
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]


def is_subject(user) -> bool:
    """True when at least one masking or row filter policy applies to this user (and tagged data exists)."""
    principal = user if isinstance(user, Principal) else principal_for(user)
    if principal.is_system or not tags.has_any_tags():
        return False
    if any(not policies.is_exempt(p, principal) for p in policies.enabled_policies()):
        return True
    return any(not policies.is_exempt(p, principal) for p in row_filters.enabled_row_policies())


def deny_if_subject(user, what: str) -> None:
    """Refuses features that read data outside the governed catalogs (they cannot be masked or row-filtered)."""
    if is_subject(user):
        raise GovernanceBlocked(f"{what} is not available while governance policies apply to you, because it reads "
                                "data that cannot be masked or row-filtered. Use the SQL editor instead.")


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
    """
    Applies the masks and row filters that `user` is subject to for (catalog.schema.table) to an Arrow table Python
    code already holds (there is no SQL to rewrite here, so this runs the same specs directly against the table).
    """
    principal = user if isinstance(user, Principal) else principal_for(user)
    if principal.is_system or not tags.has_any_tags() or table.num_columns == 0:
        return table
    cur = _tester().cursor()
    try:
        cur.register("__gov_in", table)
        described = cur.execute("DESCRIBE SELECT * FROM __gov_in").fetchall()
        columns = [{"column": r[0], "type": r[1]} for r in described]
        specs = policies.masks_for_table(catalog, schema_name, table_name, columns, principal)
        row_specs = row_filters.filters_for_table(catalog, schema_name, table_name, columns, principal)
        if not specs and not row_specs:
            return table
        replace = ", ".join(f'{s.expression} AS "{s.column}"' for s in specs)
        select = f"SELECT * REPLACE ({replace})" if specs else "SELECT *"
        where = " AND ".join(f"({p.predicate})" for p in row_specs)
        sql = select + " FROM __gov_in" + (f" WHERE {where}" if where else "")
        return cur.execute(sql).arrow().read_all()
    finally:
        try:
            cur.unregister("__gov_in")
        except Exception:
            pass
        cur.close()
