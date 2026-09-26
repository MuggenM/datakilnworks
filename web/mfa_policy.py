"""Organisation-wide two-factor policy on top of the per-user TOTP in `web/mfa.py`.

An administrator can REQUIRE two-factor authentication for chosen roles with a grace period. This module owns the setting, the per-user status
and the statistics; `web/app.py` enforces it in a middleware (`_mfa_policy_gate`) so a direct API call cannot skip it.

Who is covered: accounts whose password this studio checks (`auth_source` local or ldap) with a role selected in the policy. Accounts that sign
in through OIDC or SAML are never asked for a second factor here (their identity provider owns that) and are reported separately in the stats.

Status per covered user
  compliant  two-factor authentication is on
  grace      not on yet, before the user's deadline (banner in the UI, everything works)
  overdue    not on, deadline passed: the API refuses everything except enrolling (like a forced password change)
  exempt     an administrator excused the account (with a reason; e.g. a service account)
The deadline is max(when the policy was turned on, when the account was created) + grace days, unless an administrator extended it.
A new account under a policy with 0 grace days must therefore enrol at its first sign-in.

Guards: turning the policy on is refused while the acting administrator is covered but has no second factor themselves (nobody locks themselves
out by saving the form); a covered user cannot turn their own two-factor authentication off; and `MFA_POLICY_OVERRIDE=off` in the environment
switches enforcement off without touching the database (break-glass for a lost admin device with no backup codes).
"""
import datetime
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("localspark.mfa_policy")

ALL_ROLES = ("admin", "power_user", "user")
COVERED_SOURCES = ("local", "ldap")
MAX_GRACE_DAYS = 365
DAY = 86400


class PolicyError(ValueError):
    """Invalid policy change (the message is safe to show)."""


def _conn():
    from web.auth import get_db_connection
    return get_db_connection()


def init_policy_db() -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS mfa_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1), enabled INTEGER NOT NULL DEFAULT 0, roles TEXT NOT NULL,
                grace_days INTEGER NOT NULL DEFAULT 14, enabled_at INTEGER, updated_by TEXT, updated_at INTEGER)""")
            conn.execute("INSERT OR IGNORE INTO mfa_policy (id, enabled, roles, grace_days) VALUES (1, 0, ?, 14)", (json.dumps(list(ALL_ROLES)),))
    finally:
        conn.close()


def overridden() -> bool:
    return os.getenv("MFA_POLICY_OVERRIDE", "").strip().lower() in ("off", "0", "false", "disabled")


_cache: Dict[str, Any] = {"at": 0.0, "row": None}


def _invalidate() -> None:
    _cache["at"] = 0.0


def get_policy() -> Dict[str, Any]:
    """The policy (a short cache: the request gate calls this on every API request; a change applies within seconds in every process)."""
    if time.time() - _cache["at"] > 2 or _cache["row"] is None:
        init_policy_db()
        conn = _conn()
        try:
            _cache["row"] = dict(conn.execute("SELECT * FROM mfa_policy WHERE id = 1").fetchone())
            _cache["at"] = time.time()
        finally:
            conn.close()
    r = _cache["row"]
    return _policy_from_row(r)


def _policy_from_row(r: Dict[str, Any]) -> Dict[str, Any]:
    roles = [x for x in json.loads(r["roles"] or "[]") if x in ALL_ROLES]
    stored_enabled = bool(r["enabled"])
    return {"enabled": stored_enabled and not overridden(), "stored_enabled": stored_enabled, "overridden": overridden() and stored_enabled,
            "roles": roles, "grace_days": int(r["grace_days"]), "enabled_at": r["enabled_at"], "updated_by": r["updated_by"], "updated_at": r["updated_at"]}


def _audit(actor: str, action: str, target: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, target, detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


def _epoch(created_at: Optional[str]) -> int:
    try:
        return int(datetime.datetime.strptime(str(created_at)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
    except Exception:
        return 0


# ---------------------------------------------------------------- per-user status

def user_status(u: Dict[str, Any], policy: Optional[Dict[str, Any]] = None, now: Optional[float] = None) -> Dict[str, Any]:
    """{required, state, deadline, days_left, ...} for one user row (needs role, auth_source, created_at, totp_enabled or mfa_enabled, and the
    optional mfa_exempt / mfa_deadline_override columns)."""
    policy = policy or get_policy()
    now = time.time() if now is None else now
    enrolled = bool(u.get("totp_enabled") or u.get("mfa_enabled"))
    source = u.get("auth_source") or "local"
    base = {"required": False, "enrolled": enrolled, "deadline": None, "days_left": None, "exempt": bool(u.get("mfa_exempt")),
            "exempt_reason": u.get("mfa_exempt_reason") or ""}
    if not policy["enabled"]:
        return {**base, "state": "compliant" if enrolled else "off"}
    if source not in COVERED_SOURCES:
        return {**base, "state": "sso" if source in ("oidc", "saml") else "not_covered"}
    if u.get("role") not in policy["roles"]:
        return {**base, "state": "compliant" if enrolled else "not_covered"}
    if u.get("mfa_exempt"):
        return {**base, "state": "compliant" if enrolled else "exempt"}
    override = u.get("mfa_deadline_override")
    started = max(int(policy["enabled_at"] or 0), _epoch(u.get("created_at")))
    deadline = int(override) if override else started + int(policy["grace_days"]) * DAY
    base.update(required=True, deadline=deadline, days_left=max(0, -(-(deadline - int(now)) // DAY)) if deadline > now else 0,
                extended=bool(override))
    if enrolled:
        return {**base, "state": "compliant"}
    return {**base, "state": "grace" if now < deadline else "overdue"}


def status_for_user_id(user_id: str) -> Dict[str, Any]:
    conn = _conn()
    try:
        r = conn.execute("SELECT id, role, auth_source, created_at, is_active, deleted_at, totp_enabled, mfa_exempt, mfa_exempt_reason, mfa_deadline_override "
                         "FROM users WHERE id = ?", (user_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        return {"required": False, "state": "off", "enrolled": False, "deadline": None, "days_left": None}
    return user_status(dict(r))


def is_blocked(u: Dict[str, Any]) -> bool:
    """True when this user (a full users row) may only enrol: covered, not enrolled, deadline passed."""
    return user_status(u)["state"] == "overdue"


def may_disable_own_mfa(u: Dict[str, Any]) -> bool:
    """A covered, non-exempt user cannot switch their second factor off while the policy is on."""
    return not user_status(u)["required"]


# ---------------------------------------------------------------- changing the policy

def set_policy(enabled: bool, roles: List[str], grace_days: int, actor: Dict[str, Any]) -> Dict[str, Any]:
    roles = [r for r in dict.fromkeys(roles or []) if r in ALL_ROLES]
    if enabled and not roles:
        raise PolicyError("Choose at least one role the policy applies to.")
    try:
        grace_days = int(grace_days)
    except (TypeError, ValueError):
        raise PolicyError("The grace period must be a number of days.")
    if not 0 <= grace_days <= MAX_GRACE_DAYS:
        raise PolicyError(f"The grace period must be between 0 and {MAX_GRACE_DAYS} days.")
    cur = get_policy()
    if enabled:
        me = {**actor}
        me_status = user_status({**me, "totp_enabled": me.get("mfa_enabled")}, {"enabled": True, "roles": roles, "grace_days": grace_days, "enabled_at": time.time()})
        if me_status["required"] and not me_status["enrolled"]:
            raise PolicyError("You are covered by this policy but have not turned on two-factor authentication yourself. Turn it on for your own account first "
                              "(your account menu), so you cannot lock yourself out.")
    now = int(time.time())
    # the grace period starts when the policy is switched on (changing roles or days later does not give anyone a fresh start)
    restart = enabled and not cur["stored_enabled"]
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE mfa_policy SET enabled = ?, roles = ?, grace_days = ?, enabled_at = ?, updated_by = ?, updated_at = ? WHERE id = 1",
                         (1 if enabled else 0, json.dumps(roles), grace_days, now if restart else (cur["enabled_at"] or now), actor.get("username"), now))
    finally:
        conn.close()
    _invalidate()
    _audit(actor.get("username", "?"), "MFA_POLICY_UPDATE", "mfa_policy", {"enabled": enabled, "roles": roles, "grace_days": grace_days})
    return get_policy()


def _user(user_id: str) -> Dict[str, Any]:
    conn = _conn()
    try:
        r = conn.execute("SELECT id, username, role, auth_source FROM users WHERE id = ? AND deleted_at IS NULL", (user_id,)).fetchone()
    finally:
        conn.close()
    if not r:
        raise LookupError("User not found.")
    return dict(r)


def set_exempt(user_id: str, exempt: bool, reason: str, actor: Dict[str, Any]) -> None:
    u = _user(user_id)
    reason = (reason or "").strip()
    if exempt and not reason:
        raise PolicyError("Give a reason for the exemption (it is shown to other administrators and audited).")
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE users SET mfa_exempt = ?, mfa_exempt_reason = ? WHERE id = ?", (1 if exempt else 0, reason[:300] if exempt else None, user_id))
    finally:
        conn.close()
    _audit(actor.get("username", "?"), "MFA_EXEMPT" if exempt else "MFA_EXEMPT_REMOVED", f"user:{u['username']}", {"reason": reason[:300]})


def extend_deadline(user_id: str, days: int, actor: Dict[str, Any]) -> int:
    """Sets the user's deadline to `days` from now (0 removes the extension). Returns the new deadline (epoch) or 0."""
    u = _user(user_id)
    try:
        days = int(days)
    except (TypeError, ValueError):
        raise PolicyError("Days must be a number.")
    if not 0 <= days <= MAX_GRACE_DAYS:
        raise PolicyError(f"An extension is between 0 and {MAX_GRACE_DAYS} days.")
    deadline = int(time.time()) + days * DAY if days else None
    conn = _conn()
    try:
        with conn:
            conn.execute("UPDATE users SET mfa_deadline_override = ? WHERE id = ?", (deadline, user_id))
    finally:
        conn.close()
    _audit(actor.get("username", "?"), "MFA_EXTEND", f"user:{u['username']}", {"days": days})
    return deadline or 0


# ---------------------------------------------------------------- statistics

def stats(now: Optional[float] = None) -> Dict[str, Any]:
    now = time.time() if now is None else now
    policy = get_policy()
    conn = _conn()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT id, username, display_name, role, auth_source, created_at, is_active, totp_enabled, totp_enrolled_at, mfa_exempt, mfa_exempt_reason, "
            "mfa_deadline_override, last_login_at FROM users WHERE deleted_at IS NULL AND is_active = 1")]
    finally:
        conn.close()
    states = {"compliant": 0, "grace": 0, "overdue": 0, "exempt": 0}
    by_role = {r: {"total": 0, "enrolled": 0, "grace": 0, "overdue": 0, "exempt": 0} for r in ALL_ROLES}
    attention: List[Dict[str, Any]] = []
    sso = enrolled_all = 0
    weekly = [0] * 8
    for u in rows:
        st = user_status(u, policy, now)
        if u["totp_enabled"]:
            enrolled_all += 1
            e = u.get("totp_enrolled_at")
            if e and now - e < 8 * 7 * DAY:
                weekly[7 - int((now - e) // (7 * DAY))] += 1
        if st["state"] == "sso":
            sso += 1
            continue
        if not st["required"] and st["state"] != "exempt":
            continue
        role = by_role.get(u["role"])
        if role is not None:
            role["total"] += 1
        key = {"compliant": "enrolled"}.get(st["state"], st["state"])
        if role is not None and key in role:
            role[key] += 1
        if st["state"] in states:
            states[st["state"]] += 1
        if st["state"] in ("grace", "overdue", "exempt"):
            attention.append({"id": u["id"], "username": u["username"], "display_name": u["display_name"], "role": u["role"], "auth_source": u["auth_source"],
                              "state": st["state"], "deadline": st["deadline"], "days_left": st["days_left"], "extended": st.get("extended", False),
                              "exempt_reason": st["exempt_reason"], "last_login_at": u["last_login_at"]})
    covered = sum(states.values())
    attention.sort(key=lambda a: ({"overdue": 0, "grace": 1, "exempt": 2}[a["state"]], a["deadline"] or 0))
    return {"policy": policy, "covered": covered, **states, "sso_accounts": sso, "enrolled_total": enrolled_all, "users_total": len(rows),
            "coverage_percent": round(100 * states["compliant"] / covered, 1) if covered else None,
            "by_role": by_role, "enrolled_per_week": weekly, "attention": attention, "as_of": int(now)}
