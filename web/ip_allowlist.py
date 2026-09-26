"""Per-deployment IP allowlist (CIDR rules) enforced by a middleware in `web/app.py`, with a trusted-proxy setting.

Modes  off      no check.
       monitor  requests are allowed but every request that WOULD be blocked is recorded (roll a new list out safely).
       enforce  a request from an address outside every rule gets 403 before anything else runs (login, SSO callbacks, the UI shell, /docs).

Which address is "the client"? Only the TCP peer is trustworthy. `X-Forwarded-For` is honoured only when the peer itself is a *trusted proxy*
(setting `trusted_proxies`, plus the `TRUSTED_PROXIES` environment variable); the header is then read from the right, skipping trusted proxies,
and the first other address is the client (the standard safe algorithm: whatever a client puts at the left of the header is never believed). A
request from an untrusted peer is judged by the peer, whatever headers it carries. An unparsable hop fails closed. Run uvicorn with
`--no-proxy-headers` (the compose file does) so the ASGI peer is the real socket address and nothing rewrites it before this check.

Always allowed (not configurable, and never a way around the list): loopback (127.0.0.0/8, ::1) when the request carries no forwarding headers, so
`docker exec` health checks keep working, and the notebook sandbox's own calls to /api/sandbox/* (which another middleware confines to that path).
Loopback WITH forwarding headers is judged like anyone else: it means a reverse proxy on the same host that has not been declared trusted.

Guards: a change that would block the administrator making it (judged with the new proxy settings, from their own request) is refused; rules of
/0 are refused; `IP_ALLOWLIST_OVERRIDE=off` in the environment suspends enforcement without touching the stored setting (break-glass).
"""
import collections
import ipaddress
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("localspark.ip_allowlist")

MODES = ("off", "monitor", "enforce")
MAX_RULES = 500
FORWARD_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded")


class AllowlistError(ValueError):
    """Invalid configuration (the message is safe to show)."""


def _conn():
    from web.auth import get_db_connection
    return get_db_connection()


def init_db() -> None:
    conn = _conn()
    try:
        with conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS ip_allowlist (
                id INTEGER PRIMARY KEY CHECK (id = 1), mode TEXT NOT NULL DEFAULT 'off', rules TEXT NOT NULL DEFAULT '[]',
                trusted_proxies TEXT NOT NULL DEFAULT '[]', updated_by TEXT, updated_at INTEGER)""")
            conn.execute("INSERT OR IGNORE INTO ip_allowlist (id) VALUES (1)")
    finally:
        conn.close()


# ---------------------------------------------------------------- parsing

def parse_ip(text: Any) -> Optional[ipaddress._BaseAddress]:
    """An address as an ipaddress object; IPv4-mapped IPv6 (::ffff:a.b.c.d) counts as the IPv4 address. None when it is not an address."""
    s = str(text or "").strip()
    if s.startswith("[") and "]" in s:
        s = s[1:s.index("]")]
    elif s.count(":") == 1 and "." in s:                # a.b.c.d:port
        s = s.rsplit(":", 1)[0]
    try:
        ip = ipaddress.ip_address(s)
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        return ip.ipv4_mapped
    return ip


def parse_cidr(text: Any) -> ipaddress._BaseNetwork:
    s = str(text or "").strip()
    if not s:
        raise AllowlistError("Enter an address range such as 203.0.113.0/24, or a single address such as 198.51.100.7.")
    try:
        net = ipaddress.ip_network(s, strict=False)
    except ValueError:
        raise AllowlistError(f"'{s[:60]}' is not a valid address or CIDR range.")
    if net.prefixlen == 0:
        raise AllowlistError("A /0 range allows every address; turn the allowlist off instead.")
    return net


def _clean_entries(items: Any, what: str) -> List[Dict[str, str]]:
    if not isinstance(items, list):
        raise AllowlistError(f"The {what} must be a list.")
    if len(items) > MAX_RULES:
        raise AllowlistError(f"At most {MAX_RULES} {what}.")
    out, seen = [], set()
    for it in items:
        cidr = it.get("cidr") if isinstance(it, dict) else it
        net = parse_cidr(cidr)
        key = str(net)
        if key in seen:
            continue
        seen.add(key)
        out.append({"cidr": key, "note": str((it.get("note") if isinstance(it, dict) else "") or "")[:120]})
    return out


def _env_proxies() -> List[ipaddress._BaseNetwork]:
    out = []
    for part in os.getenv("TRUSTED_PROXIES", "").replace(";", ",").split(","):
        if part.strip():
            try:
                out.append(parse_cidr(part))
            except AllowlistError:
                logger.warning(f"TRUSTED_PROXIES: ignoring '{part.strip()[:40]}'")
    return out


def overridden() -> bool:
    return os.getenv("IP_ALLOWLIST_OVERRIDE", "").strip().lower() in ("off", "0", "false", "disabled")


# ---------------------------------------------------------------- configuration (cached: the gate reads it on every request)

_cache: Dict[str, Any] = {"at": 0.0, "row": None}


def _invalidate() -> None:
    _cache["at"] = 0.0


def get_config() -> Dict[str, Any]:
    if time.time() - _cache["at"] > 2 or _cache["row"] is None:
        init_db()
        conn = _conn()
        try:
            _cache["row"] = dict(conn.execute("SELECT * FROM ip_allowlist WHERE id = 1").fetchone())
            _cache["at"] = time.time()
        finally:
            conn.close()
    r = _cache["row"]
    rules = json.loads(r["rules"] or "[]")
    proxies = json.loads(r["trusted_proxies"] or "[]")
    return {"stored_mode": r["mode"], "mode": "off" if overridden() else r["mode"], "overridden": overridden() and r["mode"] != "off", "rules": rules,
            "trusted_proxies": proxies, "env_trusted_proxies": [str(n) for n in _env_proxies()], "updated_by": r["updated_by"], "updated_at": r["updated_at"]}


def _networks(entries: Iterable[Any]) -> List[ipaddress._BaseNetwork]:
    out = []
    for e in entries:
        try:
            out.append(ipaddress.ip_network(e["cidr"] if isinstance(e, dict) else e, strict=False))
        except ValueError:
            pass
    return out


def _in(ip, nets: List[ipaddress._BaseNetwork]) -> bool:
    return any(ip.version == n.version and ip in n for n in nets)


# ---------------------------------------------------------------- who is the client

def resolve_client(peer: Optional[str], headers: Dict[str, str], cfg: Optional[Dict[str, Any]] = None,
                   trusted_override: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
    """{ip (object or None), text, peer, via_proxy, forwarded (raw header), forwarding_headers_present}. See the module docstring."""
    cfg = cfg or get_config()
    proxies = _networks(trusted_override if trusted_override is not None else cfg["trusted_proxies"]) + _env_proxies()
    peer_ip = parse_ip(peer)
    headers = {k.lower(): v for k, v in headers.items()}
    present = any(h in headers for h in FORWARD_HEADERS)
    out = {"ip": peer_ip, "text": str(peer_ip) if peer_ip else (peer or ""), "peer": str(peer_ip) if peer_ip else (peer or ""), "via_proxy": False,
           "forwarded": headers.get("x-forwarded-for", ""), "forwarding_headers_present": present}
    if peer_ip is None or not _in(peer_ip, proxies):
        return out                                      # an untrusted peer is judged by itself, whatever its headers say
    xff = headers.get("x-forwarded-for", "")
    if not xff:
        return out                                      # a trusted proxy that forwarded nothing: the proxy is the client
    hops = [h.strip() for h in xff.split(",") if h.strip()]
    for hop in reversed(hops):
        ip = parse_ip(hop)
        if ip is None:
            return {**out, "ip": None, "text": hop[:60], "via_proxy": True}       # garbage in the header: fail closed
        if not _in(ip, proxies):
            return {**out, "ip": ip, "text": str(ip), "via_proxy": True}
    ip = parse_ip(hops[0])                              # every hop was a trusted proxy: the original sender is the leftmost
    return {**out, "ip": ip, "text": str(ip) if ip else hops[0][:60], "via_proxy": True}


def decide(client: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None, path: str = "") -> Tuple[bool, str]:
    """(allowed, why)."""
    cfg = cfg or get_config()
    ip = client["ip"]
    if ip is None:
        return False, "unparsable address"
    if ip.is_loopback and not client["forwarding_headers_present"] and not client["via_proxy"]:
        return True, "loopback"
    if _in(ip, _networks(cfg["rules"])):
        return True, "rule"
    return False, "no rule matches"


def check_request(peer: Optional[str], headers: Dict[str, str], path: str, sandbox_peer: bool = False) -> Tuple[str, Dict[str, Any], str]:
    """('allow'|'block'|'would_block', client, why) for the middleware. In monitor mode a request that would be blocked is allowed and recorded."""
    cfg = get_config()
    client = resolve_client(peer, headers, cfg)
    if cfg["mode"] == "off":
        return "allow", client, "off"
    if sandbox_peer and path.startswith("/api/sandbox/"):
        return "allow", client, "sandbox"
    ok, why = decide(client, cfg, path)
    if ok:
        return "allow", client, why
    return ("block" if cfg["mode"] == "enforce" else "would_block"), client, why


# ---------------------------------------------------------------- what was refused (in memory: a flood must not become a write load)

_events: "collections.deque[Dict[str, Any]]" = collections.deque(maxlen=200)
_counts: "collections.OrderedDict[str, Dict[str, Any]]" = collections.OrderedDict()
_lock_events = __import__("threading").Lock()


def record(verdict: str, client: Dict[str, Any], method: str, path: str) -> None:
    now = int(time.time())
    key = client["text"] or "?"
    with _lock_events:
        _events.appendleft({"at": now, "ip": key, "verdict": verdict, "method": method, "path": path[:120]})
        c = _counts.get(key) or {"ip": key, "blocked": 0, "would_block": 0, "first_at": now, "last_at": now}
        c["blocked" if verdict == "block" else "would_block"] += 1
        c["last_at"] = now
        _counts[key] = c
        _counts.move_to_end(key)
        while len(_counts) > 500:
            _counts.popitem(last=False)


def activity() -> Dict[str, Any]:
    with _lock_events:
        top = sorted(_counts.values(), key=lambda c: c["blocked"] + c["would_block"], reverse=True)[:20]
        return {"recent": list(_events)[:50], "top": [dict(t) for t in top]}


# ---------------------------------------------------------------- administration

def _audit(actor: str, action: str, detail: Dict[str, Any]) -> None:
    try:
        from web.governance import store
        store.init_governance_db()
        c = store.get_db()
        try:
            store.write_audit(c, actor, action, "ip_allowlist", detail)
            c.commit()
        finally:
            c.close()
    except Exception as exc:
        logger.warning(f"could not audit {action}: {exc}")


def set_config(mode: str, rules: Any, trusted_proxies: Any, actor: str, actor_peer: Optional[str], actor_headers: Dict[str, str]) -> Dict[str, Any]:
    if mode not in MODES:
        raise AllowlistError(f"The mode must be one of {', '.join(MODES)}.")
    rules = _clean_entries(rules or [], "rules")
    proxies = _clean_entries(trusted_proxies or [], "trusted proxies")
    if mode == "enforce" and not rules:
        raise AllowlistError("Enforcing an empty list would block everybody. Add at least the range you are working from.")
    if mode == "enforce":
        # would this very request still get through? (with the new proxy settings and the new rules)
        me = resolve_client(actor_peer, actor_headers, None, trusted_override=proxies)
        ok, _why = decide(me, {"rules": rules})
        if not ok:
            raise AllowlistError(f"Your current address ({me['text'] or 'unknown'}) would be blocked by these rules"
                                 f"{' (as seen through the trusted proxy settings)' if me['via_proxy'] else ''}. Add it first, so you cannot lock yourself out.")
    conn = _conn()
    try:
        init_db()
        with conn:
            conn.execute("UPDATE ip_allowlist SET mode = ?, rules = ?, trusted_proxies = ?, updated_by = ?, updated_at = ? WHERE id = 1",
                         (mode, json.dumps(rules), json.dumps(proxies), actor, int(time.time())))
    finally:
        conn.close()
    _invalidate()
    _audit(actor, "IP_ALLOWLIST_UPDATE", {"mode": mode, "rules": [r["cidr"] for r in rules], "trusted_proxies": [p["cidr"] for p in proxies]})
    return get_config()


def whoami(peer: Optional[str], headers: Dict[str, str]) -> Dict[str, Any]:
    cfg = get_config()
    c = resolve_client(peer, headers, cfg)
    ok, why = decide(c, cfg)
    warning = ""
    if c["forwarding_headers_present"] and not c["via_proxy"]:
        warning = ("This request carries forwarding headers (X-Forwarded-For...) but its sender is not a trusted proxy, so the headers are ignored and every user "
                   "behind that proxy looks like the proxy itself. If you run a reverse proxy, add its address to the trusted proxies.")
    return {"ip": c["text"], "peer": c["peer"], "via_proxy": c["via_proxy"], "forwarded_for": c["forwarded"], "allowed": ok, "why": why, "warning": warning}


def check_address(text: str) -> Dict[str, Any]:
    """Would this address be allowed under the stored rules (ignoring proxies and the mode)?"""
    ip = parse_ip(text)
    if ip is None:
        raise AllowlistError("That is not an IP address.")
    cfg = get_config()
    hit = next((r for r in cfg["rules"] if ip.version == ipaddress.ip_network(r["cidr"], strict=False).version and ip in ipaddress.ip_network(r["cidr"], strict=False)), None)
    return {"ip": str(ip), "allowed": bool(hit) or ip.is_loopback, "rule": hit["cidr"] if hit else ("loopback" if ip.is_loopback else None)}
