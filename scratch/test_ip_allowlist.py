#!/usr/bin/env python3
"""IP allowlist (web/ip_allowlist.py + the _ip_allowlist_gate middleware) against a throwaway WAREHOUSE_DIR: CIDR rules (IPv4, IPv6, mapped), the
trusted-proxy algorithm (spoofed headers, chains, garbage), loopback and sandbox exemptions, lock-out guards, monitor mode, env break-glass."""
import os, shutil, sys, tempfile, sqlite3
TMP = tempfile.mkdtemp(prefix="ipal_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES"): os.environ.pop(v, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth, ip_allowlist as ipa, sandbox_client
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def from_(ip, **kw): return TestClient(app_module.app, client=(ip, 50000), **kw)
def put(c, mode, rules=(), proxies=(), **h): return c.put("/api/ip-allowlist", json={"mode": mode, "rules": [{"cidr": r} for r in rules], "trusted_proxies": [{"cidr": p} for p in proxies]}, headers=h)
def status(c, path="/api/auth/me", **h): return c.get(path, headers=h).status_code
try:
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    auth.create_user("alice", "alicepassword1", "Alice", role="user")
    OFFICE = "203.0.113.5"
    admin = from_(OFFICE); assert admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200

    print("off by default, guards")
    check("nothing is checked by default", ipa.get_config()["mode"] == "off" and status(from_("198.51.100.9")) == 200)
    alice = from_(OFFICE); alice.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"})
    check("only administrators may read or change it", alice.get("/api/ip-allowlist").status_code == 403 and put(alice, "off").status_code == 403 and alice.post("/api/ip-allowlist/check", json={"ip": "1.1.1.1"}).status_code == 403)
    for label, r, frag in (("an empty list cannot be enforced", put(admin, "enforce"), "empty"),
                           ("a /0 rule is refused", put(admin, "enforce", ["0.0.0.0/0"]), "/0"), ("an IPv6 /0 rule is refused", put(admin, "enforce", ["::/0"]), "/0"),
                           ("a garbage rule is refused", put(admin, "enforce", ["not-a-range"]), "not a valid"),
                           ("a rule that would block the administrator making the change is refused", put(admin, "enforce", ["198.51.100.0/24"]), "would be blocked"),
                           ("a bad mode is refused", put(admin, "sometimes", ["203.0.113.0/24"]), "mode")):
        check(label, r.status_code == 400 and frag.lower() in r.json()["detail"].lower(), r.text)
    check("the refusal names the administrator's address", OFFICE in put(admin, "enforce", ["198.51.100.0/24"]).json()["detail"])
    check("nothing changed after the refusals", ipa.get_config()["mode"] == "off")

    print("enforcing rules")
    r = put(admin, "enforce", ["203.0.113.0/24", "198.51.100.77", "2001:db8::/32"])
    check("valid rules are saved (a single address becomes /32, host bits are normalised)", r.status_code == 200 and {x["cidr"] for x in r.json()["rules"]} == {"203.0.113.0/24", "198.51.100.77/32", "2001:db8::/32"}, r.text)
    check("an address inside a rule passes", status(from_("203.0.113.200")) == 200 and status(from_("198.51.100.77")) == 200)
    blocked = from_("198.51.100.9").get("/api/auth/me")
    check("an address outside every rule gets 403 with a JSON reason and its address", blocked.status_code == 403 and blocked.json()["ip_blocked"] is True and blocked.json()["your_address"] == "198.51.100.9", blocked.text)
    page = from_("198.51.100.9").get("/"); check("the UI shell gets a small denial page, not the app", page.status_code == 403 and "Access denied" in page.text and "198.51.100.9" in page.text)
    check("login, the docs and static routes are blocked as well", from_("198.51.100.9").post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 403 and status(from_("198.51.100.9"), "/docs/") == 403 and status(from_("198.51.100.9"), "/api/docs") == 403)
    check("IPv6 rules work", status(from_("2001:db8:1::5")) == 200 and status(from_("2001:dead::1")) == 403)
    check("an IPv4-mapped IPv6 peer is judged as the IPv4 address", status(from_("::ffff:203.0.113.9")) == 200 and status(from_("::ffff:198.51.100.9")) == 403)
    check("an address that is not an IP (unparsable peer) fails closed", status(from_("not-an-ip")) == 403)
    check("a request without forwarding headers from loopback always passes (docker exec / health checks)", status(from_("127.0.0.1")) == 200 and status(from_("::1")) == 200)
    check("loopback WITH forwarding headers is a proxy nobody declared: judged like anyone else", status(from_("127.0.0.1"), **{"X-Forwarded-For": "203.0.113.9"}) == 403)
    check("an untrusted peer cannot borrow an allowed address with X-Forwarded-For", status(from_("198.51.100.9"), **{"X-Forwarded-For": OFFICE}) == 403 and status(from_("198.51.100.9"), **{"X-Real-IP": OFFICE}) == 403)
    check("the administrator still gets in and sees the numbers", admin.get("/api/ip-allowlist").json()["config"]["mode"] == "enforce")

    print("trusted proxies")
    put(admin, "off", [])                       # a proxy has to be declared while the list is not (yet) locking it out
    behind = from_("10.0.0.2"); behind.cookies.update(admin.cookies)
    r = put(behind, "enforce", ["203.0.113.0/24"], **{"X-Forwarded-For": OFFICE})
    check("behind a proxy that is not declared, the administrator looks like the proxy and the change is refused", r.status_code == 400 and "10.0.0.2" in r.json()["detail"], r.text)
    r = put(behind, "enforce", ["203.0.113.0/24"], ["10.0.0.0/8"], **{"X-Forwarded-For": OFFICE})
    check("with the proxy declared the administrator's real address is used and the change is accepted", r.status_code == 200 and r.json()["trusted_proxies"][0]["cidr"] == "10.0.0.0/8", r.text)
    P = from_("10.0.0.2")
    check("a trusted proxy's forwarded client is judged", status(P, **{"X-Forwarded-For": "203.0.113.9"}) == 200 and status(P, **{"X-Forwarded-For": "198.51.100.9"}) == 403)
    check("only the rightmost non-proxy hop counts: a spoofed left entry does not help", status(P, **{"X-Forwarded-For": "203.0.113.9, 198.51.100.9"}) == 403)
    check("chains of proxies are skipped from the right", status(P, **{"X-Forwarded-For": "203.0.113.9, 10.0.0.5"}) == 200 and status(P, **{"X-Forwarded-For": "198.51.100.9, 10.0.0.5, 10.0.0.6"}) == 403)
    check("a garbage hop fails closed", status(P, **{"X-Forwarded-For": "203.0.113.9, banana"}) == 403)
    check("a trusted proxy that forwards nothing is judged as itself", status(P) == 403)
    check("ports and brackets in the header are understood", status(P, **{"X-Forwarded-For": "203.0.113.9:4711"}) == 200 and str(ipa.parse_ip("[2001:db8::7]:443")) == "2001:db8::7" and str(ipa.parse_ip("::ffff:10.1.2.3")) == "10.1.2.3" and ipa.parse_ip("10.1.2") is None)
    check("an untrusted address inside the proxy range is not special; a proxy outside it is untrusted", status(from_("192.0.2.1"), **{"X-Forwarded-For": OFFICE}) == 403)
    put(admin, "enforce", ["203.0.113.0/24"], [])
    os.environ["TRUSTED_PROXIES"] = "10.0.0.0/8"; ipa._invalidate()
    check("TRUSTED_PROXIES in the environment is added to the setting", status(P, **{"X-Forwarded-For": "203.0.113.9"}) == 200 and admin.get("/api/ip-allowlist").json()["config"]["env_trusted_proxies"] == ["10.0.0.0/8"])
    del os.environ["TRUSTED_PROXIES"]; ipa._invalidate()
    check("...and removing it restores the strict behaviour", status(P, **{"X-Forwarded-For": "203.0.113.9"}) == 403)
    r = admin.put("/api/ip-allowlist", json={"mode": "enforce", "rules": [{"cidr": "203.0.113.0/24"}], "trusted_proxies": [{"cidr": "0.0.0.0/0"}]})
    check("a proxy range of /0 (trust everybody) is refused", r.status_code == 400)

    print("who am I, checks, sandbox")
    me = admin.get("/api/ip-allowlist", headers={"X-Forwarded-For": "1.2.3.4"}).json()["me"]
    check("the administrator sees their address and a warning when forwarding headers are not trusted", me["ip"] == OFFICE and me["allowed"] and "trusted proxy" in me["warning"], me)
    check("an address can be checked against the rules", admin.post("/api/ip-allowlist/check", json={"ip": "203.0.113.77"}).json() == {"ip": "203.0.113.77", "allowed": True, "rule": "203.0.113.0/24"} and admin.post("/api/ip-allowlist/check", json={"ip": "8.8.8.8"}).json()["allowed"] is False and admin.post("/api/ip-allowlist/check", json={"ip": "x"}).status_code == 400)
    real = sandbox_client.is_sandbox_peer; sandbox_client.is_sandbox_peer = lambda ip: ip == "172.30.0.5"
    sb = from_("172.30.0.5")
    check("the notebook sandbox's own /api/sandbox/* calls are not cut off by the allowlist", ipa.check_request("172.30.0.5", {}, "/api/sandbox/sql", sandbox_peer=True)[0] == "allow")
    check("...but its calls to anything else are blocked (by both gates)", status(sb) == 403)
    sandbox_client.is_sandbox_peer = real

    print("monitor mode, break-glass, audit")
    r = put(admin, "monitor", ["203.0.113.0/24"])
    check("monitor mode lets everything through", r.status_code == 200 and status(from_("198.51.100.9")) == 200)
    put(admin, "monitor", [])  # monitoring with no rule is allowed (nothing is blocked)
    act = admin.get("/api/ip-allowlist").json()["activity"]
    check("...and records what would have been refused", any(e["ip"] == "198.51.100.9" and e["verdict"] == "would_block" for e in act["recent"]) and any(t["ip"] == "198.51.100.9" and t["would_block"] >= 1 for t in act["top"]), act["top"][:2])
    put(admin, "enforce", ["203.0.113.0/24"]); os.environ["IP_ALLOWLIST_OVERRIDE"] = "off"
    cfg = admin.get("/api/ip-allowlist").json()["config"]
    check("IP_ALLOWLIST_OVERRIDE=off suspends enforcement without touching the setting", status(from_("198.51.100.9")) == 200 and cfg["overridden"] is True and cfg["stored_mode"] == "enforce")
    del os.environ["IP_ALLOWLIST_OVERRIDE"]
    check("...and removing it enforces again", status(from_("198.51.100.9")) == 403)
    real_check = ipa.check_request
    def boom(*a, **k): raise RuntimeError("bug")
    ipa.check_request = boom
    check("an internal error in the check does not lock everybody out (it is logged)", status(from_("198.51.100.9")) == 200)
    ipa.check_request = real_check
    check("turning it off frees everyone at once", put(admin, "off", []).status_code == 200 and status(from_("198.51.100.9")) == 200)
    acts = [r[0] for r in sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "governance.db")).execute("SELECT action FROM governance_audit")]
    check("changes are audited", "IP_ALLOWLIST_UPDATE" in acts)
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
