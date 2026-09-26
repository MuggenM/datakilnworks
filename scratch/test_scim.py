#!/usr/bin/env python3
"""SCIM 2.0 provisioning (web/scim.py) against a throwaway WAREHOUSE_DIR, driving /scim/v2 with Entra- and Okta-style requests: auth and tokens,
discovery, users (create/filter/page/PUT/PATCH/DELETE/restore), groups, role mapping and caps, isolation from local accounts, login integration,
limits, audit."""
import base64, json, os, shutil, sqlite3, sys, tempfile
TMP = tempfile.mkdtemp(prefix="scim_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES"): os.environ.pop(v, None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from web import app as app_module, auth, groups, scim, oidc_auth
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
def db(sql, *p):
    with sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "auth.db")) as c:
        c.row_factory = sqlite3.Row; cur = c.execute(sql, p); c.commit(); return cur.fetchall()
PATCH = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
def u_payload(name, **kw):
    return {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"], "userName": name, "externalId": kw.pop("ext", "ext-" + name), "name": {"givenName": "Given", "familyName": "Family"},
            "displayName": kw.pop("display", "Display " + name), "emails": [{"value": name if "@" in name else name + "@corp.example", "type": "work", "primary": True}], "active": True, **kw}
try:
    with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")
    auth.create_user("bob", "bobpassword1", "Bob", role="user")          # a LOCAL account SCIM must never see
    admin = TestClient(app_module.app); assert admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"}).status_code == 200
    alice_web = TestClient(app_module.app); alice_web.post("/api/auth/login", json={"username": "bob", "password": "bobpassword1"})

    print("tokens and switching it on")
    check("only administrators manage SCIM", alice_web.get("/api/scim").status_code == 403 and alice_web.post("/api/scim/tokens", json={"name": "x"}).status_code == 403)
    r = admin.post("/api/scim/tokens", json={"name": "Entra ID production"}); tok = r.json()["token"]
    check("a token is created and shown once", r.status_code == 200 and tok.startswith("dkw_scim_") and len(tok) > 40)
    check("the token is stored only as a hash", tok not in json.dumps([dict(x) for x in db("SELECT * FROM scim_tokens")]) and tok not in json.dumps(admin.get("/api/scim").json()))
    S = TestClient(app_module.app); S.headers.update({"Authorization": f"Bearer {tok}", "Content-Type": "application/scim+json"})
    r = S.get("/scim/v2/Users"); check("while SCIM is off every call is refused (403), even with a good token", r.status_code == 403 and "turned off" in r.json()["detail"], r.text)
    r = TestClient(app_module.app).get("/scim/v2/Users"); check("no token: 401 with a WWW-Authenticate challenge", r.status_code == 401 and "Bearer" in r.headers.get("www-authenticate", ""))
    check("a wrong token: 401", TestClient(app_module.app).get("/scim/v2/Users", headers={"Authorization": "Bearer dkw_scim_nope"}).status_code == 401 and TestClient(app_module.app).get("/scim/v2/Users", headers={"Authorization": "Bearer x"}).status_code == 401)
    check("a session cookie is not accepted on the SCIM API", admin.get("/scim/v2/Users").status_code == 401)
    r = admin.put("/api/scim/config", json={"enabled": True, "login_source": "oidc", "default_role": "user", "max_role": "power_user"}); check("SCIM is switched on", r.status_code == 200 and r.json()["enabled"] is True, r.text)
    check("bad configuration is refused", admin.put("/api/scim/config", json={"default_role": "admin", "max_role": "user"}).status_code == 400 and admin.put("/api/scim/config", json={"login_source": "ldap"}).status_code == 400 and admin.put("/api/scim/config", json={"group_roles": {"x": "root"}}).status_code == 400)

    print("discovery")
    spc = S.get("/scim/v2/ServiceProviderConfig"); check("ServiceProviderConfig announces patch and filter", spc.status_code == 200 and spc.json()["patch"]["supported"] and spc.json()["filter"]["supported"] and spc.headers["content-type"].startswith("application/scim+json"), spc.text[:200])
    check("ResourceTypes and Schemas describe User and Group", {x["name"] for x in S.get("/scim/v2/ResourceTypes").json()["Resources"]} == {"User", "Group"} and {x["name"] for x in S.get("/scim/v2/Schemas").json()["Resources"]} == {"User", "Group"})

    print("users: create, isolation, filters")
    r = S.post("/scim/v2/Users", content=json.dumps(u_payload("Alice@Corp.Example")))
    a = r.json(); check("create returns 201, a Location and the SCIM resource", r.status_code == 201 and r.headers["location"].endswith("/scim/v2/Users/" + a["id"]) and a["userName"] == "alice@corp.example" and a["active"] is True and a["externalId"] == "ext-Alice@Corp.Example" and a["emails"][0]["primary"] is True and a["name"]["givenName"] == "Given" and a["meta"]["resourceType"] == "User", r.text[:300])
    check("the account is external, has no usable password and is marked SCIM-managed", db("SELECT auth_source, scim_managed FROM users WHERE username = 'alice@corp.example'")[0]["auth_source"] == "oidc" and db("SELECT scim_managed FROM users WHERE username = 'alice@corp.example'")[0][0] == 1 and TestClient(app_module.app).post("/api/auth/login", json={"username": "alice@corp.example", "password": "anything"}).status_code == 401)
    check("a duplicate userName is a 409 uniqueness error", (lambda x: x.status_code == 409 and x.json()["scimType"] == "uniqueness")(S.post("/scim/v2/Users", content=json.dumps(u_payload("alice@corp.example")))))
    check("a duplicate externalId is a 409", S.post("/scim/v2/Users", content=json.dumps(u_payload("other@corp.example", ext=a["externalId"]))).status_code == 409)
    r = S.post("/scim/v2/Users", content=json.dumps(u_payload("bob"))); check("a local account cannot be taken over (409)", r.status_code == 409 and "not managed by SCIM" in r.json()["detail"], r.text)
    check("...nor seen: filter finds nothing, GET by id is 404", S.get('/scim/v2/Users?filter=userName eq "bob"').json()["totalResults"] == 0 and S.get("/scim/v2/Users/" + db("SELECT id FROM users WHERE username='bob'")[0][0]).status_code == 404)
    aid = db("SELECT id FROM users WHERE username='admin'")[0][0]
    check("the bootstrap admin is invisible and untouchable", S.get("/scim/v2/Users/" + aid).status_code == 404 and S.patch("/scim/v2/Users/" + aid, content=json.dumps({"schemas": [PATCH], "Operations": [{"op": "replace", "path": "active", "value": False}]})).status_code == 404 and S.delete("/scim/v2/Users/" + aid).status_code == 404 and db("SELECT is_active FROM users WHERE username='admin'")[0][0] == 1)
    check("bad userNames are refused", all(S.post("/scim/v2/Users", content=json.dumps(u_payload(n))).status_code == 400 for n in ("", "has space", "Ünicode", "-lead", "a" * 200)) and S.post("/scim/v2/Users", content=json.dumps({"schemas": [], "displayName": "x"})).status_code == 400)
    for i in range(5): S.post("/scim/v2/Users", content=json.dumps(u_payload(f"user{i}@corp.example", display=f"User {i}")))
    F = lambda q: S.get("/scim/v2/Users", params={"filter": q}).json()
    check("userName eq (case-insensitive, as IdPs send it)", F('userName eq "ALICE@corp.example"')["totalResults"] == 1)
    check("externalId eq / emails.value eq / displayName co / sw", F('externalId eq "ext-user3@corp.example"')["totalResults"] == 1 and F('emails.value eq "user2@corp.example"')["totalResults"] == 1 and F('displayName co "ser 4"')["totalResults"] == 1 and F('userName sw "user"')["totalResults"] == 5)
    check("and / or / not / pr / parentheses", F('userName sw "user" and displayName ew "1"')["totalResults"] == 1 and F('userName eq "user0@corp.example" or userName eq "user1@corp.example"')["totalResults"] == 2 and F('userName sw "user" and not (displayName co "3")')["totalResults"] == 4 and F("externalId pr")["totalResults"] == 6 and F('active eq true')["totalResults"] == 6)
    bad = S.get("/scim/v2/Users", params={"filter": "userName equals x"}); check("an invalid filter is 400 invalidFilter", bad.status_code == 400 and bad.json()["scimType"] == "invalidFilter", bad.text)
    p1 = S.get("/scim/v2/Users?startIndex=1&count=2").json(); p2 = S.get("/scim/v2/Users?startIndex=3&count=2").json()
    check("pagination: startIndex/count/totalResults", p1["totalResults"] == 6 and len(p1["Resources"]) == 2 and p2["startIndex"] == 3 and p1["Resources"][0]["id"] != p2["Resources"][0]["id"] and len(S.get("/scim/v2/Users?count=0").json()["Resources"]) == 0 and S.get("/scim/v2/Users?startIndex=0").json()["startIndex"] == 1)
    check("count is capped at 200 and non-numbers are 400", S.get("/scim/v2/Users?count=100000").json()["itemsPerPage"] == 6 and S.get("/scim/v2/Users?count=abc").status_code == 400)

    print("users: PATCH / PUT / DELETE")
    P = lambda ops, uid=a["id"]: S.patch("/scim/v2/Users/" + uid, content=json.dumps({"schemas": [PATCH], "Operations": ops}))
    r = P([{"op": "Replace", "path": "active", "value": "False"}]); check("Entra deactivation: op 'Replace', active as the string 'False'", r.status_code == 200 and r.json()["active"] is False and db("SELECT is_active FROM users WHERE username='alice@corp.example'")[0][0] == 0, r.text)
    try: oidc_auth._provision({}, {"preferred_username": "alice@corp.example", "name": "X"}); ok = False
    except oidc_auth.OidcError as e: ok = "deactivated" in str(e)
    check("a deactivated SCIM user cannot sign in through OIDC", ok)
    r = P([{"op": "replace", "path": "active", "value": True}]); check("reactivation", r.json()["active"] is True)
    r = P([{"op": "replace", "path": "displayName", "value": "Alice Wonder"}, {"op": "replace", "path": "name.givenName", "value": "Alice"}, {"op": "add", "path": "emails[type eq \"work\"].value", "value": "alice.new@corp.example"}])
    j = r.json(); check("attribute paths: displayName, name.givenName, emails[type eq \"work\"].value", j["displayName"] == "Alice Wonder" and j["name"]["givenName"] == "Alice" and j["emails"][0]["value"] == "alice.new@corp.example", r.text)
    r = P([{"op": "replace", "value": {"active": False, "displayName": "Okta Style", "externalId": "okta-1"}}]); j = r.json()
    check("Okta style: no path, a value object", j["active"] is False and j["displayName"] == "Okta Style" and j["externalId"] == "okta-1", r.text)
    P([{"op": "replace", "path": "active", "value": True}])
    check("an attribute this studio does not keep is ignored, not an error", P([{"op": "replace", "path": "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:department", "value": "R&D"}, {"op": "add", "path": "title", "value": "CEO"}]).status_code == 200)
    check("a rename is refused (mutability)", (lambda x: x.status_code == 400 and x.json()["scimType"] == "mutability")(P([{"op": "replace", "path": "userName", "value": "renamed@corp.example"}])))
    check("an unknown operation is 400", P([{"op": "move", "path": "x"}]).status_code == 400 and S.patch("/scim/v2/Users/" + a["id"], content=json.dumps({"schemas": [PATCH]})).status_code == 400)
    r = P([{"op": "remove", "path": "emails"}]); check("remove clears an attribute", "emails" not in r.json())
    r = S.put("/scim/v2/Users/" + a["id"], content=json.dumps({"schemas": [], "userName": "alice@corp.example", "displayName": "Replaced", "active": True})); j = r.json()
    check("PUT replaces: attributes not sent are cleared", r.status_code == 200 and j["displayName"] == "Replaced" and "emails" not in j and "externalId" not in j and j["name"].get("givenName") is None, r.text)
    check("PUT with another userName is refused", S.put("/scim/v2/Users/" + a["id"], content=json.dumps({"userName": "new@corp.example"})).status_code == 400)
    check("GET by id returns the same resource", S.get("/scim/v2/Users/" + a["id"]).json()["displayName"] == "Replaced")
    r = S.delete("/scim/v2/Users/" + a["id"]); check("DELETE is 204 and the user is gone for SCIM", r.status_code == 204 and S.get("/scim/v2/Users/" + a["id"]).status_code == 404 and F('userName eq "alice@corp.example"')["totalResults"] == 0)
    check("...soft-deleted like an administrator's delete", db("SELECT deleted_at FROM users WHERE username='alice@corp.example'")[0][0] is not None)
    r = S.post("/scim/v2/Users", content=json.dumps(u_payload("alice@corp.example"))); check("provisioning the same userName again restores the account (same id)", r.status_code == 201 and r.json()["id"] == a["id"] and r.json()["active"] is True, r.text)
    check("an inactive user is created inactive when sent active=false", S.post("/scim/v2/Users", content=json.dumps(u_payload("dormant@corp.example", active=False))).json()["active"] is False)

    print("login integration")
    db("UPDATE users SET display_name = 'Scim Name' WHERE username = 'user0@corp.example'")
    u = oidc_auth._provision({}, {"preferred_username": "user0@corp.example", "name": "Name From The Token"})
    check("an OIDC sign-in works for a SCIM user but does not change their name or role (SCIM is authoritative)", u["username"] == "user0@corp.example" and u["display_name"] == "Scim Name" and u["role"] == "user", u)
    admin.put("/api/scim/config", json={"login_source": "saml"})
    r = S.post("/scim/v2/Users", content=json.dumps(u_payload("samluser@corp.example"))); check("login_source saml creates SAML accounts", db("SELECT auth_source FROM users WHERE username='samluser@corp.example'")[0][0] == "saml")
    admin.put("/api/scim/config", json={"login_source": "oidc"})

    print("roles")
    def role(name): return db("SELECT role FROM users WHERE username = ?", name)[0][0]
    check("a new user gets the default role", role("user1@corp.example") == "user")
    uid = lambda name: db("SELECT id FROM users WHERE username = ?", name)[0][0]
    S.patch("/scim/v2/Users/" + uid("user1@corp.example"), content=json.dumps({"schemas": [PATCH], "Operations": [{"op": "add", "path": "roles", "value": [{"value": "power_user"}]}]}))
    check("the roles attribute is ignored unless enabled", role("user1@corp.example") == "user")
    r = admin.put("/api/scim/config", json={"use_roles_attribute": True}); check("enabling it applies to existing users at once", r.json()["roles_changed"] >= 1 and role("user1@corp.example") == "power_user", r.text)
    S.patch("/scim/v2/Users/" + uid("user1@corp.example"), content=json.dumps({"schemas": [PATCH], "Operations": [{"op": "replace", "path": "roles", "value": [{"value": "admin"}]}]}))
    check("nothing exceeds max_role: an 'admin' role from the IdP becomes power_user", role("user1@corp.example") == "power_user")
    admin.put("/api/scim/config", json={"max_role": "admin"}); check("...unless an administrator explicitly allows admins", role("user1@corp.example") == "admin")
    admin.put("/api/scim/config", json={"max_role": "power_user"}); check("lowering the cap demotes at once", role("user1@corp.example") == "power_user")
    admin.put("/api/scim/config", json={"use_roles_attribute": False, "group_roles": {"Data Admins": "power_user"}})

    print("groups")
    G = lambda gid=None: "/scim/v2/Groups" + (("/" + gid) if gid else "")
    m0, m1, m2 = uid("user0@corp.example"), uid("user1@corp.example"), uid("user2@corp.example")
    r = S.post(G(), content=json.dumps({"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"], "displayName": "Data Admins", "externalId": "g-1", "members": [{"value": m0}, {"value": m1}]}))
    g = r.json(); check("a group is created with members (201, Location)", r.status_code == 201 and g["displayName"] == "Data Admins" and {x["value"] for x in g["members"]} == {m0, m1} and r.headers["location"].endswith(g["id"]), r.text)
    check("it is a platform group with source 'scim' and members owned by SCIM", (lambda row: row["source"] == "scim" and row["external_label"] == "Data Admins")(db("SELECT source, external_label FROM user_groups WHERE id = ?", g["id"])[0]) and {r_["origin"] for r_ in db("SELECT origin FROM user_group_members WHERE group_id = ?", g["id"])} == {"sync"})
    check("the group appears in the platform's group list", any(x["id"] == g["id"] and x["source"] == "scim" and x["member_count"] == 2 for x in groups.list_groups()))
    check("group-to-role mapping raises members' roles (capped by max_role)", role("user0@corp.example") == "power_user" and role("user1@corp.example") == "power_user" and role("user2@corp.example") == "user")
    check("the user resource lists the SCIM groups", [x["display"] for x in S.get("/scim/v2/Users/" + m0).json()["groups"]] == ["Data Admins"])
    check("duplicate displayName is 409; unknown member is 400", S.post(G(), content=json.dumps({"displayName": "data admins"})).status_code == 409 and S.post(G(), content=json.dumps({"displayName": "New", "members": [{"value": "nope"}]})).status_code == 400 and S.post(G(), content=json.dumps({"displayName": "New", "members": [{"value": db("SELECT id FROM users WHERE username='bob'")[0][0]}]})).status_code == 400)
    check("filter displayName eq and externalId eq", S.get(G(), params={"filter": 'displayName eq "Data Admins"'}).json()["totalResults"] == 1 and S.get(G(), params={"filter": 'externalId eq "g-1"'}).json()["totalResults"] == 1 and S.get(G(), params={"filter": 'displayName eq "zzz"'}).json()["totalResults"] == 0)
    PG = lambda ops, gid=g["id"]: S.patch(G(gid), content=json.dumps({"schemas": [PATCH], "Operations": ops}))
    r = PG([{"op": "Add", "path": "members", "value": [{"value": m2}]}]); check("Entra: Add members", {x["value"] for x in r.json()["members"]} == {m0, m1, m2} and role("user2@corp.example") == "power_user", r.text)
    r = PG([{"op": "Remove", "path": f'members[value eq "{m2}"]'}]); check("Entra: Remove members[value eq \"id\"]", {x["value"] for x in r.json()["members"]} == {m0, m1} and role("user2@corp.example") == "user", r.text)
    check("removing a non-member is harmless", PG([{"op": "remove", "path": f'members[value eq "{m2}"]'}]).status_code == 200)
    r = PG([{"op": "remove", "path": "members", "value": [{"value": m1}]}]); check("Okta: remove with a value list", {x["value"] for x in r.json()["members"]} == {m0})
    r = PG([{"op": "replace", "path": "members", "value": [{"value": m1}, {"value": m2}]}]); check("replace members", {x["value"] for x in r.json()["members"]} == {m1, m2})
    r = PG([{"op": "replace", "value": {"displayName": "Data Platform Admins", "members": [{"value": m0}]}}]); j = r.json()
    check("no path: displayName and members in one value object", j["displayName"] == "Data Platform Admins" and {x["value"] for x in j["members"]} == {m0} and role("user0@corp.example") == "user", r.text)
    check("the renamed group keeps working in filters; roles follow the mapping name", S.get(G(), params={"filter": 'displayName eq "Data Platform Admins"'}).json()["totalResults"] == 1 and role("user0@corp.example") == "user")
    admin.put("/api/scim/config", json={"group_roles": {"Data Platform Admins": "power_user"}}); check("changing the mapping applies at once", role("user0@corp.example") == "power_user")
    manual = groups.add_members(g["id"], [uid("bob")], "admin")
    r = S.put(G(g["id"]), content=json.dumps({"displayName": "Data Platform Admins", "members": []})); check("PUT replaces only SCIM-owned members; a member added by hand stays and stays invisible", r.json().get("members") == [] and any(x["username"] == "bob" for x in groups.list_members(g["id"])) and role("user0@corp.example") == "user")
    try: groups.set_mapping(g["id"], "ldap", "cn=x", "admin"); ok = False
    except groups.GroupError as e: ok = "SCIM" in str(e)
    check("a SCIM group cannot be re-mapped by hand", ok)
    S.put(G(g["id"]), content=json.dumps({"displayName": "Data Platform Admins", "members": [{"value": m0}]}))
    try: groups.remove_member(g["id"], m0, "admin"); ok = False
    except groups.GroupError: ok = True
    check("a SCIM-owned member cannot be removed by hand (it would be undone)", ok)
    r = S.post(G(), content=json.dumps({"displayName": "Sales (EMEA) / Nordics!"})); j = r.json()
    check("odd displayNames are kept for the IdP and sanitised for the platform", r.status_code == 201 and j["displayName"] == "Sales (EMEA) / Nordics!" and db("SELECT name FROM user_groups WHERE id = ?", j["id"])[0][0] == "Sales _EMEA_ _ Nordics_")
    groups.create_group("Analysts", "", "admin"); r = S.post(G(), content=json.dumps({"displayName": "Analysts"}))
    check("a name clash with a hand-made group gets a suffix; the hand-made group is untouched", r.status_code == 201 and db("SELECT name FROM user_groups WHERE id = ?", r.json()["id"])[0][0] == "Analysts (SCIM)" and groups.list_groups() and any(x["name"] == "Analysts" and x["source"] == "local" for x in groups.list_groups()))
    local_gid = next(x["id"] for x in groups.list_groups() if x["name"] == "Analysts")
    check("hand-made groups are invisible to SCIM", S.get(G(local_gid)).status_code == 404 and S.delete(G(local_gid)).status_code == 404 and S.get(G(), params={"filter": 'displayName eq "Analysts"'}).json()["totalResults"] == 1)
    r = S.delete(G(g["id"])); check("DELETE group is 204 and removes its grants and roles", r.status_code == 204 and S.get(G(g["id"])).status_code == 404 and role("user0@corp.example") == "user" and not db("SELECT 1 FROM user_groups WHERE id = ?", g["id"]))

    print("limits, tokens, gates, audit")
    r = S.post("/scim/v2/Users", content=b"{not json"); check("invalid JSON is 400 invalidSyntax", r.status_code == 400 and r.json()["scimType"] == "invalidSyntax")
    check("a body that is not an object is 400", S.post("/scim/v2/Users", content=b"[1,2]").status_code == 400)
    check("a body over 1 MB is refused (413)", S.post("/scim/v2/Users", content=b'{"userName":"' + b"a" * 1_100_000 + b'"}').status_code == 413)
    check("errors use the SCIM error schema", S.get("/scim/v2/Users/nope").json()["schemas"] == ["urn:ietf:params:scim:api:messages:2.0:Error"] and S.get("/scim/v2/Users/nope").json()["status"] == "404")
    tid = next(t["id"] for t in admin.get("/api/scim").json()["tokens"] if t["name"] == "Entra ID production")
    check("the token records when it was used", admin.get("/api/scim").json()["tokens"][0]["last_used_at"] is not None)
    r2 = admin.post("/api/scim/tokens", json={"name": "Okta", "expires_days": 30}).json(); T2 = TestClient(app_module.app); T2.headers.update({"Authorization": "Bearer " + r2["token"]})
    check("a second token works and has an expiry", T2.get("/scim/v2/Users").status_code == 200 and r2["expires_at"])
    db("UPDATE scim_tokens SET expires_at = '2020-01-01 00:00:00' WHERE id = ?", r2["id"]); check("an expired token is refused", T2.get("/scim/v2/Users").status_code == 401)
    admin.delete(f"/api/scim/tokens/{tid}"); check("a revoked token is refused at once", S.get("/scim/v2/Users").status_code == 401 and admin.delete("/api/scim/tokens/nope").status_code == 404)
    check("bad token requests are refused", admin.post("/api/scim/tokens", json={"name": ""}).status_code == 400 and admin.post("/api/scim/tokens", json={"name": "x", "expires_days": 99999}).status_code == 400)
    r3 = admin.post("/api/scim/tokens", json={"name": "Entra ID production 2"}).json(); S.headers["Authorization"] = "Bearer " + r3["token"]
    st = admin.get("/api/scim").json()
    check("the admin view shows counts, the URL and recent requests (with the token's name)", st["status"]["users"] >= 6 and st["status"]["groups"] >= 2 and st["base_url"].endswith("/scim/v2") and any(e["token"] and e["status"] for e in st["status"]["events"]) and all(set(t) == {"id", "name", "prefix", "created_by", "created_at", "expires_at", "last_used_at", "revoked_at"} for t in st["tokens"]))
    acts = {x[0] for x in sqlite3.connect(os.path.join(os.environ["WAREHOUSE_DIR"], ".metadata", "governance.db")).execute("SELECT action FROM governance_audit")}
    check("SCIM changes are audited", {"SCIM_USER_CREATE", "SCIM_USER_DEACTIVATE", "SCIM_USER_DELETE", "SCIM_GROUP_CREATE", "SCIM_GROUP_UPDATE", "SCIM_GROUP_DELETE", "SCIM_TOKEN_CREATE", "SCIM_TOKEN_REVOKE", "SCIM_CONFIG_UPDATE", "SCIM_ROLE_CHANGE"} <= acts, sorted(a for a in acts if a.startswith("SCIM")))
    check("SCIM-managed users are flagged in the user list", any(u["username"] == "user0@corp.example" and u["scim_managed"] for u in auth.list_users()) and not any(u["scim_managed"] for u in auth.list_users() if u["username"] in ("admin", "bob")))
    admin.put("/api/scim/config", json={"enabled": False}); check("switching it off stops everything again", S.get("/scim/v2/Users").status_code == 403)
    admin.put("/api/scim/config", json={"enabled": True})
    office = TestClient(app_module.app, client=("203.0.113.5", 1)); office.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
    r = office.put("/api/ip-allowlist", json={"mode": "enforce", "rules": [{"cidr": "203.0.113.0/24"}], "trusted_proxies": []}); assert r.status_code == 200, r.text
    blocked = TestClient(app_module.app, client=("198.51.100.9", 1)); blocked.headers["Authorization"] = S.headers["Authorization"]
    check("the IP allowlist applies to SCIM too", blocked.get("/scim/v2/Users").status_code == 403 and "Access denied" in blocked.get("/scim/v2/Users").text)
    check("...and an allowed address gets through", (lambda c_: (c_.headers.update({"Authorization": S.headers["Authorization"]}), c_.get("/scim/v2/Users").status_code)[1])(TestClient(app_module.app, client=("203.0.113.9", 1))) == 200)
    office.put("/api/ip-allowlist", json={"mode": "off", "rules": [], "trusted_proxies": []})
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
