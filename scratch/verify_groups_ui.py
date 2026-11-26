#!/usr/bin/env python3
"""Groups end to end (Playwright + API, /usr/bin/python3) against a THROWAWAY studio bootstrapped with admin/adminpassword123, plus users
alice and bob (role user, password userpass1234) and an LDAP-sourced user carol:  GIT_UI_URL=http://localhost:8117 python scratch/verify_groups_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch()
    def session(user, pw):
        c = b.new_context(viewport={"width": 1500, "height": 1100}); assert c.request.post(f"{BASE}/api/auth/login", data={"username": user, "password": pw}).ok; return c
    actx, alice, bob = session("admin", "adminpassword123"), session("alice", "userpass1234"), session("bob", "userpass1234")
    page = actx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)

    # ---- IAM > Groups
    page.evaluate("async () => { const d = Alpine.$data(document.body); await d.openIamModal(); }")
    page.locator("[data-testid=iam-tab-groups]:visible").click()
    panel = page.locator("[data-testid=groups-panel]:visible"); panel.wait_for(timeout=8000)
    panel.locator("[data-testid=group-new-name]").fill("Analysts"); panel.locator("[data-testid=group-create]").click()
    panel.locator("[data-testid=group-name]").wait_for(timeout=8000)
    check("a group is created and selected", panel.locator("[data-testid=group-name]").input_value() == "Analysts")
    panel.locator("[data-testid=group-new-name]").fill("analysts"); panel.locator("[data-testid=group-create]").click(); time.sleep(0.8)
    check("a duplicate name is refused with a message", "already exists" in panel.locator("[data-testid=group-error]").inner_text())
    sel = panel.locator("[data-testid=group-add-select]")
    for uname in ("alice", "carol"):
        val = sel.locator(f"option:has-text('{uname} —')").first.get_attribute("value")
        sel.select_option(val); panel.locator("[data-testid=group-add]").click(); time.sleep(0.8)
    rows = panel.locator("[data-testid=group-member]")
    check("local and LDAP users are members", rows.count() == 2 and "LDAP" in panel.inner_text().upper(), panel.inner_text()[:300])
    page.locator("[data-testid=iam-tab-users]:visible").click(); time.sleep(0.5)
    check("the Users tab shows each user's groups", page.locator("tr:has-text('alice') span:text-is('Analysts')").count() >= 1)
    gid = next(g["id"] for g in actx.request.get(f"{BASE}/api/groups").json()["groups"] if g["name"] == "Analysts")

    # ---- catalog access through the group (real SQL gate)
    check("catalog created", actx.request.post(f"{BASE}/api/catalogs", data={"name": "Sales", "id": "sales"}).ok)
    def sql(ctx):
        r = ctx.request.post(f"{BASE}/api/sql/execute", data={"query": "select * from sales.dbo.nothing", "warehouse_id": "wh_starter", "catalog": "warehouse"})
        return "denied" if "Access denied" in r.text() else "passed"      # the gate answers 200 with success:false + 'Access denied'
    check("before: alice may not query the catalog", sql(alice) == "denied")
    page.evaluate("async () => { Alpine.$data(document.body).showIamModal = false; Alpine.$data(document.body).openCatalogPermissionsModal('sales'); }")
    time.sleep(1.2)
    page.locator("[data-testid=cat-grant-kind]:visible").select_option("group")
    page.locator("[data-testid=cat-grant-group]:visible").select_option(gid)
    page.locator("[data-testid=cat-grant-submit]:visible").click(); time.sleep(1.2)
    print("   perms in UI:", page.evaluate("() => JSON.stringify(Alpine.$data(document.body).catalogPermsList.map(p => p.username))"), "modal:", page.evaluate("() => Alpine.$data(document.body).showCatalogPermissionsModal"))
    check("the ACL lists the group by name with a group badge", "Analysts" in page.locator("[data-testid=cat-perm-name]:visible").first.inner_text() and "group" in page.locator("[data-testid=cat-perm-name]:visible").first.inner_text())
    check("after: alice (member) passes the catalog gate", sql(alice) == "passed")
    check("bob (not a member) is still refused", sql(bob) == "denied")

    # ---- saved query shared through the dialog
    q = actx.request.post(f"{BASE}/api/queries", data={"name": "Secret report", "query_text": "select 1"}).json()
    names = lambda ctx: [x["name"] for x in ctx.request.get(f"{BASE}/api/queries").json()["queries"]]
    check("before: the query is private to its owner", "Secret report" not in names(alice))
    check("before: a direct GET by id is 404 for alice", alice.request.get(f"{BASE}/api/queries/{q['id']}").status == 404)
    page.evaluate("async () => { Alpine.$data(document.body).showCatalogPermissionsModal = false; await Alpine.$data(document.body).openShare('saved_query', '%s', 'Secret report'); }" % q["id"])
    dlg = page.locator("[data-testid=share-dialog]:visible"); dlg.wait_for(timeout=8000)
    dlg.locator("[data-testid=share-search]").fill("Analy"); time.sleep(0.9)
    dlg.locator("[data-testid=share-result]:has-text('Analysts')").click(); time.sleep(1)
    check("the dialog lists the group with VIEW", dlg.locator("[data-testid=share-grant]:has-text('Analysts')").count() == 1)
    check("after: alice sees it, read-only", "Secret report" in names(alice) and alice.request.get(f"{BASE}/api/queries/{q['id']}").json()["my_access"] == "view")
    check("...she cannot change it", alice.request.put(f"{BASE}/api/queries/{q['id']}", data={"name": "hacked"}).status == 403)
    check("...nor delete it", alice.request.delete(f"{BASE}/api/queries/{q['id']}").status == 403)
    check("bob still cannot see it", "Secret report" not in names(bob))
    r = actx.request.post(f"{BASE}/api/grants/saved_query/{q['id']}", data={"principal": f"group:{gid}", "permission": "EDIT"}); check("upgrade to EDIT", r.ok, r.text())
    check("EDIT lets a member change it", alice.request.put(f"{BASE}/api/queries/{q['id']}", data={"description": "edited by alice"}).ok)
    check("...but still not delete it", alice.request.delete(f"{BASE}/api/queries/{q['id']}").status == 403)
    check("a non-owner cannot see or change who it is shared with", alice.request.get(f"{BASE}/api/grants/saved_query/{q['id']}").status == 403 and alice.request.post(f"{BASE}/api/grants/saved_query/{q['id']}", data={"principal": f"user:bob", "permission": "VIEW"}).status == 403)
    dup = alice.request.post(f"{BASE}/api/queries/{q['id']}/duplicate"); check("a duplicate is owned by the person who made it", dup.ok and dup.json()["owner"] == "alice", dup.text()[:150])

    # ---- pipeline run/manage through the group
    src = "/tmp/gsrc"; pipe = actx.request.post(f"{BASE}/api/autoloader/pipelines", data={"name": "grp pipe", "source_volume_path": "/Volumes/warehouse/raw/grp", "target_table": "grp_t"})
    check("pipeline created by the admin", pipe.ok, pipe.text()[:200]); pid = pipe.json()["id"]
    pl = lambda ctx: next(x for x in ctx.request.get(f"{BASE}/api/autoloader/pipelines").json()["pipelines"] if x["id"] == pid)
    check("before: alice has no access to it", pl(alice)["my_access"] is None)
    check("before: alice cannot run it", alice.request.post(f"{BASE}/api/autoloader/pipelines/{pid}/run-now").status == 403)
    r = actx.request.post(f"{BASE}/api/grants/pipeline/{pid}", data={"principal": f"group:{gid}", "permission": "RUN"}); check("share RUN with the group", r.ok, r.text())
    check("after: alice may run it", pl(alice)["my_access"] == "run" and alice.request.post(f"{BASE}/api/autoloader/pipelines/{pid}/run-now").status != 403)
    check("...but not change or delete it", alice.request.put(f"{BASE}/api/autoloader/pipelines/{pid}", data={"name": "x"}).status == 403 and alice.request.delete(f"{BASE}/api/autoloader/pipelines/{pid}").status == 403)
    check("bob still cannot run it", bob.request.post(f"{BASE}/api/autoloader/pipelines/{pid}/run-now").status == 403)
    actx.request.post(f"{BASE}/api/grants/pipeline/{pid}", data={"principal": f"group:{gid}", "permission": "MANAGE"})
    check("MANAGE lets a member update it", alice.request.put(f"{BASE}/api/autoloader/pipelines/{pid}", data={"description": "by alice"}).ok)

    # ---- dashboard
    d = actx.request.post(f"{BASE}/api/dashboards", data={"name": "Team board"}).json(); did = d["id"]
    check("before: alice cannot see the dashboard's permissions", alice.request.get(f"{BASE}/api/dashboards/{did}/permissions/users").status == 403)
    r = actx.request.post(f"{BASE}/api/dashboards/{did}/permissions/grant", data={"group": gid, "level": "viewer"}); check("dashboard grant to a group (JSON body accepted)", r.ok, r.text())
    check("after: a member can view it", alice.request.get(f"{BASE}/api/dashboards/{did}/permissions/users").status == 200)
    check("a non-member still cannot", bob.request.get(f"{BASE}/api/dashboards/{did}/permissions/users").status == 403)
    r = actx.request.post(f"{BASE}/api/dashboards/{did}/permissions/grant", data={"user": "bob", "level": "viewer"}); r2 = actx.request.post(f"{BASE}/api/dashboards/{did}/permissions/grant", data={"user": "alice", "level": "editor"})
    perms = actx.request.get(f"{BASE}/api/dashboards/{did}/permissions").json()["permissions"]["permissions"]
    check("granting several principals keeps each entry", {(x.get("user") or x.get("group")): x["level"] for x in perms} == {gid: "viewer", "bob": "viewer", "alice": "editor"}, perms)

    # ---- non-admins cannot manage groups
    check("a normal user cannot list, create or delete groups", alice.request.get(f"{BASE}/api/groups").status == 403 and alice.request.post(f"{BASE}/api/groups", data={"name": "x"}).status == 403 and alice.request.delete(f"{BASE}/api/groups/{gid}").status == 403)
    check("the principal picker works for everyone but needs 2 letters for users", len(alice.request.get(f"{BASE}/api/principals?q=a").json()["users"]) == 0 and len(alice.request.get(f"{BASE}/api/principals?q=al").json()["users"]) >= 1)

    # ---- deleting the group removes the access
    page.evaluate("async () => { Alpine.$data(document.body).shareDlg.open = false; await Alpine.$data(document.body).openIamModal(); Alpine.$data(document.body).iamTab = 'groups'; await Alpine.$data(document.body).loadGroups(); }")
    panel = page.locator("[data-testid=groups-panel]:visible"); panel.locator("[data-testid=group-row]:has-text('Analysts')").click(); time.sleep(0.8)
    page.once("dialog", lambda dlg_: dlg_.accept()); panel.locator("[data-testid=group-delete]").click(); time.sleep(1.5)
    check("the group is gone", not [g for g in actx.request.get(f"{BASE}/api/groups").json()["groups"] if g["name"] == "Analysts"])
    check("catalog access is gone", sql(alice) == "denied")
    check("shared query is private again", "Secret report" not in names(alice))
    check("pipeline run is refused again", alice.request.post(f"{BASE}/api/autoloader/pipelines/{pid}/run-now").status == 403)
    perms = actx.request.get(f"{BASE}/api/dashboards/{did}/permissions").json()["permissions"]["permissions"]
    check("the dashboard entry is gone", not any(x.get("group") for x in perms))
    page.screenshot(path="/tmp/groups_ui.png")
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
