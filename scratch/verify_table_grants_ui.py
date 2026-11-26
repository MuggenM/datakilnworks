#!/usr/bin/env python3
"""Table-level grants end to end (Playwright + API, /usr/bin/python3) against a THROWAWAY studio with users alice and bob (role user,
password userpass1234), catalog 'sales' holding Delta tables dbo.orders (3 rows) and dbo.customers, and group 'Analysts' (bob):
    GIT_UI_URL=http://localhost:8117 python scratch/verify_table_grants_ui.py"""
import os, sys, time
from playwright.sync_api import sync_playwright
BASE = os.getenv("GIT_UI_URL", "http://localhost:8117").rstrip("/")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
with sync_playwright() as p:
    b = p.chromium.launch()
    def session(u, pw):
        c = b.new_context(viewport={"width": 1500, "height": 1200}); assert c.request.post(f"{BASE}/api/auth/login", data={"username": u, "password": pw}).ok; return c
    actx, alice, bob = session("admin", "adminpassword123"), session("alice", "userpass1234"), session("bob", "userpass1234")
    def sql(ctx, q):
        r = ctx.request.post(f"{BASE}/api/sql/execute", data={"query": q, "warehouse_id": "wh_starter", "catalog": "warehouse"}); t = r.json()
        return ("denied" if "Access denied" in (t.get("error") or "") else ("ok:%s" % len(t.get("rows") or t.get("data") or []) if t.get("success") else "error:" + str(t.get("error"))[:120]))
    tprev = lambda ctx, t: ctx.request.get(f"{BASE}/api/table/dbo/{t}/preview?catalog=sales").status
    cats = lambda ctx: {c["id"]: c for c in ctx.request.get(f"{BASE}/api/catalogs").json()["catalogs"]}
    tables = lambda c: sorted(t["name"] for s in c["schemas"] for t in s["tables"])

    print("before any grant")
    check("alice cannot query the table", sql(alice, "select * from sales.dbo.orders") == "denied")
    check("...nor preview it", tprev(alice, "orders") == 403)
    check("the catalog is not listed for her", "sales" not in cats(alice))
    check("the admin can (sanity)", sql(actx, "select * from sales.dbo.orders").startswith("ok"), sql(actx, "select * from sales.dbo.orders"))

    print("share one table with alice through the UI")
    page = actx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)
    page.evaluate("""async () => { const d = Alpine.$data(document.body); d.currentView = 'catalog'; await d.fetchCatalogs?.();
        await d.selectTable('dbo', 'orders', 'sales'); d.activeCatalogTab = 'permissions'; await d.fetchCatalogPermissions('sales'); }""")
    time.sleep(1.5)
    panel = page.locator("[data-testid=table-grants-panel]:visible"); panel.wait_for(timeout=10000)
    panel.locator("[data-testid=share-table]").click()
    dlg = page.locator("[data-testid=share-dialog]:visible"); dlg.wait_for(timeout=8000)
    check("the dialog offers Select and Modify for a table", set(dlg.locator("[data-testid=share-perm] option").all_inner_texts()) == {"SELECT", "MODIFY"})
    dlg.locator("[data-testid=share-search]").fill("alic"); time.sleep(1)
    dlg.locator("[data-testid=share-result]:has-text('alice')").click(); time.sleep(1)
    check("alice is listed", dlg.locator("[data-testid=share-grant]:has-text('alice')").count() == 1)
    page.keyboard.press("Escape"); time.sleep(0.5)
    check("the catalog's grant list shows it", panel.locator("[data-testid=table-grant-row]:has-text('sales.dbo.orders')").count() == 1)

    print("what alice can and cannot do now")
    check("she can query the table (real rows)", sql(alice, "select * from sales.dbo.orders") == "ok:3", sql(alice, "select * from sales.dbo.orders"))
    check("...including with quoted/uppercase names", sql(alice, 'select count(*) from "SALES".DBO."ORDERS"').startswith("ok"))
    check("she cannot query the neighbour table", sql(alice, "select * from sales.dbo.customers") == "denied")
    check("...not via quoted identifiers", sql(alice, 'select * from "sales"."dbo"."customers"') == "denied")
    check("...not via a comment inside the name", sql(alice, "select * from sales/**/.dbo.customers") == "denied")
    check("...not by joining it with the granted one", sql(alice, "select * from sales.dbo.orders o join sales.dbo.customers c on 1=1") == "denied")
    check("...not by writing", sql(alice, "insert into sales.dbo.orders select * from sales.dbo.orders") == "denied")
    check("preview: granted table yes, neighbour no", tprev(alice, "orders") == 200 and tprev(alice, "customers") == 403, (tprev(alice, "orders"), tprev(alice, "customers")))
    ca = cats(alice)
    check("the explorer shows the catalog with only that table, read-only", "sales" in ca and tables(ca["sales"]) == ["orders"] and ca["sales"].get("partial_access") and not ca["sales"]["user_can_write"], ca.get("sales"))
    check("bob (no grant) still sees nothing", sql(bob, "select * from sales.dbo.orders") == "denied" and "sales" not in cats(bob))
    check("alice cannot grant access to herself or others", alice.request.post(f"{BASE}/api/grants/table/sales.dbo.customers", data={"principal": "user:alice", "permission": "SELECT"}).status == 403)
    check("...nor list who has access", alice.request.get(f"{BASE}/api/catalogs/sales/table-grants").status == 403)

    print("a schema grant to a group")
    gid = actx.request.post(f"{BASE}/api/groups", data={"name": "Analysts"}).json()["id"]
    bid = next(u["id"] for u in actx.request.get(f"{BASE}/api/users").json()["users"] if u["username"] == "bob")
    actx.request.post(f"{BASE}/api/groups/{gid}/members", data={"user_ids": [bid]})
    check("share the schema with the group", actx.request.post(f"{BASE}/api/grants/schema/sales.dbo", data={"principal": f"group:{gid}", "permission": "SELECT"}).ok)
    check("bob (member) reads both tables", sql(bob, "select * from sales.dbo.orders") == "ok:3" and sql(bob, "select * from sales.dbo.customers").startswith("ok"))
    check("alice's own grant is unchanged (still only orders)", sql(alice, "select * from sales.dbo.customers") == "denied")
    page.evaluate("async () => { await Alpine.$data(document.body).loadTableGrants(); }"); time.sleep(0.8)
    check("the list shows table and schema grants", panel.locator("[data-testid=table-grant-row]").count() == 2)

    print("revoking and dropping")
    panel.locator("[data-testid=table-grant-row]:has-text('sales.dbo.orders') [data-testid=table-grant-revoke]").click(); time.sleep(1.2)
    check("revoking alice's grant takes effect at once", sql(alice, "select * from sales.dbo.orders") == "denied")
    check("bob's schema grant is unaffected", sql(bob, "select * from sales.dbo.orders") == "ok:3")
    actx.request.post(f"{BASE}/api/grants/table/sales.dbo.orders", data={"principal": "user:alice", "permission": "SELECT"})
    r = actx.request.delete(f"{BASE}/api/table/dbo/orders?catalog=sales"); check("dropping the table works", r.ok, r.text()[:150])
    grants = actx.request.get(f"{BASE}/api/catalogs/sales/table-grants").json()["grants"]
    check("a dropped table takes its grants with it (a new table of that name starts clean)", not any(g["resource_id"] == "sales.dbo.orders" for g in grants) and any(g["resource_id"] == "sales.dbo" for g in grants), grants)
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
