#!/usr/bin/env python3
"""
Dashboard apps (web/dashboard_apps.py): dashboards with pages, typed parameters, text blocks, samples, cloning, export.
Throwaway WAREHOUSE_DIR, no compose services needed.
Run in the studio image: docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_dashboard_apps.py

Covers: the placeholder scanner (strings / comments / casts / slices are not placeholders), typed binding of hostile values,
spec validation refusals, the permission rules that the dashboard endpoints did not have (list / read / query / export / edit),
that every shipped sample validates AND runs against a real warehouse, parameter filtering and cross-filter values, a masked
viewer seeing masked values in an app while the author does not, the catalog ACL applying to a widget, and viewer export.
"""
import io
import json
import os
import sys
import tempfile

TMP_ROOT = tempfile.mkdtemp(prefix="dashapps_test_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP_ROOT, "warehouse")
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
os.makedirs(os.environ["WAREHOUSE_DIR"])
for var in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"):
    os.environ.pop(var, None)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from fastapi.testclient import TestClient

from web import auth
from web import dashboard_apps as da

FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:300]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


def raises(exc, fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except exc:
        return True


print("=== placeholder scanner ===")
names = lambda sql: [n for _, _, n in da.scan_placeholders(sql)]
check("plain :name and {{name}}", names("select :a, {{ b }} from t where c = :c") == ["a", "b", "c"])
check("a :word inside a string literal is not a placeholder", names("select ':a' as x, 'it''s :b' from t where c = :c") == ["c"])
check("a :word inside a quoted identifier is not a placeholder", names('select "col:a" from t where c = :c') == ["c"])
check("comments are skipped", names("select 1 -- :a\n /* :b */ from t where c = :c") == ["c"])
check("a :: cast is not a placeholder", names("select x::int, y::varchar from t where c = :c") == ["c"])
check("a slice and a struct key are not placeholders", names("select l[1:n], {'a': b} from t") == [])
check("time literal is not a placeholder", names("select '12:30'::time, ts::time from t") == [])

print("=== typed binding ===")
P = lambda **k: dict({"id": "p", "type": "text", "label": "P"}, **k)
render = lambda p, v, o=None: da._render(p, v, o)[0]
check("text: quote doubled", render(P(), "x'; DROP TABLE t; --") == "'x''; DROP TABLE t; --'")
check("text: contains :other and {{x}} stays inert text", render(P(), "a :other {{x}}") == "'a :other {{x}}'")
check("text: too long refused", raises(da.ParamError, render, P(), "x" * 501))
check("number: int", render(P(type="number"), "42") == "42")
check("number: float", render(P(type="number"), "4.5") == "4.5")
check("number: injection refused", raises(da.ParamError, render, P(type="number"), "1; drop table t"))
check("number: nan / inf refused", raises(da.ParamError, render, P(type="number"), "nan") and raises(da.ParamError, render, P(type="number"), "inf"))
check("number: bool refused", raises(da.ParamError, render, P(type="number"), True))
check("date: valid", render(P(type="date"), "2026-02-03") == "DATE '2026-02-03'")
check("date: invalid refused", raises(da.ParamError, render, P(type="date"), "2026-13-01") and raises(da.ParamError, render, P(type="date"), "x' or 1=1"))
check("select: in options", render(P(type="select"), "North", ["North", "South"]) == "'North'")
check("select: not in options refused", raises(da.ParamError, render, P(type="select"), "East", ["North", "South"]))
check("select as identifier: quoted", render(P(type="select", as_identifier=True), "warehouse.hr.employees", ["warehouse.hr.employees"]) == '"warehouse"."hr"."employees"')
check("select as identifier: injection refused", raises(da.ParamError, render, P(type="select", as_identifier=True), 'a"; drop', ['a"; drop']))
bound = da.bind_sql("select ':region' as lit, :region as r, x::int from t where a = :a", {"region": "'N'", "a": "5"})
check("bind_sql replaces only real placeholders", bound == "select ':region' as lit, 'N' as r, x::int from t where a = 5", bound)
check("a bound value containing :word is not rewritten again",
      da.bind_sql("select :a, :b", {"a": "'x :b y'", "b": "7"}) == "select 'x :b y', 7")

print("=== spec validation ===")
ok_param = {"id": "region", "type": "select", "options": ["N", "S"]}
good_w = {"type": "table", "title": "T", "query": "select * from t where r = :region", "page": "p1"}
page1 = [{"id": "p1", "title": "Page 1"}]
check("a valid spec passes", da.validate_spec([ok_param], page1, [good_w])["widgets"][0]["page"] == "p1")
check("undeclared :param refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, query="select :nope")]))
check("DDL refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, query="drop table t")]))
check("INSERT refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, query="insert into t values (1)")]))
check("two statements refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, query="select 1; select 2")]))
check("unparsable SQL refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, query="selec from")]))
check("unknown page refused", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, page="zz")]))
check("bad parameter id refused", raises(da.SpecError, da.validate_spec, [dict(ok_param, id="Bad Id")], page1, []))
check("duplicate parameter id refused", raises(da.SpecError, da.validate_spec, [ok_param, ok_param], page1, []))
check("unknown parameter type refused", raises(da.SpecError, da.validate_spec, [dict(ok_param, type="script")], page1, []))
check("select without choices refused", raises(da.SpecError, da.validate_spec, [{"id": "x", "type": "select"}], page1, []))
check("bad date default refused", raises(da.SpecError, da.validate_spec, [{"id": "d", "type": "date", "default": "yesterday"}], page1, []))
check("select default not in options refused", raises(da.SpecError, da.validate_spec, [dict(ok_param, default="Z")], page1, []))
check("click parameter must be declared", raises(da.SpecError, da.validate_spec, [ok_param], page1, [dict(good_w, filter_dimension="nope")]))
check("unknown widget type refused", raises(da.SpecError, da.validate_spec, [], page1, [dict(good_w, type="iframe")]))
check("bad width refused", raises(da.SpecError, da.validate_spec, [], page1, [dict(good_w, width="w-full")]))
check("too many widgets refused", raises(da.SpecError, da.validate_spec, [], page1, [dict(good_w, id=f"w{i}", query="select 1") for i in range(da.MAX_WIDGETS + 1)]))
check("a text widget keeps markdown and has no query", da.validate_spec([], page1, [{"type": "text", "title": "n", "markdown": "**hi**"}])["widgets"][0]["query"] == "")
kept = da.validate_spec([], [], [dict(good_w, page=None, query="select 1", drill_down={"enabled": True}, result={"rows": [1]})])["widgets"][0]
check("fields of older widgets (drill_down) survive; a client-echoed result does not", kept.get("drill_down") and "result" not in kept)
check("every sample validates", all(da.validate_spec(s["parameters"], s["pages"], [dict(w, id=f"w{i}") for i, w in enumerate(s["widgets"])]) for s in da.SAMPLES))
check("there are 3 samples", len(da.SAMPLES) == 3)

print("=== HTTP: setup ===")
from deltalake import write_deltalake
import pandas as pd

write_deltalake(os.path.join(os.environ["WAREHOUSE_DIR"], "hr", "employees"),
                pd.DataFrame({"name": ["Ada", "Grace"], "email": ["ada@example.com", "grace@example.com"]}), mode="overwrite")
auth.create_user("carol", "carolpass123", "Carol", role="power_user")
auth.create_user("bob", "bobpass1234", "Bob", role="user")
auth.create_user("dave", "davepass123", "Dave", role="user")

from web import app as app_module  # noqa: E402
from web import warehouses
from web.governance import store as gov_store, tags as gov_tags, policies as gov_policies
gov_store.init_governance_db()
gov_tags.set_tag(catalog="warehouse", schema_name="hr", table_name="employees", column_name="email", tag_key="pii", tag_value="email", actor="admin")
gov_policies.create_policy({"name": "mask emails for dashboard apps test", "tag_key": "pii", "tag_value": "email",
                            "mask_type": "null", "except_roles": ["admin"]}, actor="admin")
warehouses.create_catalog("Finance", "finance", "", None, False, "admin")

client = TestClient(app_module.app)
_TOKENS = {}
def H(u):  # a real session token per user (X-User only works in the single-user mode GOVERNANCE_REQUIRE_AUTH=false)
    if u not in _TOKENS:
        _TOKENS[u] = auth.create_access_token(auth.get_user_by_username(u))
    return {"Authorization": "Bearer " + _TOKENS[u]}

print("=== samples catalogue and cloning ===")
r = client.get("/api/dashboard-apps/samples", headers=H("bob"))
check("samples are listed for any signed-in user", r.status_code == 200 and len(r.json()["samples"]) == 3, r.text)
cloned = {}
for s in da.SAMPLES:
    r = client.post(f"/api/dashboard-apps/samples/{s['id']}/clone", json={"name": "My " + s["title"]}, headers=H("carol"))
    check(f"clone {s['id']}", r.status_code == 200 and r.json()["name"] == "My " + s["title"] and r.json()["created_by"] == "carol", r.text)
    cloned[s["id"]] = r.json()
check("a clone gets fresh ids", len({w["id"] for c in cloned.values() for w in c["widgets"]}) == sum(len(c["widgets"]) for c in cloned.values()))
check("clone of an unknown sample is 404", client.post("/api/dashboard-apps/samples/nope/clone", json={}, headers=H("carol")).status_code == 404)

print("=== permissions (the dashboard endpoints had none) ===")
sales = cloned["sample_sales_explorer"]
sid = sales["id"]
cat = lambda u: [a["id"] for a in client.get("/api/dashboard-apps", headers=H(u)).json()["apps"]]
check("owner sees her app in the catalogue", sid in cat("carol"))
check("a stranger does not see it in the catalogue", sid not in cat("bob"))
check("a stranger does not see it in the dashboard list", sid not in [d["id"] for d in client.get("/api/dashboards", headers=H("bob")).json()["dashboards"]])
check("a stranger cannot read it (404)", client.get(f"/api/dashboards/{sid}", headers=H("bob")).status_code == 404)
check("a stranger cannot query it", client.post(f"/api/dashboards/{sid}/query", json={"parameters": {}}, headers=H("bob")).status_code == 404)
check("a stranger cannot read its filters", client.get(f"/api/dashboards/{sid}/filters", headers=H("bob")).status_code == 404)
wid = sales["widgets"][-1]["id"]
check("a stranger cannot export a widget", client.get(f"/api/dashboards/{sid}/widgets/{wid}/export?format=csv", headers=H("bob")).status_code == 404)
check("a stranger cannot delete a widget", client.delete(f"/api/dashboards/{sid}/widgets/{wid}", headers=H("bob")).status_code == 404)
check("a stranger cannot clone it", client.post(f"/api/dashboard-apps/{sid}/clone", json={}, headers=H("bob")).status_code == 404)
check("an admin sees it", sid in cat("admin"))
from web.dashboard_permissions import grant_permission
grant_permission(sid, user="bob", level="viewer", granted_by="carol")
check("a viewer grant shows it", sid in cat("bob") and client.get(f"/api/dashboards/{sid}", headers=H("bob")).status_code == 200)
check("a viewer cannot delete a widget (403)", client.delete(f"/api/dashboards/{sid}/widgets/{wid}", headers=H("bob")).status_code == 403)
check("a viewer cannot save the spec (403)", client.put(f"/api/dashboards/{sid}/spec", json={"pages": [{"id": "x", "title": "x"}]}, headers=H("bob")).status_code == 403)
check("a viewer cannot add a widget (403)", client.post(f"/api/dashboards/{sid}/widgets", json={"title": "x", "type": "kpi", "query": "select 1"}, headers=H("bob")).status_code == 403)
check("a viewer cannot edit a widget (403)", client.put(f"/api/dashboards/{sid}/widgets/{wid}", json={"title": "x"}, headers=H("bob")).status_code == 403)
check("a viewer can clone it into their own", client.post(f"/api/dashboard-apps/{sid}/clone", json={"name": "Bob copy"}, headers=H("bob")).status_code == 200)
check("dave (no grant) still cannot", client.get(f"/api/dashboards/{sid}", headers=H("dave")).status_code == 404)
# a legacy dashboard (no permission record): readable by a signed-in user, editable by an admin only
from web.dashboards import load_dashboards_store
legacy = load_dashboards_store()[0]["id"]
check("a legacy dashboard stays readable", client.get(f"/api/dashboards/{legacy}", headers=H("dave")).status_code == 200)
check("a legacy dashboard is not editable by a plain user", client.delete(f"/api/dashboards/{legacy}/widgets/none", headers=H("dave")).status_code == 403)

print("=== running the samples against a real warehouse ===")
ex = cloned["sample_customer_segments"]
r = client.get(f"/api/dashboards/{ex['id']}", headers=H("carol"))
d = r.json()
check("sample 2 loads with parameters and pages", r.status_code == 200 and len(d["parameters"]) == 2 and len(d["pages"]) == 2 and d["my_access"] == "owner", r.text[:300])
check("every widget of sample 2 ran", all(w["result"]["success"] for w in d["widgets"]), [w["result"].get("error") for w in d["widgets"] if not w["result"]["success"]])
total = next(w for w in d["widgets"] if w["title"] == "Customers")["result"]["rows"][0]["customers"]
check("1500 synthetic customers", total == 1500, total)
d2 = client.post(f"/api/dashboards/{ex['id']}/query", json={"parameters": {"segment": "SMB", "min_spend": "5000"}}, headers=H("carol")).json()
filt = next(w for w in d2["widgets"] if w["title"] == "Customers")["result"]["rows"][0]["customers"]
check("segment + minimum spend narrow the result", 0 < filt < total, filt)
top = next(w for w in d2["widgets"] if w["title"] == "Top 50 customers")["result"]
check("the table obeys the filters", top["row_count"] > 0 and all(r["segment"] == "SMB" and r["annual_spend"] >= 5000 for r in top["rows"]))
bad = client.post(f"/api/dashboards/{ex['id']}/query", json={"parameters": {"segment": "Nope", "min_spend": "0"}}, headers=H("carol")).json()
check("a segment outside the options is refused (every widget reports it)", all((not w["result"]["success"]) and "not one of the choices" in w["result"]["error"] for w in bad["widgets"] if w["type"] != "text"))
inj = client.post(f"/api/dashboards/{ex['id']}/query", json={"parameters": {"min_spend": "0 OR 1=1"}}, headers=H("carol")).json()
check("an injected number is refused", all(not w["result"]["success"] for w in inj["widgets"] if w["type"] != "text"))
check("a text widget returns an empty successful result", all(w["result"]["success"] for w in d["widgets"] if w["type"] == "text"))

sales_d = client.get(f"/api/dashboards/{sid}", headers=H("carol")).json()
check("sample 1 widgets all ran", all(w["result"]["success"] for w in sales_d["widgets"]), [w["result"].get("error") for w in sales_d["widgets"] if not w["result"]["success"]])
rev = lambda dd: next(w for w in dd["widgets"] if w["title"] == "Revenue")["result"]["rows"][0]["revenue"]
all_rev = rev(sales_d)
north = rev(client.post(f"/api/dashboards/{sid}/query", json={"parameters": {"region": "North", "from_date": "2026-01-01", "to_date": "2026-12-31"}}, headers=H("carol")).json())
check("filtering by region (what clicking a bar does) lowers revenue", 0 < north < all_rev, (north, all_rev))
none_d = rev(client.post(f"/api/dashboards/{sid}/query", json={"parameters": {"region": "ALL", "from_date": "", "to_date": ""}}, headers=H("carol")).json())
check("ALL and a cleared date mean no filter", none_d >= all_rev, (none_d, all_rev))
dates = rev(client.post(f"/api/dashboards/{sid}/query", json={"parameters": {"from_date": "2026-03-01", "to_date": "2026-03-31"}}, headers=H("carol")).json())
check("a date range narrows it", 0 < dates < all_rev, dates)

te = cloned["sample_table_explorer"]
ted = client.get(f"/api/dashboards/{te['id']}", headers=H("carol")).json()
opts = ted["parameter_options"]["tbl"]
check("the table picker lists real tables (and no ALL, it is required)", "warehouse.hr.employees" in opts and "ALL" not in opts, opts[:10])
rows = next(w for w in ted["widgets"] if w["title"] == "Rows")["result"]
check("the first table is selected by default and counted", rows["success"], rows.get("error"))
tp = client.post(f"/api/dashboards/{te['id']}/query", json={"parameters": {"tbl": "warehouse.hr.employees"}}, headers=H("carol")).json()
prev = next(w for w in tp["widgets"] if w["title"] == "First 100 rows")["result"]
check("a chosen table previews", prev["success"] and prev["row_count"] == 2, prev.get("error"))
cols = next(w for w in tp["widgets"] if w["title"] == "Columns" and w["type"] == "table")["result"]
check("the identifier's text form drives the columns list", [r["column_name"] for r in cols["rows"]] == ["name", "email"], cols)
tx = client.post(f"/api/dashboards/{te['id']}/query", json={"parameters": {"tbl": 'warehouse.hr.employees"; drop table x; --'}}, headers=H("carol")).json()
check("a table name outside the offered list is refused", all((not w["result"]["success"]) for w in tx["widgets"] if w["type"] != "text"))
tx2 = client.post(f"/api/dashboards/{te['id']}/query", json={"parameters": {"tbl": "finance.x.y"}}, headers=H("carol")).json()
check("a table that is not offered is refused", all((not w["result"]["success"]) for w in tx2["widgets"] if w["type"] != "text"))

print("=== governance applies to the viewer, not the author ===")
emp = client.post("/api/dashboard-apps", json={"name": "Employees app"}, headers=H("carol")).json()
eid = emp["id"]
r = client.post(f"/api/dashboards/{eid}/widgets", json={"title": "Staff", "type": "table", "query": "select name, email from warehouse.hr.employees order by name"}, headers=H("carol"))
check("adding a widget to an app works", r.status_code == 200 and r.json()["page"] == "page1", r.text[:300])
grant_permission(eid, user="bob", level="viewer", granted_by="carol")
get_emails = lambda u: [x["email"] for x in client.get(f"/api/dashboards/{eid}", headers=H(u)).json()["widgets"][0]["result"]["rows"]]
check("the admin sees real emails", get_emails("admin") == ["ada@example.com", "grace@example.com"], get_emails("admin"))
check("the masked viewer sees NULL emails in the same app", get_emails("bob") == [None, None], get_emails("bob"))
r = client.post(f"/api/dashboards/{eid}/widgets", json={"title": "Fin", "type": "table", "query": "select * from finance.x.y"}, headers=H("carol"))
fw = r.json()
check("a widget on a catalog the viewer cannot read reports a denial, not data",
      r.status_code == 200 and not fw["result"]["success"] and "finance" in (fw["result"]["error"] or ""), fw["result"])
fin_widget = next(w for w in client.get(f"/api/dashboards/{eid}", headers=H("bob")).json()["widgets"] if w["title"] == "Fin")
check("... for the viewer too", not fin_widget["result"]["success"] and "Access denied" in fin_widget["result"]["error"], fin_widget["result"])

print("=== export by a viewer ===")
swid = emp["id"] and client.get(f"/api/dashboards/{eid}", headers=H("bob")).json()["widgets"][0]["id"]
for fmt, ctype in (("csv", "text/csv"), ("json", "application/json"), ("parquet", "application/octet-stream"),
                   ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")):
    r = client.get(f"/api/dashboards/{eid}/widgets/{swid}/export?format={fmt}", headers=H("bob"))
    check(f"viewer exports {fmt}", r.status_code == 200 and r.headers["content-type"].startswith(ctype) and len(r.content) > 0, (r.status_code, r.text[:200]))
csv_text = client.get(f"/api/dashboards/{eid}/widgets/{swid}/export?format=csv", headers=H("bob")).text
check("the viewer's export carries the viewer's masking", "ada@example.com" not in csv_text and "Ada" in csv_text, csv_text)
csv_admin = client.get(f"/api/dashboards/{eid}/widgets/{swid}/export?format=csv", headers=H("admin")).text
check("the admin's export has real values", "ada@example.com" in csv_admin)
seg_t = next(w for w in d["widgets"] if w["title"] == "Top 50 customers")["id"]
r = client.get(f"/api/dashboards/{ex['id']}/widgets/{seg_t}/export?format=csv&params=" + json.dumps({"segment": "SMB"}), headers=H("carol"))
check("an export honours the page's parameter values", r.status_code == 200 and "Enterprise" not in r.text and "SMB" in r.text, r.text[:200])
check("an unsupported export format is refused", client.get(f"/api/dashboards/{eid}/widgets/{swid}/export?format=exe", headers=H("carol")).status_code == 400)

print("=== editing: spec, pages, widgets ===")
r = client.put(f"/api/dashboards/{eid}/spec", json={"parameters": [{"id": "who", "type": "text"}], "pages": [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}]}, headers=H("carol"))
check("the owner saves parameters and pages", r.status_code == 200 and len(r.json()["pages"]) == 2, r.text)
after = client.get(f"/api/dashboards/{eid}", headers=H("carol")).json()
check("widgets of a removed page move to the first page", all(w["page"] == "a" for w in after["widgets"]), [w.get("page") for w in after["widgets"]])
check("a spec that leaves a widget with an undeclared parameter is refused",
      client.put(f"/api/dashboards/{eid}/widgets/{swid}", json={"query": "select * from warehouse.hr.employees where name = :nobody"}, headers=H("carol")).status_code == 400)
r = client.put(f"/api/dashboards/{eid}/widgets/{swid}", json={"query": "select name from warehouse.hr.employees where name = :who", "page": "b", "title": "One"}, headers=H("carol"))
check("the owner edits a widget (moves it to page b, uses a parameter)", r.status_code == 200 and r.json()["page"] == "b" and r.json()["title"] == "One", r.text[:300])
r = client.post(f"/api/dashboards/{eid}/query", json={"parameters": {"who": "Ada"}}, headers=H("carol")).json()
one = next(w for w in r["widgets"] if w["id"] == swid)["result"]
check("a text parameter filters the edited widget", one["success"] and [x["name"] for x in one["rows"]] == ["Ada"], one)
r = client.put(f"/api/dashboards/{eid}/spec", json={"pages": [{"id": "a", "title": "A"}] * 2}, headers=H("carol"))
check("duplicate page ids refused", r.status_code == 400)
r = client.post(f"/api/dashboards/{eid}/widgets", json={"title": "Note", "type": "text", "query": "", "markdown": "<script>alert(1)</script> **b**", "page": "a"}, headers=H("carol"))
check("a text widget is stored as data (the page escapes it when rendering)", r.status_code == 200 and r.json()["markdown"].startswith("<script>") and r.json()["result"]["success"], r.text[:200])
check("widget on an unknown page refused", client.post(f"/api/dashboards/{eid}/widgets", json={"title": "x", "type": "kpi", "query": "select 1", "page": "zz"}, headers=H("carol")).status_code == 400)

print("=== legacy dashboards are unchanged ===")
lg = client.get(f"/api/dashboards/{legacy}", headers=H("carol")).json()
check("a dashboard without parameters still loads with the old filters", "filters" in lg and lg["parameters"] == [] and lg["pages"] == [])
check("... and is not in the Apps catalogue", legacy not in cat("admin"))
legacy_rows = client.post(f"/api/dashboards/{legacy}/query", json={"parameters": {"department": "ALL"}}, headers=H("carol")).status_code
check("... and still answers parameter queries", legacy_rows == 200)

print()
if FAIL:
    print(f"{len(FAIL)} FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL PASS")
