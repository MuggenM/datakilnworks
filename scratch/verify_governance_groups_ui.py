#!/usr/bin/env python3
"""Groups in masking / row-filter policies end to end (Playwright + API, /usr/bin/python3) against a THROWAWAY studio prepared by the caller:
warehouse table hr.people(id,name,email,region) with rows EMEA/EMEA/APAC, tag pii=email on the column, tag region_scoped on the table, row policy
'Region filter' (attribute mode, column region), users alice/bob/carol (role user, password userpass1234), groups 'PII readers' (alice, carol)
and 'EMEA team' (alice):  GIT_UI_URL=http://localhost:8117 python scratch/verify_governance_groups_ui.py"""
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
        c = b.new_context(viewport={"width": 1500, "height": 1300}); assert c.request.post(f"{BASE}/api/auth/login", data={"username": u, "password": pw}).ok; return c
    actx = session("admin", "adminpassword123"); users = {u: session(u, "userpass1234") for u in ("alice", "bob", "carol")}
    def rows(u):
        r = users[u].request.post(f"{BASE}/api/sql/execute", data={"query": "select id, email, region from hr.people order by id", "warehouse_id": "wh_starter", "catalog": "warehouse"}).json()
        if not r.get("success"): return "error: " + str(r.get("error"))[:100]
        return [tuple(x) for x in r["rows"]]     # columns: id, email, region
    groups = {g["name"]: g["id"] for g in actx.request.get(f"{BASE}/api/groups").json()["groups"]}
    page = actx.new_page(); errs = []; page.on("pageerror", lambda e: errs.append(str(e)))
    page.goto(BASE, wait_until="networkidle"); time.sleep(1)

    print("masking policy created through the UI, with no group exemption")
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.currentView = 'governance'; await d.loadGovernance?.(); d.govTab = 'policies'; d.openGovPolicyModal(null); }")
    time.sleep(1.2)
    page.evaluate("() => { const f = Alpine.$data(document.body).govPolicyForm; f.name = 'Mask PII'; f.tag_key = 'pii'; f.mask_type = 'redact'; }")
    check("the form lists the groups as exemptions", page.locator("[data-testid=mask-exempt-groups]:visible input[type=checkbox]").count() == 2)
    page.evaluate("async () => { await Alpine.$data(document.body).saveGovPolicy(); }"); time.sleep(1.2)
    r = rows("alice"); check("before: alice's emails are masked and (no region assigned) no rows show", r == [], r)

    print("region attribute given to a group through the UI")
    page.evaluate("async () => { const d = Alpine.$data(document.body); d.govTab = 'rowpolicies'; await d.fetchGovAttributes(); }"); time.sleep(0.8)
    page.locator("[data-testid=attr-type]:visible").select_option("group")
    page.locator("[data-testid=attr-group]:visible").select_option(groups["EMEA team"])
    page.evaluate("() => { const f = Alpine.$data(document.body).govAttrForm; f.attribute_key = 'region'; f.values_text = 'EMEA'; }")
    page.evaluate("async () => { await Alpine.$data(document.body).saveGovAttribute(); }"); time.sleep(1.2)
    a = actx.request.get(f"{BASE}/api/governance/attributes").json()["attributes"]
    check("the attribute is stored for the group", any(x["principal_type"] == "group" and x["principal_value"] == groups["EMEA team"] and x["values"] == ["EMEA"] for x in a), a)
    check("the attribute list names the group", "group:EMEA team" in page.locator("body").inner_text())
    ra = rows("alice")
    check("alice (member) now sees the EMEA rows, emails still masked", isinstance(ra, list) and [x[0] for x in ra] == [1, 2] and all("@" not in str(x[1]) for x in ra), ra)
    check("carol and bob (not in the EMEA team) see no rows", rows("carol") == [] and rows("bob") == [])

    print("group exemption from the masking policy through the UI")
    pid = next(x["id"] for x in actx.request.get(f"{BASE}/api/governance/masking-policies").json()["policies"] if x["name"] == "Mask PII")
    page.evaluate("async (pid) => { const d = Alpine.$data(document.body); await d.fetchGovPolicies(); d.openGovPolicyModal(d.govPolicies.find(x => x.id === pid)); }", pid); time.sleep(1)
    page.locator("[data-testid=mask-exempt-groups]:visible label:has-text('PII readers') input").check()
    page.evaluate("async () => { await Alpine.$data(document.body).saveGovPolicy(); }"); time.sleep(1.2)
    pol = actx.request.get(f"{BASE}/api/governance/masking-policies").json()["policies"]
    check("the policy stores the exempt group", next(x for x in pol if x["id"] == pid)["except_groups"] == [groups["PII readers"]])
    ra = rows("alice"); check("alice (member of PII readers) sees real e-mail addresses", isinstance(ra, list) and ra and all("@" in str(x[1]) for x in ra), ra)
    page.evaluate("() => { const d = Alpine.$data(document.body); d.govTab = 'policies'; }"); time.sleep(0.5)
    check("the policy list shows the exemption by group name", "group:PII readers" in page.locator("body").inner_text())
    check("bob is still masked (and, with no attribute, sees no rows)", rows("bob") == [])

    print("membership changes act at once")
    actx.request.delete(f"{BASE}/api/groups/{groups['PII readers']}/members/alice")
    ra = rows("alice"); check("removed from PII readers: alice is masked again on her next query", isinstance(ra, list) and ra and all("@" not in str(x[1]) for x in ra), ra)
    actx.request.post(f"{BASE}/api/groups/{groups['PII readers']}/members", data={"user_ids": ["alice"]})
    check("added back: unmasked again", "@" in str(rows("alice")[0][1]))

    print("deleting groups")
    actx.request.delete(f"{BASE}/api/groups/{groups['PII readers']}")
    pol = actx.request.get(f"{BASE}/api/governance/masking-policies").json()["policies"]
    check("the deleted group no longer exempts (policy stricter, not looser)", next(x for x in pol if x["id"] == pid)["except_groups"] == [])
    ra = rows("alice"); check("alice is masked again", isinstance(ra, list) and all("@" not in str(x[1]) for x in ra), ra)
    actx.request.delete(f"{BASE}/api/groups/{groups['EMEA team']}")
    check("and, with the EMEA group gone, sees no rows again", rows("alice") == [])
    check("no page errors", not errs, errs)
    b.close()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
