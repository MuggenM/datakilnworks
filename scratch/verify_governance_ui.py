#!/usr/bin/env python3
"""
UI verification for tag-based masking (Playwright, headless Chromium).
Point it at a THROWAWAY studio started with WAREHOUSE_DIR=<dir>/warehouse containing a Delta table hr/employees
(columns id, first_name, email, ssn, salary): it creates tags and policies.
    GOVERNANCE_UI_URL=http://localhost:8100 python3 scratch/verify_governance_ui.py
Tests:
1. No JS errors on load or during any interaction; Governance nav is hidden from plain users.
2. Governance view: define a tag, create a masking policy (live mask preview), suggestions scan + apply,
   preview-as-user, audit and coverage.
3. Catalog explorer: tag a column through the modal; masked users see lock badges and a masked-column counter.
4. SQL editor results render the masked-column banner and lock icons.
"""

import json
import os
import sys
import time
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("GOVERNANCE_UI_URL", "http://localhost:8100").rstrip("/")
OUT_DIR = os.getenv("GOVERNANCE_UI_OUT", "/tmp/governance_ui_shots")
os.makedirs(OUT_DIR, exist_ok=True)
FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def new_session(browser, username, password):
    ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
    page = ctx.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
    r = ctx.request.post(f"{BASE_URL}/api/auth/login", data={"username": username, "password": password})
    check(f"logged in as {username}", r.ok)
    page.goto(BASE_URL, wait_until="networkidle")
    time.sleep(1.5)
    return ctx, page, errors


def data(page, expr):
    return page.evaluate(f"() => {{ const d = Alpine.$data(document.body); return {expr}; }}")


def act(page, code):
    page.evaluate(f"async () => {{ const d = Alpine.$data(document.body); {code} }}")


def open_table(page):
    act(page, "d.currentView = 'catalog'; await d.selectTable('hr', 'employees', 'warehouse'); d.activeCatalogTab = 'schema';")
    page.locator("th:visible:has-text('Column Name')").first.wait_for(state="visible", timeout=8000)
    time.sleep(1)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        print("\n1. Admin: Governance view")
        ctx, page, errors = new_session(browser, "admin", "adminpassword123")
        check("no JS errors on initial load", not errors, errors[:3])
        nav = page.get_by_role("button", name="Governance")
        check("Governance is in the sidebar for an admin", nav.first.is_visible())
        nav.first.click()
        page.locator("h2:has-text('Tags & Masking Policies')").wait_for(state="visible")
        time.sleep(1)
        check("status cards and trust-boundary notice render", page.locator("text=Trust boundary").first.is_visible() and page.locator("text=Mask functions").first.is_visible())
        check("seeded tags are listed", page.locator("td.font-mono:has-text('pii')").first.is_visible() and page.locator("td.font-mono:has-text('sensitivity')").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g1_tags_dark.png")

        # define a tag through the form
        page.get_by_placeholder("tag key, e.g. pii").fill("owner_team")
        page.get_by_placeholder("allowed values (comma separated, optional)").fill("hr, finance")
        page.get_by_role("button", name="Create tag").click()
        page.locator("td.font-mono:has-text('owner_team')").wait_for(state="visible", timeout=5000)
        check("a new tag can be defined", True)
        page.get_by_role("button", name="Where used").first.click()
        time.sleep(0.5)
        check("'Where used' opens the usage panel", page.locator("text=Objects tagged").first.is_visible())

        # masking policy through the modal
        page.get_by_role("button", name="Masking Policies").click()
        page.get_by_role("button", name="New policy").click()
        modal = page.locator("h3:has-text('New masking policy')")
        modal.wait_for(state="visible")
        dlg = page.locator("div.fixed:has(h3:has-text('New masking policy'))")
        dlg.get_by_placeholder("Mask PII for analysts").fill("Mask PII for analysts")
        dlg.locator("select").nth(0).select_option("pii")
        dlg.locator("select").nth(1).select_option("partial") if dlg.locator("select").count() > 2 and False else None
        dlg.locator("select:has(option[value='partial'])").select_option("partial")
        time.sleep(0.8)
        check("the editor shows what the mask does per column type", dlg.locator("text=keeps up to the last 4 characters").first.is_visible())
        dlg.get_by_placeholder("try a value, e.g. ada@example.com").fill("123-45-6789")
        dlg.get_by_role("button", name="Test").click()
        dlg.locator("text=*******6789").first.wait_for(state="visible", timeout=5000)
        check("the live tester masks a sample value", True)
        page.screenshot(path=f"{OUT_DIR}/g2_policy_modal_dark.png")
        dlg.get_by_role("button", name="Save policy").click()
        page.locator("text=Mask PII for analysts").first.wait_for(state="visible", timeout=5000)
        check("the policy is saved and listed", page.locator("text=ENABLED").first.is_visible())
        # custom expression validation error shows inline
        page.get_by_role("button", name="New policy").click()
        dlg = page.locator("div.fixed:has(h3:has-text('New masking policy'))")
        dlg.locator("select:has(option[value='custom'])").select_option("custom")
        dlg.get_by_placeholder("left({col}, 2) || '***'").fill("count({col})")
        time.sleep(1.2)
        check("an unsafe custom expression is rejected inline", dlg.locator("text=not allowed").first.is_visible())
        dlg.get_by_role("button", name="Cancel").click()

        # suggestions
        page.get_by_role("button", name="Suggestions").click()
        page.get_by_role("button", name="Scan column names").click()
        page.locator("td.font-mono:has-text('employees.ssn')").first.wait_for(state="visible", timeout=8000)
        check("suggestions list untagged sensitive columns", page.locator("td.font-mono:has-text('employees.email')").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g3_suggestions_dark.png")
        page.get_by_role("button", name="Apply").first.click()
        time.sleep(1.5)
        applied = ctx.request.get(f"{BASE_URL}/api/governance/tags/pii/usage").json()["assignments"]
        check("accepted suggestions become tag assignments", any(a["column_name"] == "email" for a in applied), applied[:2])

        # preview as user
        page.get_by_role("button", name="Preview as user").click()
        page.locator("select:has(option:text-is('Choose a user…'))").select_option("analyst_bob")
        page.get_by_placeholder("SELECT * FROM warehouse.hr.employees").fill("SELECT * FROM warehouse.hr.employees")
        page.get_by_role("button", name="Preview", exact=True).click()
        page.locator("text=masked for analyst_bob").first.wait_for(state="visible", timeout=8000)
        check("preview-as shows the masked columns and rewritten SQL", page.locator("text=Rewritten SQL").first.is_visible() and page.locator("pre:has-text('gov_mask')").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g4_preview_dark.png")

        # audit & coverage
        page.get_by_role("button", name="Audit & Coverage").click()
        page.locator("td.font-mono:has-text('TAG_SET')").first.wait_for(state="visible", timeout=8000)
        check("audit lists tag and policy events", page.locator("td.font-mono:has-text('POLICY_CREATE')").first.is_visible())
        check("coverage cards render", page.locator("text=Assignments by level").first.is_visible())

        print("\n1b. Row filter policies and user attributes")
        page.get_by_role("button", name="Tags").first.click()
        page.get_by_placeholder("tag key, e.g. pii").fill("row_scope")
        page.get_by_role("button", name="Create tag").click()
        page.locator("td.font-mono:has-text('row_scope')").wait_for(state="visible", timeout=5000)
        # The table-level assignment itself doesn't need to reflect in the page's own JS state, so the API is enough.
        ctx.request.put(f"{BASE_URL}/api/governance/objects/warehouse/hr/regional_sales/tags",
                        data=json.dumps({"set": [{"tag_key": "row_scope", "tag_value": ""}], "unset": []}),
                        headers={"Content-Type": "application/json"})
        page.get_by_role("button", name="Row Filters").click()
        page.locator("p:has-text('A row filter restricts')").wait_for(state="visible", timeout=5000)
        page.get_by_role("button", name="New row filter").click()
        modal = page.locator("div.fixed:has(h3:has-text('New row filter policy'))")
        modal.wait_for(state="visible")
        modal.get_by_placeholder("Restrict rows by region").fill("Owner row filter UI test")
        modal.locator("select:has(option[value='row_scope'])").select_option("row_scope")
        modal.get_by_placeholder("region", exact=True).first.fill("owner")
        modal.get_by_role("button", name="Save row filter").click()
        page.locator("text=Owner row filter UI test").first.wait_for(state="visible", timeout=5000)

        def row_policy_card(name):
            # Tabs stay in the DOM when inactive (x-show only toggles visibility), so a bare text/ENABLED lookup can
            # resolve to a hidden element from a different tab; scope to this specific card instead.
            return page.locator(f"span:text-is('{name}')").locator("xpath=ancestor::div[contains(@class,'rounded-xl')][1]")

        card = row_policy_card("Owner row filter UI test")
        check("the row filter policy is saved and listed", card.locator("text=ENABLED").is_visible() and card.locator("text=owner").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g9_row_filters_dark.png")

        card.get_by_role("button", name="Disable").click()
        card.locator("text=DISABLED").wait_for(state="visible", timeout=5000)
        check("disabling a row filter updates its status", True)
        card.get_by_role("button", name="Enable").click()
        card.locator("text=ENABLED").wait_for(state="visible", timeout=5000)
        check("re-enabling it updates the status back", True)

        page.locator("input[x-model='govAttrForm.principal_value']").fill("analyst_bob")
        page.locator("input[x-model='govAttrForm.attribute_key']").fill("region")
        page.locator("input[x-model='govAttrForm.values_text']").fill("EMEA")
        page.get_by_role("button", name="Assign").click()
        page.locator("td.font-mono:has-text('user:analyst_bob')").first.wait_for(state="visible", timeout=5000)
        check("a user attribute assignment is saved and listed", page.locator("td.font-mono:has-text('EMEA')").first.is_visible())

        page.get_by_role("button", name="Preview as user").click()
        page.locator("select:has(option:text-is('Choose a user…'))").select_option("analyst_bob")
        page.get_by_placeholder("SELECT * FROM warehouse.hr.employees").fill("SELECT * FROM warehouse.hr.regional_sales")
        page.get_by_role("button", name="Preview", exact=True).click()
        page.locator("text=row-filtered for analyst_bob").first.wait_for(state="visible", timeout=8000)
        check("preview-as reports the row filter and shows it in the rewritten SQL",
              page.locator("text=row-filtered for analyst_bob").first.is_visible() and page.locator("pre:has-text('WHERE')").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g10_row_filter_preview_dark.png")

        page.get_by_role("button", name="Audit & Coverage").click()
        page.locator("td.font-mono:has-text('ROW_POLICY_CREATE')").first.wait_for(state="visible", timeout=8000)
        check("audit lists row policy events", page.locator("td.font-mono:has-text('ATTRIBUTE_SET')").first.is_visible())
        page.get_by_role("button", name="Row Filters").click()
        page.on("dialog", lambda d: d.accept())
        row_policy_card("Owner row filter UI test").locator("button:has(i.ph-trash)").click()
        # Scoped to the row-filter list itself: the (unrelated, point-in-time) coverage snapshot fetched earlier for
        # the audit tab can still name this policy in "matching nothing" until that tab is refreshed, which is stale
        # display state, not a sign the delete failed.
        for _ in range(20):
            if row_policy_card("Owner row filter UI test").count() == 0:
                break
            time.sleep(0.3)
        check("a row filter policy can be deleted from the UI", row_policy_card("Owner row filter UI test").count() == 0)
        check("no JS errors during the governance tour", not errors, errors[:3])

        print("\n2. Admin: tag a column in the catalog explorer")
        open_table(page)
        check("the schema tab has Tags and Access columns", page.locator("th:has-text('Tags')").first.is_visible() and page.locator("th:has-text('Access')").first.is_visible())
        check("suggested tags appear as chips on their columns", page.locator("span.font-mono:has-text('pii=email')").first.is_visible())
        check("an exempt admin sees every column as visible", page.locator("text=masked for you").count() == 0 or not page.locator("text=column(s) masked for you").first.is_visible())
        row = page.locator("tr:has(td:text-is('first_name'))")
        row.locator("button[title='Tag this column']").click()
        page.locator("h3:has-text('Tags on')").wait_for(state="visible")
        modal = page.locator("div.fixed:has(h3:has-text('Tags on'))")
        modal.locator("select").nth(0).select_option("pii")
        modal.locator("select:visible").nth(1).wait_for(state="visible")       # the value list appears once the tag is chosen
        modal.locator("select:visible").nth(1).select_option("email")
        modal.get_by_role("button", name="Add tag").click()
        time.sleep(1.2)
        check("the modal lists the new direct tag", modal.locator("div.font-mono:has-text('pii')").first.is_visible())
        check("...and it was stored", ctx.request.get(f"{BASE_URL}/api/governance/objects/warehouse/hr/employees/tags").json()["columns"][1]["direct"] == {"pii": "email"})
        modal.get_by_role("button", name="Close").click()
        time.sleep(0.6)
        check("the column now shows the chip", row.locator("span.font-mono:has-text('pii=email')").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g5_catalog_admin_dark.png")
        check("no JS errors while tagging", not errors, errors[:3])
        ctx.close()

        print("\n3. Masked user")
        ctx, page, errors = new_session(browser, "analyst_bob", "userpassword123")
        check("no JS errors on load", not errors, errors[:3])
        check("Governance is hidden from a plain user", not page.get_by_role("button", name="Governance").first.is_visible())
        open_table(page)
        check("masked columns are flagged for the user", page.locator("span:has-text('column(s) masked for you')").first.is_visible())
        check("lock badges appear in the Access column", page.locator("tbody span:has(i.ph-lock-key)").count() >= 2)
        check("tagging controls are hidden from a plain user", page.locator("button[title='Tag this column']").count() == 0 or not page.locator("button[title='Tag this column']").first.is_visible())
        page.screenshot(path=f"{OUT_DIR}/g6_catalog_user_dark.png")

        print("\n4. SQL editor result rendering")
        r = ctx.request.post(f"{BASE_URL}/api/sql/execute", data={"query": "SELECT id, email, ssn FROM warehouse.hr.employees ORDER BY id", "catalog": "warehouse"})
        result = r.json()
        # A Ray-executed result comes back as [[...]] rows with plain column names; normalise to what the grid renders
        cols = [c if isinstance(c, dict) else {"name": c, "type": "VARCHAR"} for c in result["columns"]]
        rows = [r if isinstance(r, dict) else {cols[i]["name"]: v for i, v in enumerate(r)} for r in result["rows"]]
        result = {**result, "columns": cols, "rows": rows}
        check("the API returns masked values and masked_columns", result.get("success") and rows[0]["email"] != "ada@example.com" and result.get("masked_columns"), result)
        act(page, f"d.currentView = 'sql'; d.activeSqlTab = 'table'; d.queryResult = {json.dumps(result)};")
        time.sleep(0.8)
        check("the results banner names only the masked columns that are in the result",
              page.locator("span:has-text('masked by governance policy: email, ssn')").first.is_visible(), page.locator("text=masked by governance policy").first.inner_text())
        check("masked column headers carry a lock icon", page.locator("th:has(i.ph-lock-key)").count() >= 2)
        page.screenshot(path=f"{OUT_DIR}/g7_sql_results_dark.png")
        blocked = ctx.request.post(f"{BASE_URL}/api/sql/execute", data={"query": "SELECT * FROM query('select 1')", "catalog": "warehouse"}).json()
        act(page, f"d.queryResult = {json.dumps(blocked)};")
        time.sleep(0.6)
        check("a blocked statement shows the governance notice", page.locator("text=Blocked by governance").first.is_visible())
        act(page, "d.currentView = 'catalog';")
        page.evaluate("() => document.documentElement.classList.remove('dark')")
        time.sleep(0.5)
        page.screenshot(path=f"{OUT_DIR}/g8_light_toggle.png")
        check("no JS errors for the masked user", not errors, errors[:3])
        ctx.close()
        browser.close()

    print(f"\nScreenshots: {OUT_DIR}")
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        sys.exit(1)
    print("All Governance UI checks passed.")


if __name__ == "__main__":
    main()
