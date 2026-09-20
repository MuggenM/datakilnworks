import os
import time
import asyncio
from playwright.async_api import async_playwright

ARTIFACT_DIR = "/home/martin/.gemini/antigravity-cli/brain/e598d0e4-1391-4974-9343-8271c1d6a271"

async def run_verification():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1600, "height": 1000})
        page = await context.new_page()

        print("Navigating to Data Kiln Works Studio...")
        await page.goto("http://localhost:8891")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1)

        # Authenticate and switch to catalog view directly
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.currentUser = { username: 'admin', role: 'admin', display_name: 'Administrator' };
                root.isAuthenticated = true;
                root.isLoggedIn = true;
                root.showLoginModal = false;
                root.currentView = 'catalog';
                root.fetchCatalogs();
                root.expandedCatalogs = { ...root.expandedCatalogs, 'warehouse': true };
                root.expandedSchemas = { ...root.expandedSchemas, 'warehouse.dbo': true };
            }
        """)
        await asyncio.sleep(2)

        # 1. Verify model nodes in tree
        print("Checking tree for registered models...")
        model_btn = page.locator("button:has-text('employee_turnover_predictor')").first
        await model_btn.wait_for(state="visible", timeout=10000)
        assert await model_btn.is_visible(), "employee_turnover_predictor model not visible in tree!"
        print("Confirmed employee_turnover_predictor model node visible in tree.")

        # Check @champion badge in tree
        champ_pill = page.locator("button:has-text('employee_turnover_predictor') span:has-text('@champion')").first
        assert await champ_pill.is_visible(), "@champion pill not visible on tree node!"
        print("Confirmed @champion pill rendered in tree node.")

        # 2. Click model to open Model Inspection Details Pane
        print("Clicking model to open inspection details...")
        await model_btn.click()
        await asyncio.sleep(1.5)

        # Verify header details
        model_header = page.locator("h2:has-text('employee_turnover_predictor')")
        assert await model_header.is_visible(), "Model details header not visible!"
        print("Model Inspection Details Pane loaded.")

        # Take screenshot in Dark Mode (Versions & Aliases tab)
        dark_screenshot_path = os.path.join(ARTIFACT_DIR, "unity_catalog_model_governance_dark.png")
        await page.screenshot(path=dark_screenshot_path, full_page=False)
        print(f"Saved {dark_screenshot_path}")

        # 3. Test Signature & Schema tab
        sig_tab = page.locator("button:has-text('Model Signature & Schema')")
        await sig_tab.click()
        await asyncio.sleep(1)
        assert await page.locator("text=Input Features Schema").is_visible(), "Signature schema not visible!"
        print("Model Signature & Schema verified.")

        # 4. Test SQL Inference Preview tab
        inference_tab = page.locator("button:has-text('SQL Inference Preview')")
        await inference_tab.click()
        await asyncio.sleep(1)
        assert await page.locator("text=Batch Scoring via 3-Level Governance (@champion)").is_visible(), "Inference preview not visible!"
        print("SQL Inference Preview tab verified.")

        inference_dark_path = os.path.join(ARTIFACT_DIR, "unity_catalog_sql_inference_preview_dark.png")
        await page.screenshot(path=inference_dark_path, full_page=False)
        print(f"Saved {inference_dark_path}")

        # 5. Test Assign Alias Modal
        print("Testing Assign Alias Modal...")
        versions_tab = page.locator("button:has-text('Versions & Aliases')")
        await versions_tab.click()
        await asyncio.sleep(0.5)

        assign_alias_btn = page.locator("button:has-text('Assign Alias')").first
        await assign_alias_btn.click()
        await asyncio.sleep(1)

        modal_title = page.locator("h3:has-text('Assign Model Alias')")
        assert await modal_title.is_visible(), "Assign Alias Modal did not open!"
        print("Assign Alias Modal opened successfully.")

        # Type custom alias 'staging_candidate' and submit
        custom_input = page.locator("input[placeholder*='e.g. shadow, ab_test_v2']")
        await custom_input.fill("staging_candidate")
        save_btn = page.locator("button:has-text('Save Alias')")
        await save_btn.click()
        await asyncio.sleep(2)

        # Check that @staging_candidate now appears on the version
        assert await page.locator("text=@staging_candidate").first.is_visible(), "New alias @staging_candidate was not rendered!"
        print("Successfully assigned and verified custom alias @staging_candidate.")

        alias_assigned_path = os.path.join(ARTIFACT_DIR, "unity_catalog_alias_assigned_dark.png")
        await page.screenshot(path=alias_assigned_path, full_page=False)
        print(f"Saved {alias_assigned_path}")

        # Clean up the test alias so we don't leave temporary debris
        delete_alias_btn = page.locator("span:has-text('@staging_candidate') button").first
        if await delete_alias_btn.is_visible():
            await delete_alias_btn.click()
            await asyncio.sleep(1.5)
            print("Cleaned up temporary alias @staging_candidate.")

        # 6. Test 'Query with SQL' action
        print("Testing 'Query with SQL' button...")
        query_sql_btn = page.locator("button:has-text('Query with SQL')").first
        await query_sql_btn.click()
        await asyncio.sleep(1.5)

        # Verify we transitioned to SQL Editor view
        run_query_btn = page.locator("button[title*='Run Query (Ctrl+Enter)']").first
        await run_query_btn.wait_for(state="visible", timeout=5000)
        print("Transitioned to SQL Editor view.")

        # Execute the query
        await run_query_btn.click()
        await page.wait_for_function("() => !Alpine.$data(document.querySelector('[x-data]')).isExecuting", timeout=15000)
        await asyncio.sleep(2)

        res_info = await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                return {
                    sqlQuery: root.sqlQuery,
                    isExecuting: root.isExecuting,
                    queryResult: root.queryResult ? {
                        columns: root.queryResult.columns,
                        rowCount: root.queryResult.rows ? root.queryResult.rows.length : 0,
                        error: root.queryResult.error,
                        success: root.queryResult.success
                    } : null
                };
            }
        """)
        print("SQL execution debug info:", res_info)

        assert res_info['queryResult'] and res_info['queryResult']['success'], f"Query execution failed: {res_info}"
        col_names = [c['name'] for c in res_info['queryResult']['columns']]
        assert 'turnover_risk_score' in col_names, f"Expected turnover_risk_score in columns: {col_names}"
        print(f"Verified query returned {res_info['queryResult']['rowCount']} rows with columns: {col_names}")

        grid = page.locator(".sql-results-grid")
        await grid.wait_for(state="visible", timeout=5000)

        sql_exec_dark_path = os.path.join(ARTIFACT_DIR, "unity_catalog_model_sql_executed_dark.png")
        await page.screenshot(path=sql_exec_dark_path, full_page=False)
        print(f"Saved {sql_exec_dark_path}")

        # 7. Light mode verification
        print("Testing Light Mode...")
        await page.evaluate("""
            () => {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('theme', 'light');
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.darkMode = false;
                root.currentView = 'catalog';
                root.selectModel('employee_turnover_predictor', 'warehouse', 'dbo');
            }
        """)
        await asyncio.sleep(2)

        light_screenshot_path = os.path.join(ARTIFACT_DIR, "unity_catalog_model_governance_light.png")
        await page.screenshot(path=light_screenshot_path, full_page=False)
        print(f"Saved {light_screenshot_path}")

        await browser.close()
        print("ALL STEP 2 VERIFICATIONS PASSED PERFECTLY!")

if __name__ == "__main__":
    asyncio.run(run_verification())
