import os
import sys
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

        # Authenticate and switch to SQL Editor
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.currentUser = { username: 'admin', role: 'admin', display_name: 'Administrator' };
                root.isAuthenticated = true;
                root.isLoggedIn = true;
                root.showLoginModal = false;
                root.currentView = 'sql';
            }
        """)
        await asyncio.sleep(2)

        test_sql = """-- Databricks MLflow Parity: SQL-Native In-Lakehouse ML Inference
SELECT 
    id,
    income,
    predict('finance.risk.credit_default_model', {'income': income, 'debt_to_income': debt_to_income, 'credit_score': credit_score}) AS credit_risk_decision,
    predict_score('finance.risk.credit_default_model', {'income': income, 'debt_to_income': debt_to_income, 'credit_score': credit_score}) AS default_probability,
    predict('employee_turnover_predictor@champion', {'salary': income * 0.65, 'tenure_years': 2.5}) AS employee_attrition_risk
FROM (
    SELECT 101 AS id, 160000 AS income, 0.08 AS debt_to_income, 815 AS credit_score
    UNION ALL
    SELECT 102 AS id, 23000 AS income, 0.72 AS debt_to_income, 560 AS credit_score
) applicants;"""

        # Set Monaco editor content and run query
        await page.evaluate(f"""
            () => {{
                const root = Alpine.$data(document.querySelector('[x-data]'));
                if (window._monacoEditor) {{
                    window._monacoEditor.setValue({repr(test_sql)});
                }}
                root.sqlQuery = {repr(test_sql)};
                root.runQuery();
            }}
        """)
        print("Dispatched query to serverless warehouse...")
        await asyncio.sleep(3)

        # Wait until query completes and results are populated
        await page.wait_for_function("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                return root.queryResult && root.queryResult.success && root.queryResult.rows && root.queryResult.rows.length > 0;
            }
        """, timeout=15000)
        print("Query executed and results populated in Alpine.js state.")
        await asyncio.sleep(1)

        # Ensure results grid is visible
        grid = page.locator(".sql-results-grid").first
        await grid.wait_for(state="visible", timeout=5000)
        print("Query results grid loaded successfully.")

        # Capture Dark Mode Screenshot
        dark_shot = os.path.join(ARTIFACT_DIR, "sql_native_inference_dark.png")
        await page.screenshot(path=dark_shot, full_page=False)
        print(f"Saved Dark Mode screenshot: {dark_shot}")

        # Switch to Light Mode via root.toggleTheme()
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                if (root.isDarkMode) {
                    root.toggleTheme();
                }
            }
        """)
        await asyncio.sleep(1.5)

        # Capture Light Mode Screenshot
        light_shot = os.path.join(ARTIFACT_DIR, "sql_native_inference_light.png")
        await page.screenshot(path=light_shot, full_page=False)
        print(f"Saved Light Mode screenshot: {light_shot}")

        await browser.close()
        print("Verification completed successfully!")

if __name__ == "__main__":
    asyncio.run(run_verification())
