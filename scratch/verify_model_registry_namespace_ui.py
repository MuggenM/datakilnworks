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

        # Authenticate and switch to models/experiments view
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.currentUser = { username: 'admin', role: 'admin', display_name: 'Administrator' };
                root.isAuthenticated = true;
                root.isLoggedIn = true;
                root.showLoginModal = false;
                root.currentView = 'experiments';
                root.mlSubTab = 'models';
                root.fetchRegisteredModels();
            }
        """)
        await asyncio.sleep(2)

        # 1. Open Register Model Modal
        print("Opening Register Model modal...")
        reg_btn = page.locator("button:has-text('Register Model')").first
        await reg_btn.click()
        await asyncio.sleep(1)

        modal_title = page.locator("h3:has-text('Register New ML Model')")
        assert await modal_title.is_visible(), "Register Model modal did not open!"
        print("Register Model modal opened.")

        # Capture modal screenshot in Dark Mode
        modal_screenshot_path = os.path.join(ARTIFACT_DIR, "unity_catalog_register_model_modal_dark.png")
        await page.screenshot(path=modal_screenshot_path, full_page=False)
        print(f"Saved {modal_screenshot_path}")

        # Fill 3-level model name
        name_input = page.locator("input[placeholder*='warehouse.dbo.credit_risk_evaluator']").first
        await name_input.fill("marketing.analytics.customer_churn_risk")

        desc_input = page.locator("textarea[placeholder*='Business purpose']").first
        await desc_input.fill("Predicts voluntary customer churn using behavioral engagement metrics.")

        submit_btn = page.locator("button:has-text('Register Model')").last
        await submit_btn.click()
        await asyncio.sleep(2)
        print("Submitted model registration with 3-level name.")

        # 2. Switch to Catalog view to verify model tree hierarchy
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.currentView = 'catalog';
                root.fetchCatalogs();
            }
        """)
        await asyncio.sleep(2)

        # Expand warehouse and schemas if needed, or select model directly
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.selectModel('customer_churn_risk', 'marketing', 'analytics');
            }
        """)
        await asyncio.sleep(1.5)

        # Verify model inspection header
        header = page.locator("h2:has-text('customer_churn_risk')")
        assert await header.is_visible(), "customer_churn_risk header not visible!"
        print("Model inspection view loaded for customer_churn_risk.")

        catalog_details_dark_path = os.path.join(ARTIFACT_DIR, "unity_catalog_3level_model_details_dark.png")
        await page.screenshot(path=catalog_details_dark_path, full_page=False)
        print(f"Saved {catalog_details_dark_path}")

        # 3. Test light mode
        print("Switching to Light Mode...")
        await page.evaluate("""
            () => {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('theme', 'light');
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.darkMode = false;
            }
        """)
        await asyncio.sleep(1)

        catalog_details_light_path = os.path.join(ARTIFACT_DIR, "unity_catalog_3level_model_details_light.png")
        await page.screenshot(path=catalog_details_light_path, full_page=False)
        print(f"Saved {catalog_details_light_path}")

        # Clean up test model
        await page.evaluate("""
            async () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                await fetch('/api/2.0/mlflow/registered-models/customer_churn_risk', { method: 'DELETE' });
                await root.fetchRegisteredModels();
            }
        """)
        await asyncio.sleep(1)

        await browser.close()
        print("ALL UI VERIFICATIONS COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(run_verification())
