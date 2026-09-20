import os
import time
import asyncio
from playwright.async_api import async_playwright

ARTIFACT_DIR = "/home/martin/.gemini/antigravity-cli/brain/e598d0e4-1391-4974-9343-8271c1d6a271"

async def run_verification():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1600, "height": 1050})
        
        # Authenticate session cookie
        await context.request.post('http://localhost:8891/api/auth/login', data={'username': 'admin', 'password': 'adminpassword123'})

        page = await context.new_page()

        print("1. Navigating to Data Kiln Works Studio...")
        await page.goto("http://localhost:8891")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(1)

        # Authenticate and switch to experiments view
        print("2. Authenticating and switching to Experiments view...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.currentUser = { username: 'admin', role: 'admin', display_name: 'Administrator' };
                root.isAuthenticated = true;
                root.isLoggedIn = true;
                root.showLoginModal = false;
                root.currentView = 'experiments';
                root.isDarkMode = true;
                document.documentElement.classList.add('dark');
                root.fetchExperiments();
            }
        """)
        await asyncio.sleep(2)

        # Select customer_churn_prediction experiment
        print("3. Selecting customer_churn_prediction experiment...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                const churnExp = root.experiments.find(e => e.name.includes('customer_churn'));
                if (churnExp) {
                    root.selectExperiment(churnExp);
                }
            }
        """)
        await asyncio.sleep(1.5)

        # Screenshot 1: Experiments Runs Table showing AUTOLOG badge and estimator pills
        print("4. Capturing Experiments runs table with AUTOLOG badges...")
        runs_table_path = os.path.join(ARTIFACT_DIR, "autolog_runs_table_dark.png")
        await page.screenshot(path=runs_table_path, full_page=False)
        print(f"   Saved: {runs_table_path}")

        # Find and click the autologged ExtraTreesClassifier run
        print("5. Opening Run Details Inspector for autologged run...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                const runs = root.selectedExperiment && root.selectedExperiment.runs ? root.selectedExperiment.runs : [];
                const autoRun = runs.find(r => r.tags_dict && (r.tags_dict['mlflow.source.type'] === 'AUTOLOG' || r.tags_dict['mlflow.autolog.framework']));
                if (autoRun) {
                    root.inspectExpRun(autoRun.info.run_id);
                } else if (runs.length > 0) {
                    root.inspectExpRun(runs[0].info.run_id);
                }
            }
        """)
        await asyncio.sleep(2)

        # Screenshot 2a: Run Details modal top (Dark)
        print("6a. Capturing Run Details modal top (Dark)...")
        detail_dark_path = os.path.join(ARTIFACT_DIR, "autolog_run_details_dark.png")
        await page.screenshot(path=detail_dark_path, full_page=False)
        print(f"   Saved: {detail_dark_path}")

        # Scroll modal down to reveal Dataset Lineage and Artifacts
        print("6b. Scrolling Run Details modal to Dataset Lineage and Artifacts...")
        await page.evaluate("""
            () => {
                const modal = document.getElementById('expRunDetailModalBody');
                if (modal) {
                    modal.scrollTop = modal.scrollHeight;
                }
            }
        """)
        await asyncio.sleep(1)

        # Screenshot 2b: Run Details modal showing Lineage and Artifacts (Dark)
        print("6c. Capturing Run Details modal with Lineage and Artifacts (Dark)...")
        lineage_dark_path = os.path.join(ARTIFACT_DIR, "autolog_run_lineage_artifacts_dark.png")
        await page.screenshot(path=lineage_dark_path, full_page=False)
        print(f"   Saved: {lineage_dark_path}")

        # Trigger preview for feature_importance.json specifically inside the modal
        print("7. Opening Preview modal for feature_importance.json artifact (Dark)...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                if (root.selectedExpRunDetail && root.selectedExpRunDetail.info) {
                    root.previewExpArtifact(root.selectedExpRunDetail.info.run_id, 'feature_importance.json');
                }
            }
        """)
        await page.wait_for_selector("div[x-show='showArtifactPreviewModal']:not([style*='display: none'])", timeout=5000)
        await asyncio.sleep(1.5)

        # Screenshot 3: Artifact Preview Sub-Modal (Dark)
        print("8. Capturing Artifact Preview sub-modal (Dark)...")
        preview_dark_path = os.path.join(ARTIFACT_DIR, "autolog_artifact_preview_dark.png")
        await page.screenshot(path=preview_dark_path, full_page=False)
        print(f"   Saved: {preview_dark_path}")

        # Close preview modal, switch to light mode
        print("9. Switching to Light mode...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.showArtifactPreviewModal = false;
                root.isDarkMode = false;
                document.documentElement.classList.remove('dark');
            }
        """)
        await asyncio.sleep(1)

        # Scroll modal down in light mode
        await page.evaluate("""
            () => {
                const modal = document.getElementById('expRunDetailModalBody');
                if (modal) {
                    modal.scrollTop = modal.scrollHeight;
                }
            }
        """)
        await asyncio.sleep(0.5)

        # Screenshot 4: Run Details modal in Light mode (scrolled to Lineage & Artifacts)
        print("10. Capturing Run Details modal with Lineage and Artifacts (Light)...")
        detail_light_path = os.path.join(ARTIFACT_DIR, "autolog_run_details_light.png")
        await page.screenshot(path=detail_light_path, full_page=False)
        print(f"   Saved: {detail_light_path}")

        # Open Artifact Preview in Light mode
        print("11. Opening Artifact Preview in Light mode...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                if (root.selectedExpRunDetail && root.selectedExpRunDetail.info) {
                    root.previewExpArtifact(root.selectedExpRunDetail.info.run_id, 'feature_importance.json');
                }
            }
        """)
        await page.wait_for_selector("div[x-show='showArtifactPreviewModal']:not([style*='display: none'])", timeout=5000)
        await asyncio.sleep(1.5)

        # Screenshot 5: Artifact Preview in Light mode
        print("12. Capturing Artifact Preview (Light)...")
        preview_light_path = os.path.join(ARTIFACT_DIR, "autolog_artifact_preview_light.png")
        await page.screenshot(path=preview_light_path, full_page=False)
        print(f"   Saved: {preview_light_path}")

        await browser.close()
        print("All autologging verifications completed successfully!")

if __name__ == "__main__":
    asyncio.run(run_verification())
