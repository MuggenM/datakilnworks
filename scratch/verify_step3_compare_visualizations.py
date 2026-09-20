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
                root.fetchExperiments();
            }
        """)
        await asyncio.sleep(2)

        # Select customer churn experiment
        print("Selecting customer_churn_prediction experiment...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                const churnExp = root.experiments.find(e => e.name.includes('customer_churn'));
                if (churnExp) {
                    root.selectExperiment(churnExp);
                }
            }
        """)
        await asyncio.sleep(2)

        # Select all runs
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.selectAllExpRuns();
            }
        """)
        await asyncio.sleep(0.5)

        # Verify 3 runs selected
        selected_count = await page.evaluate("() => Alpine.$data(document.querySelector('[x-data]')).selectedExpRunIds.length")
        print(f"Selected runs count: {selected_count}")
        assert selected_count == 3, f"Expected 3 runs selected, got {selected_count}"

        # 3. Open Compare Runs Modal
        print("3. Opening Compare Runs Modal...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.openCompareRunsModal();
            }
        """)
        await asyncio.sleep(2.5)

        # Verify modal is open
        modal = page.locator("div[x-show='showCompareRunsModal']")
        assert await modal.is_visible(), "Compare Runs Modal is not visible!"
        print("Compare Runs Modal opened successfully.")

        # ============================================================
        # 4. TAB 1: OVERVIEW & DIFF
        # ============================================================
        print("4. Verifying Overview & Diff Tab...")
        await page.locator("text=Evaluation Metrics Comparison").wait_for(state="visible", timeout=5000)
        await page.locator("text=Hyperparameter Divergence (Diff)").wait_for(state="visible", timeout=5000)
        
        overview_dark_path = os.path.join(ARTIFACT_DIR, "experiments_comparison_overview_dark.png")
        await page.screenshot(path=overview_dark_path, full_page=False)
        print(f"Saved: {overview_dark_path}")

        # ============================================================
        # 5. TAB 2: PARALLEL COORDINATES (DARK MODE)
        # ============================================================
        print("5. Switching to Parallel Coordinates Tab...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.setCompareTab('parallel');
            }
        """)
        await asyncio.sleep(1.5)

        # Verify SVG and paths
        svg = page.locator("#parallel-coords-svg")
        assert await svg.is_visible(), "Parallel Coordinates SVG not visible!"
        
        path_count = await page.evaluate("() => document.querySelectorAll('#parallel-paths-group path').length")
        print(f"Rendered Bézier curves count: {path_count}")
        assert path_count == 3, f"Expected 3 paths rendered, got {path_count}"

        # Highlight first run in legend to trigger glow
        print("Highlighting first run in legend...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                const firstRun = root.compareRunsData.runs[0];
                root.highlightParallelRun(firstRun.run_id);
            }
        """)
        await asyncio.sleep(0.5)

        parallel_dark_path = os.path.join(ARTIFACT_DIR, "experiments_parallel_coordinates_dark.png")
        await page.screenshot(path=parallel_dark_path, full_page=False)
        print(f"Saved: {parallel_dark_path}")

        # ============================================================
        # 6. TAB 2: PARALLEL COORDINATES (LIGHT MODE)
        # ============================================================
        print("6. Switching to Light Mode for Parallel Coordinates...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.isDarkMode = false;
                document.documentElement.classList.remove('dark');
                localStorage.setItem('dbx_theme', 'light');
                root.renderParallelCoordinates();
            }
        """)
        await asyncio.sleep(0.8)

        parallel_light_path = os.path.join(ARTIFACT_DIR, "experiments_parallel_coordinates_light.png")
        await page.screenshot(path=parallel_light_path, full_page=False)
        print(f"Saved: {parallel_light_path}")

        # Switch back to Dark Mode
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.isDarkMode = true;
                document.documentElement.classList.add('dark');
                localStorage.setItem('dbx_theme', 'dark');
                root.renderParallelCoordinates();
            }
        """)
        await asyncio.sleep(0.5)

        # ============================================================
        # 7. TAB 3: SCATTER PLOT (DARK MODE)
        # ============================================================
        print("7. Switching to Scatter Plot Tab...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.setCompareTab('scatter');
            }
        """)
        await asyncio.sleep(1.5)

        scatter_canvas = page.locator("#compare-scatter-chart")
        assert await scatter_canvas.is_visible(), "Scatter plot canvas not visible!"
        
        scatter_points_count = await page.evaluate("() => Alpine.$data(document.querySelector('[x-data]')).getScatterPlotCount()")
        print(f"Scatter plotted runs count: {scatter_points_count}")
        assert scatter_points_count == 3, f"Expected 3 points in scatter plot, got {scatter_points_count}"

        scatter_dark_path = os.path.join(ARTIFACT_DIR, "experiments_scatter_plot_dark.png")
        await page.screenshot(path=scatter_dark_path, full_page=False)
        print(f"Saved: {scatter_dark_path}")

        # Test Axis Swapping
        print("Testing axis swapping...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.swapScatterAxes();
            }
        """)
        await asyncio.sleep(0.5)

        # ============================================================
        # 8. TAB 3: SCATTER PLOT (LIGHT MODE)
        # ============================================================
        print("8. Switching to Light Mode for Scatter Plot...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.isDarkMode = false;
                document.documentElement.classList.remove('dark');
                localStorage.setItem('dbx_theme', 'light');
                root.renderCompareScatterChart();
            }
        """)
        await asyncio.sleep(0.8)

        scatter_light_path = os.path.join(ARTIFACT_DIR, "experiments_scatter_plot_light.png")
        await page.screenshot(path=scatter_light_path, full_page=False)
        print(f"Saved: {scatter_light_path}")

        # Switch back to Dark Mode
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.isDarkMode = true;
                document.documentElement.classList.add('dark');
                localStorage.setItem('dbx_theme', 'dark');
                root.renderCompareScatterChart();
            }
        """)
        await asyncio.sleep(0.5)

        # ============================================================
        # 9. TAB 4: LEARNING CURVES (DARK MODE)
        # ============================================================
        print("9. Switching to Learning Curves Tab...")
        await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                root.setCompareTab('curves');
            }
        """)
        await asyncio.sleep(1.5)

        curves_canvas = page.locator("#compare-runs-chart")
        assert await curves_canvas.is_visible(), "Curves canvas not visible!"

        curves_count = await page.evaluate("""
            () => {
                const root = Alpine.$data(document.querySelector('[x-data]'));
                return root.expCompareChartInstance?.data?.datasets?.length || 0;
            }
        """)
        print(f"Learning curves datasets count: {curves_count}")
        assert curves_count == 3, f"Expected 3 datasets in curves chart, got {curves_count}"

        curves_dark_path = os.path.join(ARTIFACT_DIR, "experiments_learning_curves_dark.png")
        await page.screenshot(path=curves_dark_path, full_page=False)
        print(f"Saved: {curves_dark_path}")

        print("All verification steps completed successfully!")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run_verification())
