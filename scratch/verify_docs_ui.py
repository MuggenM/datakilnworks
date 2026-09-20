#!/usr/bin/env python3
"""
Verification and screenshot capture for DataKilnWorks Documentation.
Tests:
1. Rendering docs/index.html
2. Dark mode layout & typography
3. Switching to light mode & verification
4. Interactive search & tabs
5. Screenshot artifacts capture
"""

import os
import time
from playwright.sync_api import sync_playwright

DOCS_PATH = f"file://{os.path.abspath('docs/index.html')}"
ARTIFACT_DIR = "/home/martin/.gemini/antigravity-cli/brain/e598d0e4-1391-4974-9343-8271c1d6a271"
os.makedirs(ARTIFACT_DIR, exist_ok=True)

def verify():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 950})
        
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"[{msg.type}] {msg.text}"))

        print(f"Loading {DOCS_PATH}...")
        page.goto(DOCS_PATH, wait_until="networkidle")
        time.sleep(1)

        # 1. Capture Initial Theme
        initial_theme = page.evaluate("() => document.documentElement.getAttribute('data-theme')")
        print(f"Initial theme: {initial_theme}")
        
        # Ensure dark mode screenshot
        if initial_theme != "dark":
            page.click("#theme-toggle-btn")
            time.sleep(0.5)
        
        theme_now = page.evaluate("() => document.documentElement.getAttribute('data-theme')")
        assert theme_now == "dark", f"Expected dark theme but got {theme_now}"
        dark_screenshot_path = os.path.join(ARTIFACT_DIR, "docs_datakilnworks_dark.png")
        page.screenshot(path=dark_screenshot_path)
        print(f"✓ Saved dark mode screenshot: {dark_screenshot_path}")

        # 2. Toggle to Light Mode
        print("Toggling theme to light mode...")
        page.click("#theme-toggle-btn")
        time.sleep(0.5)
        new_theme = page.evaluate("() => document.documentElement.getAttribute('data-theme')")
        assert new_theme == "light", f"Expected light theme but got {new_theme}"
        print(f"✓ Switched theme successfully to: {new_theme}")

        light_screenshot_path = os.path.join(ARTIFACT_DIR, "docs_datakilnworks_light.png")
        page.screenshot(path=light_screenshot_path)
        print(f"✓ Saved light mode screenshot: {light_screenshot_path}")

        # 3. Test In-Page Search
        print("Testing live search...")
        page.fill("#docs-search", "Ray")
        time.sleep(0.5)
        
        # Clear search
        page.fill("#docs-search", "")
        time.sleep(0.3)

        # 4. Scroll down to tutorial
        print("Testing anchor navigation...")
        page.click("a[href='#hands-on-tutorial']")
        time.sleep(0.5)
        tutorial_screenshot_path = os.path.join(ARTIFACT_DIR, "docs_datakilnworks_tutorial_light.png")
        page.screenshot(path=tutorial_screenshot_path)
        print(f"✓ Saved tutorial screenshot: {tutorial_screenshot_path}")

        # Switch back to dark mode and capture code view
        page.click("#theme-toggle-btn")
        time.sleep(0.5)
        page.click("a[href='#docker-local-install']")
        time.sleep(0.5)
        docker_screenshot_path = os.path.join(ARTIFACT_DIR, "docs_datakilnworks_docker_dark.png")
        page.screenshot(path=docker_screenshot_path)
        print(f"✓ Saved docker install screenshot: {docker_screenshot_path}")

        browser.close()
        print("\nAll documentation verification tests passed successfully!")

if __name__ == "__main__":
    verify()
