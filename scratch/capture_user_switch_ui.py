#!/usr/bin/env python3
"""
Playwright script to verify UI user switching and capture dual-theme screenshots.
Uses cookie-based authentication and interactive UI clicks.
"""

import os
import time
import requests
from playwright.sync_api import sync_playwright

BASE_URL = "http://localhost:8891"
ARTIFACT_DIR = "/home/martin/.gemini/antigravity-cli/brain/e598d0e4-1391-4974-9343-8271c1d6a271"
os.makedirs(ARTIFACT_DIR, exist_ok=True)

def get_session_token(username, password):
    res = requests.post(f"{BASE_URL}/api/auth/login", json={"username": username, "password": password})
    assert res.status_code == 200, f"Login failed for {username}"
    return res.cookies.get("localspark_session")

def run():
    bob_token = get_session_token("analyst_bob", "userpassword123")
    admin_token = get_session_token("admin", "adminpassword123")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})

        # Set bob session cookie
        context.add_cookies([{
            "name": "localspark_session",
            "value": bob_token,
            "url": "http://localhost:8891"
        }])

        page = context.new_page()
        print("1. Loading Localspark Studio as analyst_bob...")
        page.goto(f"{BASE_URL}/")
        page.wait_for_timeout(3000)

        # Ensure we are in Workspace view
        page.evaluate("""
            () => {
                const alpine = window.Alpine ? Alpine.$data(document.querySelector('[x-data]')) : null;
                if (alpine) {
                    alpine.currentView = 'workspace';
                    alpine.isDarkMode = true;
                }
                document.documentElement.classList.add('dark');
            }
        """)
        page.wait_for_timeout(2000)

        # Capture analyst_bob workspace in dark mode
        bob_dark = os.path.join(ARTIFACT_DIR, "user_switch_workspace_analyst_bob_dark.png")
        page.screenshot(path=bob_dark)
        print(f"✓ Saved analyst_bob workspace (Dark): {bob_dark}")

        # Switch to light mode
        page.evaluate("""
            () => {
                const alpine = window.Alpine ? Alpine.$data(document.querySelector('[x-data]')) : null;
                if (alpine) alpine.isDarkMode = false;
                document.documentElement.classList.remove('dark');
            }
        """)
        page.wait_for_timeout(1000)

        bob_light = os.path.join(ARTIFACT_DIR, "user_switch_workspace_analyst_bob_light.png")
        page.screenshot(path=bob_light)
        print(f"✓ Saved analyst_bob workspace (Light): {bob_light}")

        # Switch back to dark mode
        page.evaluate("""
            () => {
                const alpine = window.Alpine ? Alpine.$data(document.querySelector('[x-data]')) : null;
                if (alpine) alpine.isDarkMode = true;
                document.documentElement.classList.add('dark');
            }
        """)
        page.wait_for_timeout(600)

        # Open the user profile dropdown menu
        print("2. Opening user profile dropdown menu...")
        page.click("button:has-text('analyst_bob')")
        page.wait_for_timeout(1000)

        dropdown_dark = os.path.join(ARTIFACT_DIR, "user_switch_menu_dropdown_dark.png")
        page.screenshot(path=dropdown_dark)
        print(f"✓ Saved user profile dropdown menu (Dark): {dropdown_dark}")

        # Click Admin in the Switch Account menu
        print("3. Switching account to admin via quick switch...")
        page.click("button:has-text('Admin'):not(:has-text('Power'))")
        page.wait_for_timeout(3500)

        # Verify admin workspace
        admin_dark = os.path.join(ARTIFACT_DIR, "user_switch_workspace_admin_dark.png")
        page.screenshot(path=admin_dark)
        print(f"✓ Saved admin workspace showing all users (Dark): {admin_dark}")

        # Admin light mode
        page.evaluate("""
            () => {
                const alpine = window.Alpine ? Alpine.$data(document.querySelector('[x-data]')) : null;
                if (alpine) alpine.isDarkMode = false;
                document.documentElement.classList.remove('dark');
            }
        """)
        page.wait_for_timeout(1000)

        admin_light = os.path.join(ARTIFACT_DIR, "user_switch_workspace_admin_light.png")
        page.screenshot(path=admin_light)
        print(f"✓ Saved admin workspace (Light): {admin_light}")

        # Navigate to Prompt Playground
        print("4. Navigating to Prompt Playground as Admin...")
        page.evaluate("""
            () => {
                const alpine = window.Alpine ? Alpine.$data(document.querySelector('[x-data]')) : null;
                if (alpine) alpine.currentView = 'playground';
            }
        """)
        page.wait_for_timeout(2000)

        playground_light = os.path.join(ARTIFACT_DIR, "user_switch_playground_admin_light.png")
        page.screenshot(path=playground_light)
        print(f"✓ Saved admin playground (Light): {playground_light}")

        browser.close()
        print("\nAll browser UI screenshots captured successfully!")

if __name__ == "__main__":
    run()
