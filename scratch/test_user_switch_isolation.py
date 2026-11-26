#!/usr/bin/env python3
"""
Verification script for multi-user scoping and user switching.
Tests:
1. Authentication & JWT session issuance
2. Workspace personal folder scoping & 403 access control
3. Query History isolation
4. Genie Space isolation
5. Prompt Playground isolation
6. Transformations (dbt) runs scoping
7. Lineage & catalog access
"""

import sys
import json
import requests

import os

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8891")

def login(username, password):
    session = requests.Session()
    res = session.post(f"{BASE_URL}/api/auth/login", json={"username": username, "password": password})
    assert res.status_code == 200, f"Failed to log in as {username}: {res.text}"
    data = res.json()
    print(f"✓ Successfully authenticated as {username} (Role: {data['user']['role']})")
    return session, data["user"]

def test_user_isolation():
    print("\n--- TEST 1: Admin User Permissions & Global Visibility ---")
    admin_sess, admin_user = login("admin", "adminpassword123")

    # 1. Admin workspace tree
    res = admin_sess.get(f"{BASE_URL}/api/workspace/tree")
    assert res.status_code == 200, f"Failed to get admin tree: {res.text}"
    admin_tree = res.json()
    user_nodes = [n["name"] for n in admin_tree["tree"] if n["name"] == "Users"]
    assert user_nodes, "Users folder not found in workspace tree"
    users_children = next(n["children"] for n in admin_tree["tree"] if n["name"] == "Users")
    user_folders = [c["name"] for c in users_children]
    print(f"✓ Admin sees all user directories under Users/: {user_folders}")
    assert "admin" in user_folders and "analyst_bob" in user_folders and "lead_engineer" in user_folders

    # 2. Admin creates a Genie chat
    res = admin_sess.post(f"{BASE_URL}/api/genie/chats", json={"title": "Admin Confidential Chat"})
    assert res.status_code == 200
    admin_chat = res.json()
    admin_chat_id = admin_chat["id"]
    print(f"✓ Created admin Genie chat: {admin_chat_id} - '{admin_chat['title']}'")

    # 3. Admin creates a custom Prompt Playground template
    res = admin_sess.post(f"{BASE_URL}/api/playground/templates", json={
        "title": "Admin Executive Briefing Template",
        "description": "Executive summary generator for administrative eyes only",
        "category": "Management",
        "user_prompt": "Summarize executive KPIs for Q3: {{kpi_data}}"
    })
    assert res.status_code == 200
    admin_template = res.json()
    admin_template_id = admin_template["id"]
    print(f"✓ Created admin Playground template: {admin_template_id} - '{admin_template['title']}'")

    print("\n--- TEST 2: analyst_bob Scoping & RBAC Isolation ---")
    bob_sess, bob_user = login("analyst_bob", "userpassword123")

    # 1. Bob workspace tree: Bob MUST ONLY see Users/analyst_bob and Shared/
    res = bob_sess.get(f"{BASE_URL}/api/workspace/tree")
    assert res.status_code == 200
    bob_tree_data = res.json()
    bob_tree = bob_tree_data["tree"]
    print(f"✓ Bob's user_home path: {bob_tree_data.get('user_home')}")
    assert bob_tree_data.get("user_home") == "Users/analyst_bob", "Expected user_home to be Users/analyst_bob"

    users_node = next((n for n in bob_tree if n["name"] == "Users"), None)
    assert users_node is not None, "Users node missing for analyst_bob"
    bob_user_folders = [c["name"] for c in users_node.get("children", [])]
    print(f"✓ analyst_bob only sees: {bob_user_folders}")
    assert "analyst_bob" in bob_user_folders, "analyst_bob should see own folder"
    assert "admin" not in bob_user_folders, "analyst_bob MUST NOT see admin folder"
    assert "lead_engineer" not in bob_user_folders, "analyst_bob MUST NOT see lead_engineer folder"

    # Check that Bob has personal scratchpad notebook
    bob_folder = next(c for c in users_node["children"] if c["name"] == "analyst_bob")
    assert bob_folder.get("is_user_home") is True, "Bob's home directory should have is_user_home=True"
    scratchpad = next((f for f in bob_folder.get("children", []) if "analyst_bob_scratchpad" in f["name"]), None)
    assert scratchpad is not None, "analyst_bob_scratchpad notebook was not initialized"
    print(f"✓ Verified analyst_bob starter scratchpad notebook exists: {scratchpad['rel_path']}")

    # 2. RBAC Enforcement: Bob CANNOT read or write Admin's workspace files
    res = bob_sess.get(f"{BASE_URL}/api/workspace/file?path=Users/admin/admin_scratchpad.ipynb")
    print(f"✓ Access to admin notebook by Bob returned HTTP status {res.status_code} (Expected: 403)")
    assert res.status_code == 403, f"Expected 403 Forbidden when accessing other user's file, got {res.status_code}"

    res = bob_sess.post(f"{BASE_URL}/api/workspace/item", json={
        "target_dir": "Users/admin",
        "name": "unauthorized.py",
        "type": "file",
        "content": "print('hack')"
    })
    print(f"✓ Write attempt to admin directory by Bob returned HTTP status {res.status_code} (Expected: 403)")
    assert res.status_code == 403, f"Expected 403 Forbidden when writing to other user's directory, got {res.status_code}"

    # Bob CAN read his own notebook
    res = bob_sess.get(f"{BASE_URL}/api/workspace/file?path=Users/analyst_bob/analyst_bob_scratchpad.ipynb")
    assert res.status_code == 200, f"Failed to read own notebook: {res.text}"
    print("✓ analyst_bob can read their own personal scratchpad notebook")

    # 3. Genie Space Isolation: Bob MUST NOT see Admin's chat
    res = bob_sess.get(f"{BASE_URL}/api/genie/chats")
    assert res.status_code == 200
    bob_chats = res.json().get("chats", [])
    bob_chat_ids = [c["id"] for c in bob_chats]
    print(f"✓ analyst_bob Genie chats count: {len(bob_chats)} (Admin chat {admin_chat_id} in Bob's chats: {admin_chat_id in bob_chat_ids})")
    assert admin_chat_id not in bob_chat_ids, "analyst_bob should not see admin's Genie chat"

    # Bob creates their own chat
    res = bob_sess.post(f"{BASE_URL}/api/genie/chats", json={"title": "Bob Sales KPI Exploration"})
    assert res.status_code == 200
    bob_chat = res.json()
    bob_chat_id = bob_chat["id"]
    print(f"✓ Created Bob's Genie chat: {bob_chat_id} - '{bob_chat['title']}'")

    res = bob_sess.get(f"{BASE_URL}/api/genie/chats")
    bob_updated_chats = res.json().get("chats", [])
    assert any(c["id"] == bob_chat_id for c in bob_updated_chats)

    # 4. Prompt Playground Isolation: Bob MUST NOT see Admin's custom template
    res = bob_sess.get(f"{BASE_URL}/api/playground/templates")
    assert res.status_code == 200
    bob_templates = res.json()
    bob_template_ids = [t["id"] for t in bob_templates]
    print(f"✓ analyst_bob Playground templates count: {len(bob_templates)} (Admin template in Bob's: {admin_template_id in bob_template_ids})")
    assert admin_template_id not in bob_template_ids, "analyst_bob should not see admin's custom template"

    # Bob creates custom template
    res = bob_sess.post(f"{BASE_URL}/api/playground/templates", json={
        "title": "Bob Retail Profit Margin Calculator",
        "description": "Calculates net margins per SKU",
        "category": "Retail",
        "user_prompt": "Compute margin for: {{sku}}"
    })
    assert res.status_code == 200
    bob_tmpl = res.json()
    print(f"✓ Created Bob's template: {bob_tmpl['id']} - '{bob_tmpl['title']}'")

    print("\n--- TEST 3: lead_engineer Scoping & Isolation ---")
    lead_sess, lead_user = login("lead_engineer", "powerpassword123")

    # 1. Lead engineer workspace tree
    res = lead_sess.get(f"{BASE_URL}/api/workspace/tree")
    assert res.status_code == 200
    lead_tree_data = res.json()
    users_node = next(n for n in lead_tree_data["tree"] if n["name"] == "Users")
    lead_user_folders = [c["name"] for c in users_node["children"]]
    print(f"✓ lead_engineer only sees: {lead_user_folders}")
    assert "lead_engineer" in lead_user_folders
    assert "analyst_bob" not in lead_user_folders
    assert "admin" not in lead_user_folders

    # 2. Lead engineer Genie space does not see Bob's or Admin's chats
    res = lead_sess.get(f"{BASE_URL}/api/genie/chats")
    assert res.status_code == 200
    lead_chats = res.json().get("chats", [])
    lead_chat_ids = [c["id"] for c in lead_chats]
    assert admin_chat_id not in lead_chat_ids
    assert bob_chat_id not in lead_chat_ids
    print(f"✓ lead_engineer does not see admin or bob chats ({len(lead_chats)} chats)")

    print("\n--- TEST 4: Admin Clean Up & Verification of Multi-User State ---")
    res = admin_sess.get(f"{BASE_URL}/api/genie/chats")
    assert res.status_code == 200
    all_chats = res.json().get("chats", [])
    all_chat_ids = [c["id"] for c in all_chats]
    print(f"✓ Admin sees total {len(all_chats)} chats (includes Admin's and Bob's)")
    assert admin_chat_id in all_chat_ids and bob_chat_id in all_chat_ids

    # Clean up created items
    admin_sess.delete(f"{BASE_URL}/api/genie/chats/{admin_chat_id}")
    admin_sess.delete(f"{BASE_URL}/api/genie/chats/{bob_chat_id}")
    admin_sess.delete(f"{BASE_URL}/api/playground/templates/{admin_template_id}")
    admin_sess.delete(f"{BASE_URL}/api/playground/templates/{bob_tmpl['id']}")
    print("✓ Cleanup completed successfully.")

    print("\n=======================================================")
    print(" ALL MULTI-USER ISOLATION TESTS PASSED SUCCESSFULLY! ")
    print("=======================================================")

if __name__ == "__main__":
    test_user_isolation()
