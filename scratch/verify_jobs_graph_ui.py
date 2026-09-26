#!/usr/bin/env python3
"""Jobs & Pipelines graph editor (drag to edit) and the run page (Playwright, host /usr/bin/python3). Builds a throwaway studio `dagui` on port 8117,
seeds workflows through the API and drives the canvas with real mouse gestures: layout, move, connect, cycle refusal, delete an edge, add / rename /
delete a task, save + persistence, validation errors, the Graph/List toggle, a run page coloured by result (success, failure with skipped
downstream, live RUNNING / PENDING, cancel, repair), the run selector and the minimap / zoom controls. Removes the container."""
import json, os, subprocess, sys, time
from playwright.sync_api import sync_playwright
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); BASE = "http://localhost:8117"
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
sh = lambda *a: subprocess.run(a, capture_output=True, text=True)
LONG = "select count(*) from range(100000000000)"
def T(tid, deps=(), q="select 1", **kw): return {"id": tid, "name": tid, "type": "sql", "depends_on": list(deps), "parameters": {"query": q}, **kw}
JOBS = {
    "diamond": [T("ingest"), T("clean", ["ingest"]), T("enrich", ["ingest"]), T("publish", ["clean", "enrich"])],
    "failing": [T("extract"), T("broken", ["extract"], "select * from table_that_does_not_exist"), T("after", ["broken"]), T("side", ["extract"])],
    "slowjob": [T("first"), T("slow", ["first"], LONG), T("last", ["slow"])],
}
sh("docker", "rm", "-f", "dagui")
sh("docker", "run", "-d", "--name", "dagui", "-p", "8117:8891", "-v", f"{ROOT}/web:/workspace/web", "-v", f"{ROOT}/docs:/workspace/docs", "-w", "/workspace", "-e", "WAREHOUSE_DIR=/workspace/warehouse",
   "-e", "INIT_ADMIN_USERNAME=admin", "-e", f"INIT_ADMIN_PASSWORD_HASH={HASH}", "localspark-lakehouse-notebook", "python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8891", "--no-proxy-headers")
try:
    for _ in range(90):
        if sh("curl", "-s", "-o", "/dev/null", f"{BASE}/api/docs").returncode == 0 and sh("docker", "exec", "dagui", "python", "-c", "import sqlite3;sqlite3.connect('/workspace/warehouse/.metadata/auth.db').execute('select 1 from users')").returncode == 0: break
        time.sleep(1)
    time.sleep(3)
    sh("docker", "exec", "dagui", "python", "-c", "import sqlite3;c=sqlite3.connect('/workspace/warehouse/.metadata/auth.db');c.execute('UPDATE users SET must_change_password=0');c.commit()")
    with sync_playwright() as p:
        b = p.chromium.launch(); ctx = b.new_context(viewport={"width": 1600, "height": 1000}); page = ctx.new_page(); errors = []; dialogs = []
        page.on("pageerror", lambda e: errors.append(str(e) + " @ " + str(getattr(e, "stack", ""))[:600])); page.on("dialog", lambda d: (dialogs.append(d.message), d.accept()))
        page.on("console", lambda m: errors.append(m.text[:200]) if m.type == "error" and "Failed to load resource" not in m.text else None)
        check("logged in", ctx.request.post(f"{BASE}/api/auth/login", data={"username": "admin", "password": "adminpassword123"}).ok)
        for jid, tasks in JOBS.items():
            r = ctx.request.post(f"{BASE}/api/jobs", data={"id": jid, "name": jid.title(), "description": "test", "enabled": True, "tasks": tasks}); assert r.ok, r.text()
        page.goto(BASE, wait_until="networkidle"); time.sleep(1)
        ev = lambda code: page.evaluate(code)
        ev("() => { const d = Alpine.$data(document.body); d.currentView = 'jobs'; d.fetchJobs(); }"); page.wait_for_timeout(1200)
        pick = lambda jid: (ev(f"() => {{ const d = Alpine.$data(document.body); d.selectJob(d.jobs.find(j => j.id === '{jid}')); }}"), page.wait_for_timeout(900))
        nodes = lambda: page.locator("[data-testid=dag-editor] [data-testid=dag-node]")
        edges = lambda: page.locator("[data-testid=dag-editor] [data-testid=dag-edge]")
        node = lambda tid: page.locator(f"[data-testid=dag-editor] [data-testid=dag-node][data-id={tid}]")
        box = lambda loc: loc.bounding_box()
        def drag(fx, fy, tx, ty, steps=12):
            page.mouse.move(fx, fy); page.mouse.down(); page.mouse.move(tx, ty, steps=steps); page.mouse.up(); page.wait_for_timeout(250)

        print("layout")
        pick("diamond")
        check("the graph view is the default and shows one node per task and one edge per dependency", page.locator("[data-testid=jobview-graph]").is_visible() and nodes().count() == 4 and edges().count() == 4, (nodes().count(), edges().count()))
        bx = {t: box(node(t)) for t in ("ingest", "clean", "enrich", "publish")}
        check("tasks are laid out in layers left to right by dependency", bx["ingest"]["x"] < bx["clean"]["x"] and abs(bx["clean"]["x"] - bx["enrich"]["x"]) < 2 and bx["clean"]["x"] < bx["publish"]["x"] and abs(bx["clean"]["y"] - bx["enrich"]["y"]) > 30, bx)
        check("the canvas has a minimap and zoom controls", page.locator("[data-testid=dag-minimap]").first.is_visible() and page.locator("[data-testid=dag-fit]").first.is_visible())
        page.screenshot(path="/tmp/dag_editor.png")

        print("move, connect, delete an edge")
        b0 = box(node("enrich")); drag(b0["x"] + 60, b0["y"] + 30, b0["x"] + 60, b0["y"] + 130)
        check("dragging a node moves it and marks the workflow as changed", box(node("enrich"))["y"] > b0["y"] + 80 and page.locator("[data-testid=graph-dirty]").is_visible(), (b0, box(node("enrich"))))
        po = node("ingest").locator("[data-testid=dag-port-out]").bounding_box(); bp = box(node("publish"))
        drag(po["x"] + po["width"] / 2, po["y"] + po["height"] / 2, bp["x"] + 100, bp["y"] + 30)
        check("dragging from a task's out-port onto another adds the dependency (ingest -> publish)", edges().count() == 5 and page.locator("[data-testid=dag-editor] [data-testid=dag-edge][data-from=ingest][data-to=publish]").count() == 1, edges().count())
        po = node("publish").locator("[data-testid=dag-port-out]").bounding_box(); bi = box(node("ingest"))
        n_toast = len(errors); drag(po["x"] + po["width"] / 2, po["y"] + po["height"] / 2, bi["x"] + 100, bi["y"] + 30)
        check("a dependency that would make a circle is refused", edges().count() == 5 and page.locator("[data-testid=dag-editor] [data-testid=dag-edge][data-from=publish][data-to=ingest]").count() == 0)
        po = node("ingest").locator("[data-testid=dag-port-out]").bounding_box(); bp = box(node("publish"))
        drag(po["x"] + po["width"] / 2, po["y"] + po["height"] / 2, bp["x"] + 100, bp["y"] + 30)
        check("...and so is a duplicate", edges().count() == 5)
        page.locator("[data-testid=dag-editor] [data-testid=dag-edge][data-from=ingest][data-to=publish] .dag-edge-hit").dispatch_event("pointerdown", {"bubbles": True})
        page.wait_for_timeout(200)
        check("clicking an edge shows its remove button", page.locator("[data-testid=dag-edge-delete]").first.is_visible())
        page.keyboard.press("Delete"); page.wait_for_timeout(250)
        check("Delete removes the selected dependency", edges().count() == 4)
        page.locator("[data-testid=dag-editor] [data-testid=dag-edge][data-from=clean][data-to=publish] .dag-edge-hit").dispatch_event("pointerdown", {"bubbles": True}); page.wait_for_timeout(150)
        page.locator("[data-testid=dag-edge-delete]").first.click(); page.wait_for_timeout(250)
        check("the x on an edge removes it too", edges().count() == 3)
        node("publish").click(); page.wait_for_timeout(250)
        check("(publish now depends only on enrich)", "enrich" in page.locator("[data-testid=graph-task-deps]").inner_text() and "clean" not in page.locator("[data-testid=graph-task-deps]").inner_text(), page.locator("[data-testid=graph-task-deps]").inner_text())
        po = node("clean").locator("[data-testid=dag-port-out]").bounding_box(); bp = box(node("publish"))
        drag(po["x"] + po["width"] / 2, po["y"] + po["height"] / 2, bp["x"] + 100, bp["y"] + 30)

        print("add, edit, rename, delete")
        page.click("[data-testid=graph-add-sql]"); page.wait_for_timeout(400)
        check("Add task creates a node and opens its panel", nodes().count() == 5 and page.locator("[data-testid=graph-task-panel]").is_visible())
        page.fill("[data-testid=graph-task-name]", "Audit log"); page.fill("[data-testid=graph-task-query]", "select 42 as answer")
        page.wait_for_timeout(200)
        check("editing the name in the panel updates the node", node(ev("() => Alpine.$data(document.body).graphSel")).inner_text().startswith("Audit log"))
        old_id = ev("() => Alpine.$data(document.body).graphSel")
        pf = node("publish").locator("[data-testid=dag-port-out]").bounding_box(); bn = box(node(old_id))
        drag(pf["x"] + pf["width"] / 2, pf["y"] + pf["height"] / 2, bn["x"] + 100, bn["y"] + 30)
        page.fill("[data-testid=graph-task-id]", "audit"); page.locator("[data-testid=graph-task-id]").press("Tab"); page.wait_for_timeout(300)
        check("renaming a task id keeps its links", node("audit").count() == 1 and page.locator("[data-testid=dag-editor] [data-testid=dag-edge][data-from=publish][data-to=audit]").count() == 1)
        page.fill("[data-testid=graph-task-id]", "clean"); page.locator("[data-testid=graph-task-id]").press("Tab"); page.wait_for_timeout(300)
        check("an id that is taken is refused", node("audit").count() == 1)
        page.click("[data-testid=graph-add-notebook]"); page.wait_for_timeout(300)
        nb = ev("() => Alpine.$data(document.body).graphSel"); page.click("[data-testid=graph-task-delete]"); page.wait_for_timeout(300)
        check("deleting a task (after confirmation) removes it", node(nb).count() == 0 and nodes().count() == 5 and any("Delete the task" in d for d in dialogs), dialogs)

        print("save and persistence")
        check("the workflow shows unsaved changes; Save is enabled", page.locator("[data-testid=graph-dirty]").is_visible() and page.locator("[data-testid=graph-save]").is_enabled())
        page.click("[data-testid=node-none]") if False else None
        page.locator("[data-testid=graph-task-retries]") if False else None
        node("audit").click(); page.locator("[data-testid=graph-task-resilience] input").first.fill("99")
        page.click("[data-testid=graph-save]"); page.wait_for_timeout(1200)
        check("the server's validation error is shown and nothing is saved", page.locator("[data-testid=graph-error]").is_visible() and "retries" in page.locator("[data-testid=graph-error]").inner_text() and page.locator("[data-testid=graph-dirty]").is_visible(), page.locator("[data-testid=graph-error]").inner_text() if page.locator("[data-testid=graph-error]").is_visible() else "no banner")
        node("audit").click(); page.locator("[data-testid=graph-task-resilience] input").first.fill("1")
        page.click("[data-testid=graph-save]"); page.wait_for_timeout(1500)
        saved = json.loads(ctx.request.get(f"{BASE}/api/jobs/diamond").text())
        st = {t["id"]: t for t in (saved.get("job") or saved)["tasks"]}
        check("Save stores tasks, dependencies and positions", set(st) == {"ingest", "clean", "enrich", "publish", "audit"} and st["audit"]["depends_on"] == ["publish"] and st["publish"]["depends_on"] == ["enrich", "clean"] and st["enrich"]["position"]["y"] > st["clean"]["position"]["y"] and st["audit"]["parameters"]["query"] == "select 42 as answer" and st["audit"]["retries"] == 1, st)
        check("the changed marker is gone", not page.locator("[data-testid=graph-dirty]").is_visible())
        page.reload(wait_until="networkidle"); time.sleep(1)
        ev("() => { const d = Alpine.$data(document.body); d.currentView = 'jobs'; d.fetchJobs(); }"); page.wait_for_timeout(1000); pick("diamond")
        check("after a reload the graph comes back exactly as saved (positions kept, 5 nodes, 5 edges)", nodes().count() == 5 and edges().count() == 5 and box(node("enrich"))["y"] > box(node("clean"))["y"] + 40, (nodes().count(), edges().count()))
        b0 = box(node("audit")); page.locator("[data-testid=dag-auto-layout]").first.click(); page.wait_for_timeout(400)
        check("Arrange automatically resets the positions (and counts as a change)", page.locator("[data-testid=graph-dirty]").is_visible() and abs(box(node("clean"))["y"] - box(node("enrich"))["y"]) > 30)
        page.click("[data-testid=graph-discard]"); page.wait_for_timeout(400)
        check("Discard brings the saved graph back", not page.locator("[data-testid=graph-dirty]").is_visible() and box(node("enrich"))["y"] > box(node("clean"))["y"] + 40)
        z0 = ev("() => document.querySelector('[data-testid=dag-editor] .dag-world').style.transform")
        page.locator("[data-testid=dag-zoom-in]").first.click(); page.wait_for_timeout(200)
        check("zoom controls change the view", ev("() => document.querySelector('[data-testid=dag-editor] .dag-world').style.transform") != z0)
        page.mouse.move(600, 500); page.mouse.wheel(0, -300); page.wait_for_timeout(200)

        print("graph and list")
        page.locator("[data-testid=jobview-list]").click(); page.wait_for_timeout(400)
        check("the list view (task cards) is still there", not page.locator("[data-testid=dag-editor]").is_visible() and page.locator("span:has-text('Depends on:'):visible").count() > 0)
        check("the choice is remembered", ev("() => localStorage.getItem('dkwJobView')") == "list")
        page.locator("[data-testid=jobview-graph]").click(); page.wait_for_timeout(500)
        check("switching back shows the graph", nodes().count() == 5 and nodes().first.is_visible())
        node("audit").click(); page.locator("[data-testid=graph-task-name]").fill("dirty edit")
        pick("failing")
        check("switching workflow with unsaved changes asks first (accepted here: the other workflow opens)", any("Discard the unsaved changes" in d for d in dialogs) and nodes().count() == 4, dialogs)

        print("run page: failure")
        page.locator("button:has-text('Run Now'):visible").first.click(); page.wait_for_selector("[data-testid=run-view]", state="visible", timeout=8000)
        page.wait_for_function("() => document.querySelector('[data-testid=run-status]') && document.querySelector('[data-testid=run-status]').innerText.trim() === 'FAILED'", timeout=30000)
        rn = lambda tid: page.locator(f"[data-testid=dag-run] [data-testid=dag-node][data-id={tid}]")
        stt = {t: rn(t).get_attribute("data-status") for t in ("extract", "broken", "after", "side")}
        check("the run page colours the graph by result: failed task red, its downstream skipped, independent branch succeeded", stt == {"extract": "SUCCESS", "broken": "FAILED", "after": "SKIPPED", "side": "SUCCESS"}, stt)
        check("the task table lists every task with its status", page.locator("[data-testid=run-task-row]").count() == 4 and "FAILED" in page.locator("[data-testid=run-task-table]").inner_text())
        rn("broken").click(); page.wait_for_timeout(300)
        check("clicking a node shows its error", "table_that_does_not_exist" in page.locator("[data-testid=run-task-log]").inner_text(), page.locator("[data-testid=run-task-log]").inner_text()[:200])
        page.locator("[data-testid=run-task-row]").nth(3).click(); page.wait_for_timeout(300)
        check("clicking a table row selects the same task on the graph", "selected" in (rn("side").get_attribute("class") or ""))
        page.screenshot(path="/tmp/dag_run_failed.png")
        check("the run offers Repair", page.locator("[data-testid=run-repair]").is_visible())
        ev("() => { const d = Alpine.$data(document.body); const j = d.jobs.find(x => x.id === 'failing'); }")
        # fix the broken task through the graph editor, save, repair
        page.locator("[data-testid=run-back]").click(); page.locator("button:has-text('Tasks (DAG Flow)')").click(); page.wait_for_timeout(500)
        node("broken").click(); page.locator("[data-testid=graph-task-query]").fill("select 1 as fixed"); page.click("[data-testid=graph-save]"); page.wait_for_timeout(1200)
        page.locator("button:has-text('Run History')").click(); page.wait_for_timeout(500)
        page.locator("tr:visible:has-text('FAILED')").first.click(); page.wait_for_selector("[data-testid=run-view]", state="visible")
        page.locator("[data-testid=run-repair]").click()
        page.wait_for_function("() => document.querySelector('[data-testid=run-status]') && document.querySelector('[data-testid=run-status]').innerText.trim() === 'SUCCESS'", timeout=30000)
        stt = {t: rn(t).get_attribute("data-status") for t in ("extract", "broken", "after", "side")}
        check("Repair re-runs only what failed; the new run is all green and says it repairs the earlier one", set(stt.values()) == {"SUCCESS"} and "Repair of" in page.locator("[data-testid=run-panel]").inner_text(), stt)
        rn("extract").click(); page.wait_for_timeout(300)
        check("a reused task is marked as such", "Reused from run" in page.locator("[data-testid=run-panel]").inner_text())
        n_opts = page.locator("[data-testid=run-select] option").count()
        page.locator("[data-testid=run-select]").select_option(index=n_opts - 1); page.wait_for_timeout(700)
        check("the run selector switches to another run of the workflow (the failed one)", page.locator("[data-testid=run-status]").inner_text().strip() == "FAILED" and rn("broken").get_attribute("data-status") == "FAILED")

        print("run page: live")
        pick("slowjob"); page.locator("button:has-text('Run Now'):visible").first.click(); page.wait_for_selector("[data-testid=run-view]", state="visible", timeout=8000)
        page.wait_for_function("() => { const n = document.querySelector('[data-testid=dag-run] [data-testid=dag-node][data-id=slow]'); return n && n.getAttribute('data-status') === 'RUNNING'; }", timeout=30000)
        stt = {t: rn(t).get_attribute("data-status") for t in ("first", "slow", "last")}
        check("while it runs: finished task green, the running task pulses, the rest is waiting", stt == {"first": "SUCCESS", "slow": "RUNNING", "last": "PENDING"} and page.locator("[data-testid=run-status]").inner_text().strip() == "RUNNING", stt)
        page.screenshot(path="/tmp/dag_run_live.png")
        check("a running run offers Cancel (and not Repair)", page.locator("[data-testid=run-cancel]").is_visible() and not page.locator("[data-testid=run-repair]").is_visible())
        page.locator("[data-testid=run-cancel]").click()
        page.wait_for_function("() => document.querySelector('[data-testid=run-status]').innerText.trim() === 'CANCELLED'", timeout=30000)
        stt = {t: rn(t).get_attribute("data-status") for t in ("first", "slow", "last")}
        check("after Cancel the run page shows the outcome", stt["first"] == "SUCCESS" and stt["slow"] == "CANCELLED" and stt["last"] == "CANCELLED", stt)
        page.locator("[data-testid=run-back]").click(); page.wait_for_timeout(300)
        check("'All runs' returns to the list", page.locator("tr:visible:has-text('CANCELLED')").first.is_visible() and not page.locator("[data-testid=run-view]").is_visible())

        print("list mode keeps the old run dialog")
        page.locator("button:has-text('Tasks (DAG Flow)')").click(); page.locator("[data-testid=jobview-list]").click(); page.wait_for_timeout(300)
        page.locator("button:has-text('Run History')").click(); page.locator("tr:visible:has-text('SUCCESS')").first.click() if page.locator("tr:visible:has-text('SUCCESS')").count() else page.locator("tbody tr:visible").first.click()
        page.wait_for_timeout(600)
        check("in list mode a run opens the classic dialog, not the run page", ev("() => Alpine.$data(document.body).showRunDetailModal") is True and not page.locator("[data-testid=run-view]").is_visible())
        errs = [e for e in errors if "dialog" not in e]
        check("no JS errors", not errs, errs)
        b.close()
finally:
    if FAIL: print(sh("docker", "logs", "--tail", "25", "dagui").stderr[-1500:])
    sh("docker", "rm", "-f", "dagui")
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
