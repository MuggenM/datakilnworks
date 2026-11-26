#!/usr/bin/env python3
"""The Genie and SQL-copilot LLM calls must not block the studio's event loop. Before, `/api/genie/ask`,
`/api/genie/chats/{id}/ask` and `/api/sql/copilot/generate` called the LLM synchronously inside async routes, so one question
(up to 60 s per call) froze every other request for every user. Also covers the chat store, which those calls now write from
worker threads: concurrent questions in one chat keep every exchange, and concurrent writers never lose a chat.

Runs a real uvicorn server (a real event loop, unlike TestClient) on a throwaway WAREHOUSE_DIR, with a fake Ollama whose
/api/chat answers after DELAY seconds. Inside the image:  python scratch/test_llm_off_loop.py"""
import json, os, sys, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TMP = tempfile.mkdtemp(prefix="llmloop_")
os.environ["WAREHOUSE_DIR"] = os.path.join(TMP, "warehouse"); os.makedirs(os.environ["WAREHOUSE_DIR"])
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY", "IP_ALLOWLIST_OVERRIDE", "TRUSTED_PROXIES", "DKW_SECRETS_DIR", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"):
    os.environ.pop(v, None)
os.environ["LOGIN_THROTTLE"] = "off"
DELAY = float(os.getenv("LLM_DELAY", "6"))

class FakeOllama(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _json(self, obj):
        body = json.dumps(obj).encode(); self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        self._json({"models": [{"name": "fake-model"}]})
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0)); time.sleep(DELAY)
        plan = {"sql": "SELECT 42 AS answer", "explanation": "fake", "suggested_visualization": "table", "x_axis": "", "y_axis": "", "tables_used": []}
        self._json({"message": {"role": "assistant", "content": json.dumps(plan)}})

llm = ThreadingHTTPServer(("127.0.0.1", 0), FakeOllama); threading.Thread(target=llm.serve_forever, daemon=True).start()
os.environ["OLLAMA_HOST"] = f"http://127.0.0.1:{llm.server_address[1]}"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests, uvicorn
from web import app as app_module, auth, genie

FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)

with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
auth.create_user("alice", "alicepassword1", "Alice", role="user")
PORT = 18765; BASE = f"http://127.0.0.1:{PORT}"
server = uvicorn.Server(uvicorn.Config(app_module.app, host="127.0.0.1", port=PORT, log_level="warning"))
threading.Thread(target=server.run, daemon=True).start()
for _ in range(120):
    try:
        if requests.get(f"{BASE}/healthz", timeout=2).ok: break
    except Exception: pass
    time.sleep(0.5)
s = requests.Session()
check("alice signs in", s.post(f"{BASE}/api/auth/login", json={"username": "alice", "password": "alicepassword1"}).status_code == 200)

def while_running(call):
    """Runs `call` in a thread; meanwhile measures how long the studio takes to answer /healthz and /api/auth/me."""
    out = {}
    t = threading.Thread(target=lambda: out.__setitem__("r", call())); t0 = time.monotonic(); t.start()
    time.sleep(1.0)                                   # the request is inside the (slow) LLM call now
    worst = 0.0
    while t.is_alive():
        a = time.monotonic(); requests.get(f"{BASE}/healthz", timeout=60); s.get(f"{BASE}/api/auth/me", timeout=60)
        worst = max(worst, time.monotonic() - a); time.sleep(0.2)
    t.join()
    return out.get("r"), worst, time.monotonic() - t0

print(f"SQL copilot (fake LLM answers after {DELAY:.0f} s)")
r, worst, took = while_running(lambda: s.post(f"{BASE}/api/sql/copilot/generate", json={"prompt": "the answer", "provider": "ollama", "model": "fake-model"}, timeout=120))
check("the copilot request really waited for the LLM", took >= DELAY, took)
check("the studio kept answering meanwhile (worst /healthz + /api/auth/me < 1 s)", worst < 1.0, f"worst {worst:.2f}s")
body = r.json() if r is not None and r.ok else {}
check("the copilot returns the LLM's SQL", "SELECT 42" in json.dumps(body), (r.status_code if r is not None else None, body))

print("Genie")
chat = s.post(f"{BASE}/api/genie/chats", json={"title": "New Exploration"}).json()
r, worst, took = while_running(lambda: s.post(f"{BASE}/api/genie/chats/{chat['id']}/ask", json={"prompt": "what is the answer", "provider": "ollama", "model": "fake-model"}, timeout=120))
check("the Genie request really waited for the LLM", took >= DELAY, took)
check("the studio kept answering meanwhile (worst /healthz + /api/auth/me < 1 s)", worst < 1.0, f"worst {worst:.2f}s")
am = (r.json() if r is not None and r.ok else {}).get("assistant_message", {})
check("Genie ran the LLM's SQL as alice and returned the row", "42" in json.dumps(am), (r.status_code if r is not None else None, am))
r, worst, took = while_running(lambda: s.post(f"{BASE}/api/genie/ask", json={"prompt": "quick question", "provider": "ollama", "model": "fake-model"}, timeout=120))
check("the quick-ask route is off the loop too", r is not None and r.ok and worst < 1.0, (getattr(r, "status_code", None), f"worst {worst:.2f}s"))

print("two questions in one chat at the same time")
chat2 = s.post(f"{BASE}/api/genie/chats", json={"title": "New Exploration"}).json()
res = []
ts = [threading.Thread(target=lambda q=q: res.append(s.post(f"{BASE}/api/genie/chats/{chat2['id']}/ask", json={"prompt": q, "provider": "ollama", "model": "fake-model"}, timeout=120))) for q in ("first question", "second question")]
t0 = time.monotonic(); [t.start() for t in ts]; [t.join() for t in ts]; took = time.monotonic() - t0
check("both answered, in parallel (not one after the other)", all(x.ok for x in res) and took < 2 * DELAY, f"{took:.1f}s")
msgs = s.get(f"{BASE}/api/genie/chats/{chat2['id']}").json().get("messages", [])
prompts = sorted(m.get("content", "") for m in msgs if m.get("role") == "user")
check("the chat keeps BOTH exchanges (the later save no longer overwrites the earlier one)", len(msgs) == 4 and prompts == ["first question", "second question"], [m.get("content", "")[:30] for m in msgs])

print("chat store under concurrent writers")
before = {c["id"] for c in genie.load_chats()}
made = []
ts = [threading.Thread(target=lambda: made.append(genie.create_chat(title="New Exploration", user="alice")["id"])) for _ in range(20)]
[t.start() for t in ts]; [t.join() for t in ts]
after = {c["id"] for c in genie.load_chats()}
check("20 chats created at once by 20 threads are all stored, none of the earlier ones lost", set(made) <= after and before <= after and len(made) == 20, (len(made), len(after - before)))
check("no temporary files are left next to the chat store", not [f for f in os.listdir(os.path.dirname(genie.CHATS_FILE)) if f.startswith(os.path.basename(genie.CHATS_FILE)) and f.endswith(".tmp")])

server.should_exit = True; llm.shutdown()
print("\nALL PASS" if not FAIL else f"\n{len(FAIL)} FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
