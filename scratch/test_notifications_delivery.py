#!/usr/bin/env python3
"""Mail and webhook delivery for real: web/email_reports.py against Mailpit (CI services stack, profile `mail`: a real SMTP
server whose HTTP API shows what arrived), web/slack_integration.py and web/webhook_alerts.py against an in-process HTTP
receiver on 127.0.0.1 that records every request (method, headers, body).
    CI_SERVICES=mail ci/run_test.sh notifications_delivery
Checks: a message with an attachment arrives with its subject, recipient and attachment name; a wrong port is a clean
error result; Slack, Teams and generic webhooks each POST one JSON body (Teams in MessageCard shape, `facts` a real list)
and carry no cookie, Authorization or API-key header when none is configured. SMTP_HOST has no default."""
import http.server
import json
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _services  # noqa: E402

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "1025"))
API = os.environ.get("MAILPIT_API", f"http://{SMTP_HOST}:8025/api/v1").rstrip("/")
if not _services.require_or_skip("Mailpit (SMTP)", SMTP_HOST, SMTP_PORT):
    sys.exit(0)

TMP = tempfile.mkdtemp(prefix="notif_")
os.environ["WAREHOUSE_DIR"] = TMP; os.makedirs(os.path.join(TMP, ".metadata"))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import requests  # noqa: E402
from web import email_reports, slack_integration, webhook_alerts  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


print("mail through Mailpit")
requests.delete(f"{API}/messages", timeout=10)                       # an empty mailbox
email_reports.update_email_config(smtp_server=SMTP_HOST, smtp_port=SMTP_PORT, use_tls=False, sender_email="studio@example.org",
                                  sender_password="any-login-works-here", sender_name="DKW CI", enabled=True)
att = os.path.join(TMP, "report.csv")
open(att, "w").write("id,amount\n1,10\n2,20\n")
r = email_reports.send_email(["analyst@example.org"], "Weekly kiln report", "<p>Hello <b>analyst</b></p>", "Hello analyst", attachments=[att])
check("send_email reports success", r.get("success") is True, r)
msgs = []
for _ in range(20):
    msgs = requests.get(f"{API}/messages", timeout=10).json().get("messages") or []
    if msgs:
        break
    time.sleep(0.5)
check("exactly one message arrived", len(msgs) == 1, msgs)
if msgs:
    m = msgs[0]
    check("with its subject", m.get("Subject") == "Weekly kiln report", m.get("Subject"))
    check("to the recipient", [t.get("Address") for t in m.get("To") or []] == ["analyst@example.org"], m.get("To"))
    full = requests.get(f"{API}/message/{m['ID']}", timeout=10).json()
    names = [a.get("FileName") for a in full.get("Attachments") or []]
    check("with the attachment by its file name", names == ["report.csv"], names)
    check("the HTML body is the one sent", "<b>analyst</b>" in (full.get("HTML") or ""), (full.get("HTML") or "")[:200])

email_reports.update_email_config(smtp_port=1)                        # a port on the mail host where nothing listens
t0 = time.time()
try:
    r = email_reports.send_email(["analyst@example.org"], "never", "<p>x</p>")
    raised = None
except Exception as e:   # noqa: BLE001
    r, raised = {}, e
check("a wrong port is an error result, not an exception", raised is None and r.get("success") is False and r.get("error"), (raised, r))
check("...and quickly (refused, no hang)", time.time() - t0 < 30, time.time() - t0)
check("...and nothing more arrived", len(requests.get(f"{API}/messages", timeout=10).json().get("messages") or []) == 1)

print("webhooks to a local receiver")
GOT = []


class Receiver(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        GOT.append({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
        self.send_response(200); self.send_header("Content-Type", "text/plain"); self.end_headers(); self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{srv.server_address[1]}"


def only_post(path):
    hits = [g for g in GOT if g["path"] == path]
    return hits[0] if len(hits) == 1 else None, len(hits)


def clean_headers(h):
    return not any(k in h for k in ("cookie", "authorization", "x-api-key"))


cfg = slack_integration.load_config(); cfg["enabled"] = True; slack_integration.save_config(cfg)
w = slack_integration.add_webhook("ci", f"{BASE}/slack", channel="#alerts")["webhook"]
r = slack_integration.send_notification("Pipeline *orders* finished", webhook_id=w["id"], title="Auto Loader", fields=[{"title": "rows", "value": "42"}])
hit, n = only_post("/slack")
check("slack: success reported", r.get("success") is True, r)
check("slack: one POST arrived", n == 1, n)
if hit:
    body = json.loads(hit["body"])
    check("slack: JSON with Block Kit blocks (header, the message, the fields)",
          hit["headers"].get("content-type", "").startswith("application/json")
          and [b["type"] for b in body["blocks"]] == ["header", "section", "section"] and body["blocks"][1]["text"]["text"] == "Pipeline *orders* finished", body)
    check("slack: no cookie, Authorization or API-key header", clean_headers(hit["headers"]), hit["headers"])

wcfg = webhook_alerts.load_config(); wcfg.update({"enabled": True, "max_retries": 1, "retry_delay": 0, "timeout": 10}); webhook_alerts.save_config(wcfg)
teams = webhook_alerts.add_webhook("teams", f"{BASE}/teams", webhook_type="teams")["webhook"]
generic = webhook_alerts.add_webhook("generic", f"{BASE}/generic", webhook_type="generic")["webhook"]
r1 = webhook_alerts.send_webhook(teams["id"], "Job failed", "Task load_orders failed", data={"job": "nightly", "attempt": 2}, severity="error")
r2 = webhook_alerts.send_webhook(generic["id"], "Job failed", "Task load_orders failed", data={"job": "nightly"}, severity="error")
check("teams and generic: success reported", r1.get("success") is True and r2.get("success") is True, (r1, r2))
hit, n = only_post("/teams")
check("teams: one POST arrived", n == 1, n)
if hit:
    card = json.loads(hit["body"])
    sec = (card.get("sections") or [{}])[0]
    check("teams: a MessageCard (@type, @context, summary, a section with title and text)",
          card.get("@type") == "MessageCard" and card.get("@context") == "http://schema.org/extensions" and card.get("summary") == "Job failed"
          and sec.get("activityTitle") == "Job failed" and sec.get("text") == "Task load_orders failed", card)
    check("teams: facts is a list of {name, value} (a JSON string inside the card shows nothing in Teams)",
          sec.get("facts") == [{"name": "job", "value": "nightly"}, {"name": "attempt", "value": "2"}], sec.get("facts"))
    check("teams: no cookie, Authorization or API-key header", clean_headers(hit["headers"]), hit["headers"])
hit, n = only_post("/generic")
check("generic: one POST arrived", n == 1, n)
if hit:
    body = json.loads(hit["body"])
    check("generic: JSON with title, message and timestamp", hit["headers"].get("content-type", "").startswith("application/json")
          and body.get("title") == "Job failed" and body.get("message") == "Task load_orders failed" and body.get("timestamp"), body)
    check("generic: no cookie, Authorization or API-key header", clean_headers(hit["headers"]), hit["headers"])
srv.shutdown()

print()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.stdout.flush()
os._exit(1 if FAIL else 0)
