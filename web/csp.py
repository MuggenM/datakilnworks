"""
Content-Security-Policy for the studio's own HTML (applied by `_security_headers` in app.py to every text/html response that does not
set its own; web/apps.py and web/dashboard_apps.py keep theirs).

What it does, and what it cannot do yet:
  * `script-src` has a per-response nonce and no 'unsafe-inline': an injected <script>, an inline event handler (<img onerror=...>,
    the classic payload of an HTML injection through one of the page's x-html / innerHTML sites) or a javascript: URL does not run,
    and scripts load only from this origin (plus cdnjs for the Monaco editor on the main page).
  * `connect-src 'self'`, `form-action 'self'`, `base-uri 'self'`, `object-src 'none'`, `frame-ancestors 'self'`: script that does run
    cannot send data to another host with fetch/XHR/WebSocket or a form post, cannot re-point relative URLs, cannot load plugins, and
    the page cannot be framed by another site.
  * It keeps 'unsafe-eval', because the page uses Alpine.js's standard build, which compiles every x-on / x-bind / x-text expression
    with `new Function`. So the policy does NOT stop an injection of Alpine directives (`<div x-init="...">` inside HTML that an x-html
    site inserts: Alpine initialises inserted content): escaping the inserted HTML stays the defence there. Removing 'unsafe-eval'
    needs Alpine's CSP build, which accepts only property names / method calls in directives, i.e. rewriting the inline expressions.
  * `style-src 'unsafe-inline'`: the Tailwind runtime and Alpine `:style` bindings write styles at run time.
  * `img-src` allows any https: image (markdown cells and dashboards show external images); that is a narrow outbound channel.

CSP_MODE=enforce (default) | report (Content-Security-Policy-Report-Only: nothing is blocked, violations are only reported) | off.
Violations are reported to POST /api/csp-report (logged, rate-limited, counted in memory).
"""

import os
import secrets
import threading
import time
from typing import Dict, Optional

MONACO_CDN = "https://cdnjs.cloudflare.com"
REPORT_PATH = "/api/csp-report"


def mode() -> str:
    m = (os.getenv("CSP_MODE") or "enforce").strip().lower()
    return m if m in ("enforce", "report", "off") else "enforce"


def new_nonce() -> str:
    return secrets.token_urlsafe(18)


def _join(d: Dict[str, str]) -> str:
    return "; ".join(f"{k} {v}".strip() for k, v in d.items())


def _base() -> Dict[str, str]:
    return {
        "default-src": "'self'",
        "base-uri": "'self'",
        "object-src": "'none'",
        "frame-ancestors": "'self'",
        "form-action": "'self'",
        "img-src": "'self' data: blob: https:",
        "font-src": "'self' data:",
        "connect-src": "'self'",
        "worker-src": "'self' blob:",
        "frame-src": "'self'",
        "manifest-src": "'self'",
    }


def policy_for(path: str, nonce: str) -> str:
    """The policy for an HTML response at `path` (the nonce is the one the template used)."""
    p = _base()
    if path in ("/api/docs", "/api/redoc") or path.startswith("/api/docs/"):
        # FastAPI's Swagger UI / ReDoc pages: assets from jsDelivr and one inline bootstrap script that carries no nonce.
        p.update({"script-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net", "style-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com",
                  "font-src": "'self' data: https://fonts.gstatic.com", "worker-src": "'self' blob:"})
    elif path == "/" or path.startswith("/share/"):
        p.update({"script-src": f"'self' 'nonce-{nonce}' 'unsafe-eval' {MONACO_CDN}", "style-src": f"'self' 'unsafe-inline' {MONACO_CDN}",
                  "font-src": f"'self' data: {MONACO_CDN}"})
    elif path.startswith("/embed/"):
        p.update({"script-src": f"'self' 'nonce-{nonce}' 'unsafe-eval'", "style-src": "'self' 'unsafe-inline'"})
    elif path.startswith("/docs"):
        p.update({"script-src": "'self'", "style-src": "'self' 'unsafe-inline'"})
    else:
        # Any other HTML (error pages, anything served as text/html in the future): no script at all.
        p.update({"script-src": "'none'", "style-src": "'self' 'unsafe-inline'"})
    p["report-uri"] = REPORT_PATH
    return _join(p)


def header_name() -> Optional[str]:
    m = mode()
    return None if m == "off" else ("Content-Security-Policy-Report-Only" if m == "report" else "Content-Security-Policy")


# ---------------------------------------------------------------- violation reports

_lock = threading.Lock()
_counts: Dict[str, int] = {}
_recent: list = []
_window = {"start": 0.0, "logged": 0}
MAX_LOG_PER_MINUTE = 20
MAX_RECENT = 50


def record(report: dict) -> bool:
    """Counts a violation report (keyed by directive + blocked source). True when it should also be logged (rate limit)."""
    directive = str(report.get("effective-directive") or report.get("violated-directive") or report.get("effectiveDirective") or "?")[:80]
    blocked = str(report.get("blocked-uri") or report.get("blockedURL") or "?")[:200]
    doc = str(report.get("document-uri") or report.get("documentURL") or "")[:200]
    key = f"{directive} {blocked}"
    now = time.time()
    with _lock:
        _counts[key] = _counts.get(key, 0) + 1
        if len(_counts) > 500:
            _counts.clear()
        _recent.append({"at": int(now), "directive": directive, "blocked": blocked, "document": doc,
                        "sample": str(report.get("script-sample") or report.get("sample") or "")[:80]})
        del _recent[:-MAX_RECENT]
        if now - _window["start"] > 60:
            _window.update(start=now, logged=0)
        _window["logged"] += 1
        return _window["logged"] <= MAX_LOG_PER_MINUTE


def stats() -> dict:
    with _lock:
        return {"mode": mode(), "counts": dict(sorted(_counts.items(), key=lambda kv: -kv[1])[:50]), "recent": list(_recent)}
