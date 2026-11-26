"""A tiny OAuth2 client-credentials server + Bearer-protected JSON API for UI tests (client my-client / secret s3cr3t-value-XYZ). Usage: python oauth_mock_server.py PORT"""
import base64, http.server, json, sys, time, urllib.parse
TOK = {}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body):
        b = json.dumps(body).encode(); self.send_response(code); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0); form = dict(urllib.parse.parse_qsl(self.rfile.read(n).decode()))
        cid, sec = form.get("client_id"), form.get("client_secret")
        if self.headers.get("Authorization", "").startswith("Basic "):
            u, _, p = base64.b64decode(self.headers["Authorization"][6:]).decode().partition(":"); cid, sec = urllib.parse.unquote(u), urllib.parse.unquote(p)
        if cid != "my-client" or sec != "s3cr3t-value-XYZ": return self._send(401, {"error": "invalid_client", "error_description": "bad client"})
        t = f"t{len(TOK) + 1}.mock-token"; TOK[t] = time.time() + 3600; self._send(200, {"access_token": t, "token_type": "Bearer", "expires_in": 3600, "scope": form.get("scope", "")})
    def do_GET(self):
        a = self.headers.get("Authorization", ""); t = a[7:] if a.startswith("Bearer ") else ""
        if t not in TOK: return self._send(401, {"error": "invalid_token"})
        self._send(200, {"data": [{"id": i} for i in range(5)]})
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
