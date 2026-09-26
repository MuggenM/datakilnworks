#!/usr/bin/env python3
"""OAuth 2.0 client credentials for HTTP connections (web/oauth_client.py, web/connections.py, web/autoloader_conn.py) against an in-process mock
authorization server + REST API in a throwaway warehouse:  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_oauth_connection.py"""
import base64, http.server, json, os, shutil, sys, tempfile, threading, time, urllib.parse
TMP = tempfile.mkdtemp(prefix="oauth_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
from web import connections, oauth_client, autoloader_conn
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)
SECRET = "s3cr3t-value-XYZ"
S = {"issued": 0, "tokens": {}, "lifetime": 3600, "token_type": "Bearer", "mode": "ok", "reqs": [], "api_calls": 0, "revoked": set(), "big": False}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body, headers=None):
        b = json.dumps(body).encode() if not isinstance(body, bytes) else body
        self.send_response(code); [self.send_header(k, v) for k, v in (headers or {}).items()]; self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        if self.path.startswith("/redirect"): return self._send(302, {}, {"Location": "http://127.0.0.1:1/steal"})
        n = int(self.headers.get("Content-Length") or 0); form = dict(urllib.parse.parse_qsl(self.rfile.read(n).decode()))
        S["reqs"].append({"path": self.path, "form": form, "auth": self.headers.get("Authorization")})
        if S["mode"] == "down": return self._send(503, {"error": "temporarily_unavailable"})
        cid, sec = form.get("client_id"), form.get("client_secret")
        if self.headers.get("Authorization", "").startswith("Basic "):
            u, _, p = base64.b64decode(self.headers["Authorization"][6:]).decode().partition(":"); cid, sec = urllib.parse.unquote(u), urllib.parse.unquote(p)
        if form.get("grant_type") != "client_credentials": return self._send(400, {"error": "unsupported_grant_type"})
        if cid != "my-client" or sec != SECRET: return self._send(401, {"error": "invalid_client", "error_description": f"Client authentication failed for {cid}"})
        if S["big"]: return self._send(200, b"x" * 200000)
        S["issued"] += 1; tok = f"tok{S['issued']}abc.def-ghi"; S["tokens"][tok] = time.time() + S["lifetime"]
        body = {"access_token": tok, "token_type": S["token_type"], "expires_in": S["lifetime"]}
        if S["mode"] == "badtoken": body["access_token"] = "bad token\r\nX-Evil: 1"
        if S["mode"] == "noexp": body.pop("expires_in")
        self._send(200, body)
    def do_GET(self):
        S["api_calls"] += 1; a = self.headers.get("Authorization", "")
        tok = a[7:] if a.startswith("Bearer ") else ""
        if S.get("reject") or tok not in S["tokens"] or tok in S["revoked"] or S["tokens"][tok] < time.time(): return self._send(401, {"error": "invalid_token"})
        if self.path.startswith("/api/records"): return self._send(200, {"data": [{"id": i, "name": f"n{i}"} for i in range(30)]})
        return self._send(200, {"ok": True})
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start(); B = f"http://127.0.0.1:{srv.server_address[1]}"
def cfg(**kw): return {"base_url": B + "/api/", "auth": "oauth2", "token_url": B + "/token", "client_id": "my-client", "allow_insecure": True, **kw}
def mk(name="oa", secret=SECRET, **kw): return connections.create_connection({"name": name, "type": "http", "config": cfg(**kw), "secret": {"client_secret": secret}}, "admin")
def bad(config, secret=None, frag=""):
    try: connections.create_connection({"name": "b" + str(abs(hash(json.dumps(config, sort_keys=True))))[:6], "type": "http", "config": config, "secret": secret if secret is not None else {"client_secret": "x"}}, "admin"); return False
    except connections.ConnectionError_ as e: return frag.lower() in str(e).lower()

print("validation")
check("the token URL is required and must be http(s) without credentials", bad({**cfg(), "token_url": ""}, frag="token URL") and bad({**cfg(), "token_url": "ftp://x/t"}, frag="token URL") and bad({**cfg(), "token_url": "https://u:p@x/t"}, frag="token URL"))
check("the client secret is sent only over https unless allowed", bad({**cfg(), "allow_insecure": False}, frag="plain http"))
check("client id and client secret are required", bad({**cfg(), "client_id": ""}, frag="client ID") and bad(cfg(), secret={}, frag="client secret"))
check("client authentication must be basic or body", bad({**cfg(), "client_auth": "jwt"}, frag="basic"))
check("extra token parameters cannot override the grant or the credentials", bad({**cfg(), "extra_params": {"grant_type": "password"}}, frag="not an allowed") and bad({**cfg(), "extra_params": {"client_secret": "x"}}, frag="not an allowed") and bad({**cfg(), "extra_params": {f"k{i}": "v" for i in range(11)}}, frag="10"))
check("a scope with control characters or quotes is refused", bad({**cfg(), "scope": 'a "b"'}, frag="scope"))
c = mk(scope="read write", extra_params={"audience": "https://api.example"})
check("a valid connection is stored; the secret is never returned", c["config"]["auth"] == "oauth2" and c["has_secret"] and SECRET not in json.dumps(connections.list_connections()) and c["config"]["extra_params"] == {"audience": "https://api.example"}, c)
check("the token URL, client id, scope and method are kept (client auth defaults to basic)", c["config"]["client_auth"] == "basic" and c["config"]["scope"] == "read write" and c["config"]["token_url"].endswith("/token"))
full = connections.get_with_secret("oa")

print("fetching and caching")
oauth_client.clear(); S["reqs"].clear(); S["issued"] = 0
t, ty = oauth_client.get_token(full); r0 = S["reqs"][0]
check("a token is fetched with the client-credentials grant, scope and extra parameters, client auth by Basic header", ty == "Bearer" and r0["form"]["grant_type"] == "client_credentials" and r0["form"]["scope"] == "read write" and r0["form"]["audience"] == "https://api.example"
      and r0["auth"].startswith("Basic ") and "client_secret" not in r0["form"] and "client_id" not in r0["form"], r0)
check("the token is cached: repeated use does not call the token endpoint", oauth_client.get_token(full)[0] == t and oauth_client.get_token(full)[0] == t and S["issued"] == 1)
check("a REST request carries it as a Bearer header", autoloader_conn._http(full, B + "/api/records").json()["data"][0]["id"] == 0 and S["issued"] == 1)
c2 = mk("oa_body", client_auth="body"); full2 = connections.get_with_secret("oa_body"); S["reqs"].clear(); oauth_client.get_token(full2)
check("client authentication in the body (client_id / client_secret form fields) works too", S["reqs"][0]["auth"] is None and S["reqs"][0]["form"]["client_id"] == "my-client" and S["reqs"][0]["form"]["client_secret"] == SECRET)
check("the cache is per connection settings (a second connection has its own token)", oauth_client.get_token(full2)[0] != t)
connections.update_connection("oa", {"secret": {"client_secret": "another"}}, "admin"); n = S["issued"]
try: oauth_client.get_token(connections.get_with_secret("oa")); ok = False
except oauth_client.OAuthError as e: ok = "invalid_client" in str(e)
check("a changed secret is never served from the old cache (and is judged by the server)", ok and S["issued"] == n)
connections.update_connection("oa", {"secret": {"client_secret": SECRET}}, "admin"); full = connections.get_with_secret("oa")

print("expiry and revocation")
oauth_client.clear(); S["lifetime"] = 3; S["issued"] = 0
t1 = oauth_client.get_token(full)[0]; time.sleep(0.2); check("a short-lived token is cached within its life", oauth_client.get_token(full)[0] == t1)
time.sleep(1.8); t2 = oauth_client.get_token(full)[0]; check("it is refreshed a little before it expires (half a short lifetime, at most 60 s)", t2 != t1 and S["issued"] == 2)
S["lifetime"] = 3600; oauth_client.clear(); S["mode"] = "noexp"; S["issued"] = 0; oauth_client.get_token(full); S["mode"] = "ok"
check("a missing expires_in falls back to a short default", list(oauth_client._cache.values())[0]["lifetime"] == oauth_client.DEFAULT_LIFETIME)
oauth_client.clear(); tk = oauth_client.get_token(full)[0]; S["revoked"].add(tk); before = S["issued"]
r = autoloader_conn._http(full, B + "/api/records")
check("a token the API rejects (401) is replaced once and the request is retried", r.status_code == 200 and S["issued"] == before + 1)
S["reject"] = True; n_issued = S["issued"]
try: autoloader_conn._http(full, B + "/api/records"); ok = False
except autoloader_conn.SourceError as e: ok = "refused the credentials" in str(e)
S["reject"] = False
check("if the fresh token is rejected too, it stops with a clear message after exactly one retry", ok and S["issued"] == n_issued + 1, (ok, S["issued"], n_issued))

print("failures and hardening")
def err(conn_name="oa", **patch):
    f = connections.get_with_secret(conn_name); f["config"] = {**f["config"], **patch}; oauth_client.clear()
    try: oauth_client.get_token(f); return None
    except oauth_client.OAuthError as e: return str(e)
oauth_client.clear(); f = connections.get_with_secret("oa"); f["secret"] = {"client_secret": "wrong-secret"}
try: oauth_client.get_token(f); e = None
except oauth_client.OAuthError as ex: e = str(ex)
check("wrong credentials: the server's error is shown, the secret never is", e and "invalid_client" in e and "wrong-secret" not in e and SECRET not in e, e)
S["mode"] = "down"; e = err(); check("a failing token endpoint gives a short message", e and "HTTP 503" in e, e); S["mode"] = "ok"
e = err(token_url="http://127.0.0.1:1/t"); check("an unreachable token endpoint fails fast and names no secret", e and "could not be reached" in e and SECRET not in e, e)
e = err(token_url=B + "/redirect"); check("a redirect from the token endpoint is not followed (the secret must not leave the URL)", e and "redirect" in e.lower(), e)
S["token_type"] = "MAC"; e = err(); check("a token type other than Bearer is refused", e and "not supported" in e, e); S["token_type"] = "Bearer"
S["mode"] = "badtoken"; e = err(); check("a token with spaces or line breaks (header injection) is refused", e and "unexpected format" in e, e); S["mode"] = "ok"
S["big"] = True; e = err(); check("an oversized answer is refused", e and ("too large" in e or "did not answer with JSON" in e), e); S["big"] = False
check("no error message ever contains the secret or a token", all(SECRET not in str(x) for x in [err(), err(token_url=B + "/x")] if x))

print("the connection in use")
oauth_client.clear(); S["issued"] = 0
r = autoloader_conn.test_connection({"type": "http", "config": full["config"], "secret": full["secret"]})
check("Test reports the token (and its lifetime) and the API answer", r["ok"] and r["message"].startswith("Token obtained (valid for 3600 s)") and "Reached" in r["message"], r)
r = autoloader_conn.test_connection({"type": "http", "config": full["config"], "secret": {"client_secret": "no"}}); check("Test with a wrong secret says so without the secret", r["ok"] is False and "invalid_client" in r["message"] and "no" != r["message"], r)
pv = autoloader_conn.preview("conn://oa/records", {"mode": "api", "records_path": "data", "pagination": {"type": "none"}}, "*", 5)
check("a REST API source is previewed through OAuth", [c["name"] for c in pv["columns"]] == ["id", "name"] and len(pv["rows"]) == 5, pv)
S["lifetime"] = 3; oauth_client.clear()
from web import autoloader
p = autoloader.create_pipeline({"name": "oauth api", "source_volume_path": "conn://oa/records", "source_options": {"mode": "api", "records_path": "data", "pagination": {"type": "none"}}, "target_table": "oauth_t", "ingest_mode": "overwrite", "file_pattern": "*"}, created_by="admin")
r1 = autoloader.run_pipeline_cycle(p["id"]); time.sleep(2.2); S["issued"] = 0; r2 = autoloader.run_pipeline_cycle(p["id"])
check("a pipeline polls the API and keeps working across token expiry (a new token is fetched by itself)", r1["files_ingested"] == 1 and r1["rows_ingested"] == 30 and (r2.get("files_found") is not None) and S["issued"] >= 1, (r1, r2))
srv.shutdown(); shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
