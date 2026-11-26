#!/usr/bin/env python3
"""Connections + Auto-Loader connection sources (web/connections.py, web/autoloader_conn.py). Throwaway WAREHOUSE_DIR; the HTTP side is an
in-process server with counters, the SFTP side is a real OpenSSH server (the atmoz/sftp image) on a throwaway docker network.
Run (see the docstring of scratch/run_connections_test.sh in this repo's notes): the studio image + `pip install paramiko`, on the
network of a container named `gtestsftp` (user/pass `user`/`pass`, upload dir /upload, a public key mounted for key auth):
  docker run --rm --network gtest_net -e SFTP_HOST=gtestsftp -e SFTP_KEY=/keys/id -v $PWD/web:/workspace/web \
     -v $PWD/scratch:/workspace/scratch -v /tmp/gs:/keys localspark-lakehouse-notebook \
     sh -c 'pip install -q paramiko && python /workspace/scratch/test_connections.py'"""
import base64, hashlib, http.server, json, os, shutil, sys, tempfile, threading, time
from urllib.parse import parse_qs, urlparse
TMP = tempfile.mkdtemp(prefix="conn_test_"); os.environ["WAREHOUSE_DIR"] = TMP
sys.path.insert(0, "/workspace")
from deltalake import DeltaTable
from web import autoloader, autoloader_conn, connections
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def rows(t, cat="dbo"):
    p = os.path.join(TMP, cat, t)
    return DeltaTable(p).to_pyarrow_table() if os.path.isdir(os.path.join(p, "_delta_log")) else None
def n(t): r = rows(t); return r.num_rows if r is not None else 0

# ------------------------------------------------------------------ the HTTP server under test
STATE = {"csv": "id,v\n1,a\n2,b\n3,c\n", "etag": '"v1"', "counts": {}, "api": [{"id": i, "name": f"n{i}"} for i in range(1, 6)]}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _count(self): STATE["counts"][(self.command, urlparse(self.path).path)] = STATE["counts"].get((self.command, urlparse(self.path).path), 0) + 1
    def _send(self, code, body=b"", ctype="text/plain", extra=None):
        self.send_response(code)
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body))); self.end_headers()
        if self.command != "HEAD": self.wfile.write(body)
    def _auth(self):
        if self.headers.get("Authorization") != "Bearer s3cret-token":
            self._send(401, b"no"); return False
        return True
    def do_HEAD(self): self._route()
    def do_GET(self): self._route()
    def _route(self):
        self._count(); u = urlparse(self.path); q = parse_qs(u.query); p = u.path
        if p.startswith("/basic/"):
            if self.headers.get("Authorization") != "Basic " + base64.b64encode(b"bob:pw").decode(): return self._send(401)
            return self._send(200, STATE["csv"].encode(), "text/csv")
        if p == "/open/data.csv": return self._send(200, STATE["csv"].encode(), "text/csv")
        if not self._auth(): return
        if p == "/files/data.csv": return self._send(200, STATE["csv"].encode(), "text/csv", {"ETag": STATE["etag"]})
        if p == "/files/noetag.csv":
            if self.command == "HEAD": return self._send(405)
            return self._send(200, STATE["csv"].encode(), "text/csv")
        if p == "/files/blob": return self._send(200, STATE["csv"].encode(), "text/csv; charset=utf-8", {"ETag": '"b"'})
        if p == "/files/junk.parquet": return self._send(200, b"not a parquet file", "application/octet-stream", {"ETag": '"j"'})
        if p == "/files/big.csv": return self._send(200, b"x" * 5000, "text/csv", {"ETag": '"big"'})
        if p == "/redir-same": return self._send(302, extra={"Location": "/files/data.csv"})
        if p == "/redir-away": return self._send(302, extra={"Location": "http://evil.example/files/data.csv"})
        if p == "/api/items":
            page, size = int(q.get("page", ["1"])[0]), int(q.get("per_page", ["2"])[0])
            items = STATE["api"][(page - 1) * size: page * size]
            return self._send(200, json.dumps({"data": {"items": items}}).encode(), "application/json")
        if p == "/api/linked":
            start = int(q.get("start", ["0"])[0]); items = STATE["api"][start:start + 2]
            body = {"results": items, "next": f"/api/linked?start={start + 2}" if start + 2 < len(STATE["api"]) else None}
            return self._send(200, json.dumps(body).encode(), "application/json")
        if p == "/api/typed":
            return self._send(200, json.dumps([{"id": 1, "when": "2026-01-01T10:00:00", "amount": 1.5, "tags": ["a", "b"], "meta": {"k": "v"}}]).encode(), "application/json")
        if p == "/api/empty": return self._send(200, json.dumps({"data": {"items": []}}).encode(), "application/json")
        if p == "/api/flat": return self._send(200, json.dumps(STATE["api"]).encode(), "application/json")
        if p == "/api/nopath": return self._send(200, json.dumps({"other": []}).encode(), "application/json")
        self._send(404, b"nf")
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H); PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{PORT}"
SRC = os.path.join(TMP, "unused")

def http_conn(name, **kw):
    cfg = {"base_url": BASE, "auth": "bearer", "allow_insecure": True}; cfg.update(kw.pop("config", {}))
    return connections.create_connection({"name": name, "type": "http", "config": cfg, "secret": kw.pop("secret", {"token": "s3cret-token"})}, "admin")
def mk(name, path, table, **kw):
    return autoloader.create_pipeline({"name": name, "source_volume_path": path, "target_table": table, **kw})
def run(p): return autoloader.run_pipeline_cycle(p["id"])
def last_error(p): return autoloader.get_pipeline(p["id"])["last_error"] or ""

# ------------------------------------------------------------------ store
print("connections store")
c = http_conn("api")
check("secret is not in the public view", "s3cret-token" not in json.dumps(c) and c["has_secret"])
raw = open(os.path.join(TMP, ".metadata", "connections.db"), "rb").read()
check("secret is not in the database file in plaintext", b"s3cret-token" not in raw)
check("audited without the secret", "s3cret-token" not in json.dumps(list(__import__("sqlite3").connect(os.path.join(TMP, ".metadata", "governance.db")).execute("select detail from governance_audit where action like 'CONNECTION_%'"))))
def bad(**data):
    try: connections.create_connection(data, "admin"); return None
    except connections.ConnectionError_ as e: return str(e)
check("credentials over plain http are refused unless allowed", "plain http" in (bad(name="x1", type="http", config={"base_url": "http://h/", "auth": "bearer"}, secret={"token": "t"}) or ""))
check("URL with embedded credentials refused", bad(name="x2", type="http", config={"base_url": "https://u:p@h/"}) is not None)
check("bad name refused", bad(name="Bad Name", type="http", config={"base_url": "https://h/"}) is not None)
check("duplicate name refused", "already exists" in (bad(name="api", type="http", config={"base_url": "https://h/"}) or ""))
check("bearer needs a token", "token is required" in (bad(name="x3", type="http", config={"base_url": "https://h/", "auth": "bearer"}) or ""))
check("sftp needs a pinned fingerprint", "fingerprint" in (bad(name="x4", type="sftp", config={"host": "h", "username": "u", "auth": "password"}, secret={"password": "p"}) or ""))
u = connections.update_connection("api", {"description": "d", "config": {"timeout_seconds": 5}}, "admin")
check("update keeps the stored secret", u["has_secret"] and connections.get_with_secret("api")["secret"]["token"] == "s3cret-token" and u["config"]["timeout_seconds"] == 5)
check("connection test reaches the server", autoloader_conn.test_connection(connections.definition_for_test({"id": "api"}))["ok"])

# ------------------------------------------------------------------ path safety
print("path safety")
for evil in ("conn://api/../etc", "conn://api/http://evil/x", "conn://api/a\\b", "conn://api/x#f"):
    try: mk("evil", evil, "e", source_options={"mode": "file"}); ok = False
    except ValueError: ok = True
    check(f"refused: {evil}", ok)
try: mk("nofile", "conn://api/", "e", source_options={"mode": "file"}); ok = False
except ValueError: ok = True
check("file mode needs a path", ok)
try: mk("nocon", "conn://ghost/x.csv", "e", source_options={"mode": "file"}); ok = False
except ValueError as e: ok = "does not exist" in str(e)
check("unknown connection refused at creation", ok)
try: mk("w", "conn://api/files/data.csv", "e", source_options={"mode": "file"}, watch_enabled=True); ok = False
except ValueError: ok = True
check("file events refused for a connection source", ok)

# ------------------------------------------------------------------ http files
print("http files")
p = mk("csv", "conn://api/files/data.csv", "t_csv", source_options={"mode": "file"})
r = run(p); check("first cycle loads the file", r["files_ingested"] == 1 and n("t_csv") == 3, r)
gets = STATE["counts"].get(("GET", "/files/data.csv"), 0)
r = run(p); check("unchanged ETag: nothing loaded and no download (HEAD only)", r["files_ingested"] == 0 and STATE["counts"].get(("GET", "/files/data.csv"), 0) == gets, r)
STATE["csv"] += "4,d\n"; STATE["etag"] = '"v2"'
r = run(p); check("changed file is loaded again", r["files_ingested"] == 1 and n("t_csv") == 7, (r, n("t_csv")))
p2 = mk("noetag", "conn://api/files/noetag.csv", "t_noetag", source_options={"mode": "file"})
r = run(p2); r2 = run(p2); check("no validators: identity is the content hash (loaded once, re-downloaded)", r["files_ingested"] == 1 and r2["files_ingested"] == 0 and n("t_noetag") == 4, (r, r2))
p3 = mk("blob", "conn://api/files/blob", "t_blob", source_options={"mode": "file"})
check("format from the Content-Type when the URL has no extension", run(p3)["files_ingested"] == 1 and n("t_blob") == 4)
p4 = mk("junk", "conn://api/files/junk.parquet", "t_junk", source_options={"mode": "file"})
r = run(p4); check("a corrupt download is quarantined locally, not retried", r["files_quarantined"] == 1 and run(p4)["files_quarantined"] == 0, r)
check("...and the quarantined copy is kept for inspection", any(f.endswith(".bad") for f in os.listdir(os.path.join(TMP, ".metadata", "autoloader_staging", p4["id"], "_quarantine"))))
p5 = mk("same redirect", "conn://api/redir-same", "t_r1", source_options={"mode": "file"})
r = run(p5); check("same-origin redirect followed (format via content type)", r.get("files_ingested") == 1 or "format" in last_error(p5), (r, last_error(p5)))
p6 = mk("away", "conn://api/redir-away", "t_r2", source_options={"mode": "file"})
run(p6); check("a redirect to another host is not followed", "another host" in last_error(p6), last_error(p6))
c_bad = http_conn("badtoken", secret={"token": "WRONG-TOKEN-VALUE"})
p7 = mk("badauth", "conn://badtoken/files/data.csv", "t_bad", source_options={"mode": "file"})
run(p7); e = last_error(p7)
check("refused credentials surface as an error, without the secret", "refused the credentials" in e and "WRONG-TOKEN-VALUE" not in e, e)
check("status is ERROR (retried), not quarantined", autoloader.get_pipeline(p7["id"])["status"] == "ERROR")
c_basic = connections.create_connection({"name": "basic", "type": "http", "config": {"base_url": BASE, "auth": "basic", "username": "bob", "allow_insecure": True}, "secret": {"password": "pw"}}, "admin")
p8 = mk("basic", "conn://basic/basic/x.csv", "t_basic", source_options={"mode": "file"}); check("basic authentication", run(p8)["files_ingested"] == 1 and n("t_basic") == 4)
c_open = connections.create_connection({"name": "open", "type": "http", "config": {"base_url": BASE}}, "admin")
p9 = mk("open", "conn://open/open/data.csv", "t_open", source_options={"mode": "file"}); check("no authentication", run(p9)["files_ingested"] == 1)
saved = autoloader_conn.MAX_BYTES; autoloader_conn.MAX_BYTES = 1000
p10 = mk("big", "conn://api/files/big.csv", "t_big", source_options={"mode": "file"}); run(p10)
check("download over the size cap is refused", "larger than" in last_error(p10) or "MB" in last_error(p10), last_error(p10)); autoloader_conn.MAX_BYTES = saved

# ------------------------------------------------------------------ REST
print("rest / json")
api = {"mode": "api", "records_path": "data.items", "pagination": {"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 2}}
pa = mk("rest page", "conn://api/api/items", "t_rest", source_options=api, ingest_mode="merge", merge_keys="id")
r = run(pa); check("paged API: every page is fetched and loaded", r["files_ingested"] == 1 and n("t_rest") == 5, (r, last_error(pa)))
r = run(pa); check("unchanged API response is skipped (snapshot hash)", r["files_ingested"] == 0, r)
STATE["api"][0]["name"] = "changed"; STATE["api"].append({"id": 6, "name": "n6"})
r = run(pa); t = rows("t_rest").to_pydict()
check("changed snapshot is merged (upsert on id)", r["files_ingested"] == 1 and sorted(t["id"]) == [1, 2, 3, 4, 5, 6] and t["name"][t["id"].index(1)] == "changed", (r, t))
pl = mk("rest link", "conn://api/api/linked", "t_link", source_options={"mode": "api", "records_path": "results", "pagination": {"type": "next_link", "next_path": "next"}})
check("next-link pagination follows relative links", run(pl)["files_ingested"] == 1 and n("t_link") == 6, last_error(pl))
pf = mk("rest flat", "conn://api/api/flat", "t_flat", source_options={"mode": "api"}); check("a root-level JSON array", run(pf)["files_ingested"] == 1 and n("t_flat") == 6, last_error(pf))
pn = mk("rest nopath", "conn://api/api/nopath", "t_np", source_options={"mode": "api", "records_path": "data.items"}); run(pn)
check("a wrong records path is reported", "records path" in last_error(pn), last_error(pn))
for bad_opt in ({"mode": "api", "pagination": {"type": "next_link"}}, {"mode": "api", "pagination": {"type": "bogus"}}, {"mode": "api", "records_path": "a b"}, {"mode": "ftp"}):
    try: mk("bo", "conn://api/api/items", "bo", source_options=bad_opt); ok = False
    except ValueError: ok = True
    check(f"invalid options refused: {list(bad_opt.items())[-1]}", ok)
try: connections.delete_connection("api", "admin"); ok = False
except connections.ConnectionError_ as e: ok = "Used by pipeline" in str(e)
check("delete refused while a pipeline uses the connection (real)", ok)
lin = autoloader._volume_lineage_id("conn://api/api/items"); check("lineage node id for a connection source", lin == "volume:conn://api/api/items")

# ------------------------------------------------------------------ preview
print("preview (before any pipeline exists)")
from web import autoloader_conn as ac
hist_before = len(autoloader.list_pipelines())
r = ac.preview("conn://api/api/items", {"mode": "api", "records_path": "data.items", "pagination": {"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 2}})
check("API: the first page's records with inferred columns", r["ok"] and [c["name"] for c in r["columns"]] == ["id", "name"] and len(r["rows"]) == 2 and r["source"] == "api", r)
check("API: it says it is only the first page (the pipeline fetches all pages)", any("First page only" in n for n in r["notes"]), r["notes"])
check("API: only one page was requested", STATE["counts"].get(("GET", "/api/items"), 0) > 0)
n_before = STATE["counts"].get(("GET", "/api/items"), 0)
ac.preview("conn://api/api/items", {"mode": "api", "records_path": "data.items", "pagination": {"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 2}})
check("...exactly one request per preview", STATE["counts"].get(("GET", "/api/items"), 0) - n_before == 1)
try: ac.preview("conn://api/api/nopath", {"mode": "api", "records_path": "data.items"}); ok = False
except ac.SourceError as e: ok = "records path" in str(e)
check("API: a wrong records path is reported", ok)
try: ac.preview("conn://api/api/empty", {"mode": "api", "records_path": "data.items"}); ok = False
except ac.SourceError as e: ok = "no records" in str(e)
check("API: an empty first page is reported", ok)
r = ac.preview("conn://api/api/typed", {"mode": "api"})
check("API: dates, decimals, lists and structs survive as JSON", json.dumps(r) and r["rows"][0][3] == ["a", "b"] and r["rows"][0][4] == {"k": "v"} and isinstance(r["rows"][0][1], str), r["rows"])
STATE["csv"] = "id,v\n" + "\n".join(f"{i},x{i}" for i in range(25)) + "\n"; STATE["etag"] = '"v9"'
r = ac.preview("conn://api/files/data.csv", {"mode": "file"})
check("file: shows the first 10 rows and says there are more", len(r["rows"]) == 10 and r["truncated"] and [c["name"] for c in r["columns"]] == ["id", "v"], r)
r = ac.preview("conn://api/files/data.csv", {"mode": "file"}, limit=3); check("file: the limit is honoured", len(r["rows"]) == 3 and r["truncated"])
check("file: types are inferred", dict((c["name"], c["type"]) for c in r["columns"])["id"].startswith("int"), r["columns"])
try: ac.preview("conn://api/files/junk.parquet", {"mode": "file"}); ok = False
except ac.SourceError as e: ok = "could not be read" in str(e)
check("file: unreadable content is explained", ok)
saved_cap = ac.PREVIEW_BYTES; ac.PREVIEW_BYTES = 1000
try: ac.preview("conn://api/files/big.csv", {"mode": "file"}); ok = False
except ac.SourceError as e: ok = "too large to preview" in str(e)
ac.PREVIEW_BYTES = saved_cap; check("file: a file over the preview cap is refused as too large", ok)
try: ac.preview("conn://badtoken/files/data.csv", {"mode": "file"}); ok = False
except ac.SourceError as e: ok = "refused the credentials" in str(e) and "WRONG-TOKEN-VALUE" not in str(e)
check("refused credentials are explained, without the secret", ok)
try: ac.preview("conn://api/../x", {"mode": "file"}); ok = False
except ac.SourceError: ok = True
check("the path rules apply to a preview too", ok)
check("nothing was created: no pipeline, no checkpoint, no staging directory", len(autoloader.list_pipelines()) == hist_before and not any(d.startswith("dkw_preview_") for d in os.listdir(tempfile.gettempdir())))

# ------------------------------------------------------------------ sftp
SH = os.getenv("SFTP_HOST")
if SH:
    print("sftp")
    import paramiko
    fp = autoloader_conn.discover_host_key(SH, 22)
    check("host key fingerprint is discovered", fp.startswith("SHA256:"), fp)
    base = {"host": SH, "port": 22, "username": "user", "auth": "password"}
    d = connections.definition_for_test({"type": "sftp", "config": base, "secret": {"password": "pass"}})
    tr = autoloader_conn.test_connection(d); check("Test without a pinned key returns the fingerprint to verify", tr.get("fingerprint") == fp and not tr["ok"], tr)
    wrong = "SHA256:" + "A" * 43
    tr = autoloader_conn.test_connection(connections.definition_for_test({"type": "sftp", "config": {**base, "host_key_sha256": wrong}, "secret": {"password": "pass"}}))
    check("a different host key is refused", not tr["ok"] and "does not match" in tr["message"], tr)
    tr = autoloader_conn.test_connection(connections.definition_for_test({"type": "sftp", "config": {**base, "host_key_sha256": fp}, "secret": {"password": "WRONG"}}))
    check("wrong password refused, password not echoed", not tr["ok"] and "WRONG" not in tr["message"], tr)
    sc = connections.create_connection({"name": "sftp1", "type": "sftp", "config": {**base, "host_key_sha256": fp}, "secret": {"password": "pass"}}, "admin")
    tr = autoloader_conn.test_connection(connections.definition_for_test({"id": "sftp1"})); check("saved connection tests OK (secret reused)", tr["ok"], tr)
    t = paramiko.Transport((SH, 22)); t.connect(username="user", password="pass"); sf = paramiko.SFTPClient.from_transport(t)
    def put(path, text, age=60):
        with sf.open(path, "w") as f: f.write(text)
        sf.utime(path, (time.time() - age, time.time() - age))
    for old in sf.listdir("/upload"):
        try: sf.remove("/upload/" + old)
        except IOError: pass
    put("/upload/a.csv", "id,v\n1,a\n2,b\n"); put("/upload/b.csv", "id,v\n3,c\n"); put("/upload/x.part", "id,v\n9,z\n"); put("/upload/.hidden.csv", "id,v\n8,h\n"); put("/upload/notes.txt", "hi")
    try: sf.mkdir("/upload/sub")
    except IOError: pass
    put("/upload/sub/deep.csv", "id,v\n7,d\n")
    pv = ac.preview("conn://sftp1/upload", {"recursive": False}, "*.csv")
    check("sftp preview: lists matching files and shows the first file's rows", pv["source"] == "sftp" and pv["sample"] and "a.csv" in pv["files"] and pv["columns"], pv)
    check("sftp preview: the pattern is applied", ac.preview("conn://sftp1/upload", {}, "b*.csv")["files"] == ["b.csv"])
    try: ac.preview("conn://sftp1/upload", {}, "*.nomatch"); ok = False
    except ac.SourceError as e: ok = "No file" in str(e)
    check("sftp preview: no match is explained", ok)
    ps = mk("sftp", "conn://sftp1/upload", "t_sftp", file_pattern="*.csv", source_options={"recursive": False, "settle_seconds": 10})
    r = run(ps); check("sftp: matching files loaded; .part, hidden, other patterns and subfolders skipped", r["files_ingested"] == 2 and n("t_sftp") == 3, (r, last_error(ps)))
    r = run(ps); check("sftp: exactly once", r["files_ingested"] == 0, r)
    put("/upload/c.csv", "id,v\n4,e\n"); r = run(ps); check("sftp: a new file is picked up", r["files_ingested"] == 1 and n("t_sftp") == 4, r)
    put("/upload/fresh.csv", "id,v\n5,f\n", age=0); r = run(ps); check("sftp: a file modified within settle_seconds is left for later", r["files_ingested"] == 0, r)
    ps2 = mk("sftp rec", "conn://sftp1/upload", "t_sftp2", file_pattern="*.csv", source_options={"recursive": True, "settle_seconds": 10})
    r = run(ps2); check("sftp: recursive includes subfolders", r["files_ingested"] == 4 and n("t_sftp2") == 5, (r, last_error(ps2)))
    check("sftp: the remote side is untouched", sorted(sf.listdir("/upload")) == sorted([".hidden.csv", "a.csv", "b.csv", "c.csv", "fresh.csv", "notes.txt", "sub", "x.part"]), sf.listdir("/upload"))
    connections.update_connection("sftp1", {"config": {"host_key_sha256": wrong}}, "admin")
    r = run(ps); check("a changed host key stops the pipeline with a clear error", "does not match" in last_error(ps) and autoloader.get_pipeline(ps["id"])["status"] == "ERROR", last_error(ps))
    connections.update_connection("sftp1", {"config": {"host_key_sha256": fp}}, "admin")
    key = os.getenv("SFTP_KEY")
    if key and os.path.exists(key):
        pem = open(key).read()
        kc = connections.create_connection({"name": "sftpkey", "type": "sftp", "config": {**base, "auth": "key", "host_key_sha256": fp}, "secret": {"private_key": pem}}, "admin")
        tr = autoloader_conn.test_connection(connections.definition_for_test({"id": "sftpkey"})); check("sftp: public-key authentication", tr["ok"], tr)
    sf.close(); t.close()
else:
    print("sftp: skipped (set SFTP_HOST)")
srv.shutdown(); shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
