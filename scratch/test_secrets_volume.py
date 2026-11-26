#!/usr/bin/env python3
"""Secrets off the warehouse volume (web/secrets_store.py) against throwaway directories: an install that keeps its keys and auth.db in
warehouse/.metadata is moved to DKW_SECRETS_DIR / DKW_COMPUTE_SECRETS_DIR by `python -m web.init`, and afterwards
  - the same keys are in use (an existing session token still works, a stored connection secret still decrypts, the user still exists),
  - nothing secret is left in .metadata (only `.moved` markers), files are 0600 and the directories 0700,
  - a process WITHOUT the variables refuses (SecretMovedError) instead of generating new keys or an empty accounts database,
  - two differing copies are refused without deleting either, and racing processes agree on one copy,
  - without the variables nothing changes (the old layout keeps working),
  - a secrets directory configured inside the warehouse is kept root-only by the kernel-isolation permission pass.
Every phase runs in a fresh subprocess: the paths are resolved when the modules are imported, like in the studio."""
import json, os, shutil, stat, subprocess, sys, tempfile, textwrap
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TMP = tempfile.mkdtemp(prefix="secvol_")
WH = os.path.join(TMP, "warehouse"); META = os.path.join(WH, ".metadata")
SEC = os.path.join(TMP, "secrets"); CSEC = os.path.join(TMP, "compute-secrets")
HASH = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)

def run(code, secrets=False, extra=None, warehouse=WH):
    env = {k: v for k, v in os.environ.items() if k not in ("DKW_SECRETS_DIR", "DKW_COMPUTE_SECRETS_DIR", "JWT_SECRET_KEY", "COMPUTE_TOKEN")}
    env.update({"WAREHOUSE_DIR": warehouse, "INIT_ADMIN_USERNAME": "admin", "INIT_ADMIN_PASSWORD_HASH": HASH, "PYTHONPATH": ROOT,
                "LOGIN_THROTTLE": "off", "NOTEBOOK_KERNEL_ISOLATION": "off"})
    if secrets:
        env.update({"DKW_SECRETS_DIR": SEC, "DKW_COMPUTE_SECRETS_DIR": CSEC})
    env.update(extra or {})
    p = subprocess.run([sys.executable, "-c", textwrap.dedent(code)], env=env, cwd=ROOT, capture_output=True, text=True, timeout=300)
    out = {}
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            out = json.loads(line[7:])
    return p.returncode, out, (p.stderr or "")[-1500:]

try:
    print("1. an install with the old layout (everything in .metadata)")
    rc, before, err = run("""
        import json, os
        from fastapi.testclient import TestClient
        from web import auth, app as app_module, secrets_store, connections
        from web.governance import macros
        with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
        auth.create_user("alice", "alicepassword1", "Alice")
        cl = TestClient(app_module.app); r = cl.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"})
        enc = secrets_store.encrypt_json({"password": "s3cret"})
        print("RESULT " + json.dumps({"login": r.status_code, "token": cl.cookies.get(auth.COOKIE_NAME), "enc": enc,
              "jwt": auth.JWT_SECRET_KEY, "salt": macros._hash_key().hex(), "authdb": auth.AUTH_DB_FILE,
              "compute": secrets_store.load_or_create_secret("compute_token")}))
    """)
    check("old layout works without the variables", rc == 0 and before.get("login") == 200, err)
    check("auth.db is in .metadata", before.get("authdb") == os.path.join(META, "auth.db"), before.get("authdb"))
    for f in ("jwt_secret", "connections.key", "auth.db", "compute_token"):
        check(f"{f} is in .metadata", os.path.exists(os.path.join(META, f)))
    with open(os.path.join(META, "llm_config.json"), "w") as f:
        json.dump({"openai_api_key": "sk-test"}, f)

    print("2. python -m web.init with the variables moves them")
    env = {k: v for k, v in os.environ.items()}
    rc, rep, err = run("""
        import json
        from web import init
        r = init.run()
        print("RESULT " + json.dumps({"failed": r.failed, "items": r.items}))
    """, secrets=True)
    sec_item = next((i for i in rep.get("items", []) if i["step"] == "secrets"), {})
    check("init succeeds", rc == 0 and not rep.get("failed"), (rep.get("items"), err))
    check("init reports what it moved", sec_item.get("status") == "ok" and "jwt_secret" in sec_item.get("message", "") and "auth.db" in sec_item.get("message", ""), sec_item)
    for f in ("jwt_secret", "connections.key", "auth.db", "llm_config.json"):
        check(f"{f} moved to DKW_SECRETS_DIR", os.path.exists(os.path.join(SEC, f)) and not os.path.exists(os.path.join(META, f)))
        check(f"{f}: marker left in .metadata", open(os.path.join(META, f + ".moved")).read().strip() == SEC)
    for f in ("compute_token", "governance_salt"):
        check(f"{f} moved to DKW_COMPUTE_SECRETS_DIR", os.path.exists(os.path.join(CSEC, f)) and not os.path.exists(os.path.join(META, f)))
    check("no auth.db -wal / -shm left in .metadata", not any(os.path.exists(os.path.join(META, "auth.db" + s)) for s in ("-wal", "-shm", "-journal")))
    left = [n for n in os.listdir(META) if n in ("jwt_secret", "connections.key", "mfa.key", "sharing.key", "sandbox_gateway_key", "auth.db", "compute_token", "governance_salt")]
    check("no managed secret left in .metadata", not left, left)
    check("secrets dirs are 0700", all(stat.S_IMODE(os.stat(d).st_mode) == 0o700 for d in (SEC, CSEC)), [oct(os.stat(d).st_mode) for d in (SEC, CSEC)])
    modes = {f: oct(stat.S_IMODE(os.stat(os.path.join(SEC, f)).st_mode)) for f in ("jwt_secret", "connections.key", "auth.db")}
    check("moved files are 0600", set(modes.values()) == {"0o600"}, modes)

    print("3. the studio afterwards uses the SAME keys")
    rc, after, err = run("""
        import json
        from fastapi.testclient import TestClient
        from web import auth, app as app_module, secrets_store, llm_settings
        cl = TestClient(app_module.app)
        tok = %r
        me = cl.get("/api/auth/me", headers={"Authorization": "Bearer " + tok}).json()
        print("RESULT " + json.dumps({"me": (me.get("user") or {}).get("username"), "dec": secrets_store.decrypt_json(%r), "jwt": auth.JWT_SECRET_KEY,
              "authdb": auth.AUTH_DB_FILE, "llm": llm_settings.LLM_CONFIG_FILE, "compute": secrets_store.load_or_create_secret("compute_token"),
              "login": cl.post("/api/auth/login", json={"username": "alice", "password": "alicepassword1"}).status_code}))
    """ % (before.get("token"), before.get("enc")), secrets=True)
    check("the studio starts", rc == 0, err)
    check("a session issued before the move is still valid (same signing key, same sessions table)", after.get("me") == "alice", after)
    check("the JWT key is unchanged", after.get("jwt") == before.get("jwt"))
    check("a stored connection secret still decrypts", after.get("dec") == {"password": "s3cret"}, after.get("dec"))
    check("the compute token is unchanged", after.get("compute") == before.get("compute"))
    check("alice still signs in (accounts moved, not re-bootstrapped)", after.get("login") == 200)
    check("auth.db and llm_config.json resolve to DKW_SECRETS_DIR", after.get("authdb") == os.path.join(SEC, "auth.db") and after.get("llm") == os.path.join(SEC, "llm_config.json"), after)

    print("4. a process without the variables refuses instead of making new keys")
    rc, out, err = run("""
        from web import secrets_store
        try:
            secrets_store.load_or_create_secret("jwt_secret"); print("RESULT {\\"made\\": true}")
        except secrets_store.SecretMovedError as e:
            import json; print("RESULT " + json.dumps({"refused": str(e)}))
    """)
    check("jwt_secret: SecretMovedError naming the directory and the variable", SEC in out.get("refused", "") and "DKW_SECRETS_DIR" in out.get("refused", ""), (out, err))
    check("no new jwt_secret was written to .metadata", not os.path.exists(os.path.join(META, "jwt_secret")))
    rc, out, err = run("import web.auth")
    check("the studio refuses to start without DKW_SECRETS_DIR (no empty accounts database, no fresh admin)", rc != 0 and "SecretMovedError" in err, err[-300:])
    check("no new auth.db was created in .metadata", not os.path.exists(os.path.join(META, "auth.db")))
    rc, out, err = run("""
        import json
        from web import secrets_store
        try:
            secrets_store.load_or_create_secret("compute_token"); print("RESULT {}")
        except secrets_store.SecretMovedError as e:
            print("RESULT " + json.dumps({"refused": str(e)}))
    """, extra={"DKW_SECRETS_DIR": SEC})
    check("a compute node missing DKW_COMPUTE_SECRETS_DIR refuses too (no second compute token / salt)", "DKW_COMPUTE_SECRETS_DIR" in out.get("refused", ""), (out, err))
    rc, out, err = run("""
        import json
        from web import secrets_store
        print("RESULT " + json.dumps({"tok": secrets_store.load_or_create_secret("compute_token")}))
        secrets_store.load_or_create_secret("jwt_secret")
    """, extra={"DKW_COMPUTE_SECRETS_DIR": CSEC})
    check("a compute node with only DKW_COMPUTE_SECRETS_DIR reads its token", out.get("tok") == before.get("compute"), (out, err))
    check("...but cannot reach the session key", rc != 0 and "SecretMovedError" in err)

    print("5. conflicts and races")
    os.makedirs(os.path.join(TMP, "c", "warehouse", ".metadata")); cwh = os.path.join(TMP, "c", "warehouse")
    open(os.path.join(cwh, ".metadata", "jwt_secret"), "w").write("aaaa")
    os.makedirs(os.path.join(TMP, "c", "sec")); open(os.path.join(TMP, "c", "sec", "jwt_secret"), "w").write("bbbb")
    rc, out, err = run("from web import secrets_store; secrets_store.secret_path('jwt_secret')", warehouse=cwh, extra={"DKW_SECRETS_DIR": os.path.join(TMP, "c", "sec")})
    check("two different copies are refused", rc != 0 and "different contents" in err, err[-300:])
    check("...and neither is deleted", open(os.path.join(cwh, ".metadata", "jwt_secret")).read() == "aaaa" and open(os.path.join(TMP, "c", "sec", "jwt_secret")).read() == "bbbb")
    os.makedirs(os.path.join(TMP, "r", "warehouse", ".metadata")); rwh = os.path.join(TMP, "r", "warehouse")
    open(os.path.join(rwh, ".metadata", "sharing.key"), "wb").write(os.urandom(32))
    orig = open(os.path.join(rwh, ".metadata", "sharing.key"), "rb").read()
    rsec = os.path.join(TMP, "r", "sec")
    env = {**os.environ, "WAREHOUSE_DIR": rwh, "DKW_SECRETS_DIR": rsec, "PYTHONPATH": ROOT}
    procs = [subprocess.Popen([sys.executable, "-c", "from web import secrets_store; print(open(secrets_store.secret_path('sharing.key'),'rb').read().hex())"],
                              env=env, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(6)]
    outs = [p.communicate(timeout=60) for p in procs]
    vals = {o[0].strip() for o in outs if o[0].strip()}
    check("six racing processes agree on the same key", vals == {orig.hex()}, (vals, [o[1][-200:] for o in outs if o[1]]))

    print("6. without the variables nothing moves")
    os.makedirs(os.path.join(TMP, "n", "warehouse")); nwh = os.path.join(TMP, "n", "warehouse")
    rc, out, err = run("""
        import json
        from web import init
        r = init.run()
        print("RESULT " + json.dumps({"failed": r.failed, "items": r.items}))
    """, warehouse=nwh)
    s = next((i for i in out.get("items", []) if i["step"] == "secrets"), {})
    check("init still succeeds", rc == 0 and not out.get("failed"), err)
    check("and warns that the keys stay on the warehouse volume", s.get("status") == "warn" and "DKW_SECRETS_DIR" in s.get("message", ""), s)
    check("auth.db and jwt_secret stay in .metadata", os.path.exists(os.path.join(nwh, ".metadata", "auth.db")) and os.path.exists(os.path.join(nwh, ".metadata", "jwt_secret")))

    if os.geteuid() == 0:
        print("7. a secrets directory inside the warehouse stays root-only")
        os.makedirs(os.path.join(TMP, "k", "warehouse", "keys")); kwh = os.path.join(TMP, "k", "warehouse")
        open(os.path.join(kwh, "keys", "jwt_secret"), "w").write("x")
        os.chmod(os.path.join(kwh, "keys", "jwt_secret"), 0o600)
        rc, out, err = run("from web import kernel_isolation; kernel_isolation.harden_permissions()", warehouse=kwh,
                           extra={"DKW_SECRETS_DIR": os.path.join(kwh, "keys"), "NOTEBOOK_KERNEL_ISOLATION": "on"})
        st_d, st_f = os.stat(os.path.join(kwh, "keys")), os.stat(os.path.join(kwh, "keys", "jwt_secret"))
        check("the permission pass ran", rc == 0, err)
        check("the directory is 0700 and not given to the data group", stat.S_IMODE(st_d.st_mode) == 0o700 and st_d.st_gid != 20000, (oct(st_d.st_mode), st_d.st_gid))
        check("the key inside is untouched (0600)", stat.S_IMODE(st_f.st_mode) == 0o600 and st_f.st_gid != 20000, (oct(st_f.st_mode), st_f.st_gid))
    else:
        print("7. skipped (not root)")
finally:
    shutil.rmtree(TMP, ignore_errors=True)
print(f"\n{'ALL PASSED' if not FAIL else f'{len(FAIL)} FAILED: {FAIL}'}")
sys.exit(1 if FAIL else 0)
