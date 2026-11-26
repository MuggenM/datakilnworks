#!/usr/bin/env python3
"""Credentials of storage mounts at rest (web/mounts.py): the secret fields of storage_mounts.json are Fernet-encrypted with
mounts.key, which lives in DKW_COMPUTE_SECRETS_DIR (the compute nodes attach the mounts too), not on the warehouse volume.
Callers still see plain values. Checks: nothing secret in the file, round trip, old plaintext files encrypted on load, '********'
keeps the stored secret, a lost key never blanks the stored ciphertext (and never mints a new key), client-sent sealing fields are
ignored, API views show neither ciphertext nor secrets, file mode 0600, and the init step reports both cases. Each phase runs in a
subprocess so module-level paths follow the environment."""
import json, os, shutil, subprocess, sys, tempfile
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TMP = tempfile.mkdtemp(prefix="mntcred_")
WH = os.path.join(TMP, "warehouse"); META = os.path.join(WH, ".metadata"); CSEC = os.path.join(TMP, "compute-secrets")
os.makedirs(META)
FILE = os.path.join(META, "storage_mounts.json")
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:400]}" if d and not c else ""))
    if not c: FAIL.append(n)

def run(code, csec=CSEC):
    env = {k: v for k, v in os.environ.items() if not k.startswith("DKW_")}
    env.update(WAREHOUSE_DIR=WH, PYTHONPATH=ROOT)
    if csec:
        env["DKW_COMPUTE_SECRETS_DIR"] = csec
    p = subprocess.run([sys.executable, "-c", "import json\nfrom web import mounts\n" + code], env=env, cwd=ROOT,
                       capture_output=True, text=True, timeout=120)
    out = [l[7:] for l in p.stdout.splitlines() if l.startswith("RESULT ")]
    if p.returncode or not out:
        return {"_error": (p.stderr or p.stdout)[-1500:]}
    return json.loads(out[-1])

def raw(): return open(FILE).read()

S3 = {"name": "lake", "type": "s3", "catalog_name": "lake",
      "config": {"bucket": "b1", "endpoint": "s3.example.test", "key_id": "AKIDEXAMPLE", "secret": "S3-SECRET-9b1e"}}
PG = {"name": "pg", "type": "postgres", "catalog_name": "pg",
      "config": {"host": "db", "user": "u", "database": "d", "password": "PG-PASSWORD-44c2", "secrets_enc": "client-forged"}}

print("encrypted at rest")
r = run(f"a = mounts.create_or_update_mount({S3!r}); b = mounts.create_or_update_mount({PG!r})\n"
        "print('RESULT ' + json.dumps({'a': a['id'], 'b': b['id'], 'mask': mounts.mask_mount_record(mounts.get_mount(b['id'])), "
        "'plain': {m['id']: m['config'] for m in mounts.load_mounts()}}))")
check("two mounts created", "a" in r, r)
text = raw()
check("no secret in the file", "S3-SECRET-9b1e" not in text and "PG-PASSWORD-44c2" not in text)
check("each record carries a ciphertext", text.count('"secrets_enc"') == 2)
check("a client-sent secrets_enc is not stored", "client-forged" not in text)
check("non-secret fields stay readable", "AKIDEXAMPLE" in text and "s3.example.test" in text)
check("callers see the plain values", r["plain"][r["a"]]["secret"] == "S3-SECRET-9b1e" and r["plain"][r["b"]]["password"] == "PG-PASSWORD-44c2", r.get("plain"))
check("callers never see the ciphertext field", all("secrets_enc" not in c for c in r["plain"].values()))
check("the API view masks the secret and has no ciphertext", r["mask"]["config"].get("password") == "********" and "secrets_enc" not in r["mask"]["config"], r["mask"])
check("the file is 0600", oct(os.stat(FILE).st_mode & 0o777) == "0o600", oct(os.stat(FILE).st_mode))
check("mounts.key is in DKW_COMPUTE_SECRETS_DIR", os.path.exists(os.path.join(CSEC, "mounts.key")))
check("and not on the warehouse volume", not os.path.exists(os.path.join(META, "mounts.key")))
A, B = r["a"], r["b"]

print("editing")
r = run(f"m = mounts.get_mount({A!r}); m['name'] = 'renamed'; m['config'] = dict(m['config'], secret='********')\n"
        "mounts.create_or_update_mount(m)\n"
        f"print('RESULT ' + json.dumps(mounts.get_mount({A!r})['config']))")
check("'********' keeps the stored secret", r.get("secret") == "S3-SECRET-9b1e", r)
r = run(f"m = mounts.get_mount({A!r}); m['config'] = dict(m['config'], secret='NEW-SECRET-0d2')\n"
        "mounts.create_or_update_mount(m)\n"
        f"print('RESULT ' + json.dumps(mounts.get_mount({A!r})['config']))")
check("a new secret replaces it", r.get("secret") == "NEW-SECRET-0d2" and "NEW-SECRET-0d2" not in raw(), r)

print("without the key")
other = os.path.join(TMP, "empty-secrets")
before = raw()
r = run("ms = mounts.load_mounts()\nprint('RESULT ' + json.dumps([m['config'] for m in ms]))", csec=other)
check("loading without the key works", isinstance(r, list), r)
check("the secrets come back empty (no authentication), not garbage", isinstance(r, list) and all(not c.get("secret") and not c.get("password") for c in r), r)
check("no new key was minted", not os.path.exists(os.path.join(other, "mounts.key")))
r = run(f"m = mounts.get_mount({B!r}); m['description'] = 'edited without the key'; m['config'] = dict(m['config'], password='********')\n"
        "mounts.create_or_update_mount(m)\nprint('RESULT {}')", csec=other)
check("an edit without the key is saved", r == {} and "edited without the key" in raw(), r)
r = run(f"print('RESULT ' + json.dumps(mounts.get_mount({B!r})['config']))")
check("with the key back, the stored password is still there", r.get("password") == "PG-PASSWORD-44c2", r)
check("and the S3 one", run(f"print('RESULT ' + json.dumps(mounts.get_mount({A!r})['config']))").get("secret") == "NEW-SECRET-0d2")

print("an old plaintext file")
with open(FILE, "w") as f:
    json.dump({"mounts": [{"id": "old1", "name": "old", "type": "s3", "catalog_name": "old",
                           "config": {"bucket": "b", "secret": "LEGACY-PLAINTEXT-5e"}}]}, f)
r = run("print('RESULT ' + json.dumps(mounts.get_mount('old1')['config']))")
check("it still loads with the plain value", r.get("secret") == "LEGACY-PLAINTEXT-5e", r)
check("and is encrypted on that load", "LEGACY-PLAINTEXT-5e" not in raw() and '"secrets_enc"' in raw())

print("the init step")
r = run("from web import init\nrep = init.Report()\ninit._seal_mount_credentials(rep)\n"
        "print('RESULT ' + json.dumps([list(x.values()) for x in rep.items]))")
check("reports encrypted at rest", "_error" not in r and any("encrypted at rest" in " ".join(x) for x in r), r)
r = run("from web import init\nrep = init.Report()\ninit._seal_mount_credentials(rep)\n"
        "print('RESULT ' + json.dumps([list(x.values()) for x in rep.items]))", csec=os.path.join(TMP, "empty2"))
check("reports mounts it cannot decrypt", "_error" not in r and any("cannot be decrypted" in " ".join(x) and "old" in " ".join(x) for x in r), r)

shutil.rmtree(TMP, ignore_errors=True)
print("\nALL PASSED" if not FAIL else f"\n{len(FAIL)} FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
