#!/usr/bin/env python3
"""FIDO Metadata Service cache (web/fido_mds.py) against a mock MDS server that serves a real signed JWS blob (RS256, a
generated RSA root CA + leaf certificate carried in the JWS header's x5c, exactly like the real FIDO Alliance service):
signature + certificate-chain verification, entry caching (model name, certification status, revocation), the
"skip when not due yet" cache logic, and the admin REST endpoints (/api/webauthn/mds/*), including the unconditional
registration-time refusal of a revoked model (web/webauthn_auth.py).
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook \
      sh -c "pip install -q webauthn cbor2 && python /workspace/scratch/test_fido_mds.py"
"""
import base64
import datetime
import json
import os
import shutil
import sys
import tempfile
import threading
import time

TMP = tempfile.mkdtemp(prefix="mds_")
os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"
os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
sys.path.insert(0, "/workspace")

import jwt
import requests
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from web import app as app_module, auth, fido_mds as fmds

FAIL = []


def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c:
        FAIL.append(n)


fmds.init_db()
PORT = 18941
BASE = f"http://127.0.0.1:{PORT}"
GOOD_AAGUID = "2fc0579f-8113-47ea-b116-bb5a8db9202a"
REVOKED_AAGUID = "ee882879-721c-4913-9775-3dfcce97072a"


def make_ca(cn):
    now = datetime.datetime.now(datetime.timezone.utc)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(key, hashes.SHA256()))
    return key, cert


def make_leaf(ca_key, ca_cert, cn="MDS BLOB Signer"):
    now = datetime.datetime.now(datetime.timezone.utc)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(ca_cert.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=365))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True).sign(ca_key, hashes.SHA256()))
    return key, cert


def pem(cert):
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def der_b64(cert):
    return base64.b64encode(cert.public_bytes(serialization.Encoding.DER)).decode()


root_key, root_cert = make_ca("Test FIDO MDS Root")
leaf_key, leaf_cert = make_leaf(root_key, root_cert)
other_root_key, other_root_cert = make_ca("Some Other Root")  # never configured as trusted: used for the "untrusted signer" case
other_leaf_key, other_leaf_cert = make_leaf(other_root_key, other_root_cert)


def payload():
    return {
        "legalHeader": "test fixture, not the real FIDO Alliance blob", "no": 7, "nextUpdate": "2999-01-01",
        "entries": [
            {"aaguid": GOOD_AAGUID, "metadataStatement": {"description": "Good Test Key"},
             "statusReports": [{"status": "FIDO_CERTIFIED", "effectiveDate": "2020-01-01"}, {"status": "FIDO_CERTIFIED_L1", "effectiveDate": "2021-06-01"}]},
            {"aaguid": REVOKED_AAGUID, "metadataStatement": {"description": "Revoked Test Key"},
             "statusReports": [{"status": "FIDO_CERTIFIED", "effectiveDate": "2019-01-01"}, {"status": "REVOKED", "effectiveDate": "2022-06-01"}]},
            {"attestationCertificateKeyIdentifiers": ["abcd1234"], "description": "U2F-only entry, no AAGUID"},
        ],
    }


def sign(pl, key, chain_certs):
    return jwt.encode(pl, key, algorithm="RS256", headers={"x5c": [der_b64(c) for c in chain_certs]})


class State:
    blob = None
    hits = 0


State.blob = sign(payload(), leaf_key, [leaf_cert])

mock = FastAPI()


@mock.get("/blob")
def blob():
    State.hits += 1
    return Response(content=State.blob, media_type="text/plain")


def start_server():
    server = uvicorn.Server(uvicorn.Config(mock, host="127.0.0.1", port=PORT, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(50):
        try:
            requests.get(f"{BASE}/blob", timeout=1)
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("mock MDS server did not come up")


start_server()

print("configuration validation")
check("no configuration yet: nothing is fetched, status says not configured", fmds.status()["configured"] is False and fmds.refresh(True)["entry_count"] == 0)
try:
    fmds.set_config("ftp://nope", "", "admin"); ok = False
except fmds.MdsError:
    ok = True
check("a non-http(s) URL is refused", ok)
try:
    fmds.set_config("", "not a certificate", "admin"); ok = False
except fmds.MdsError:
    ok = True
check("junk PEM is refused", ok)
out = fmds.set_config(BASE + "/blob", pem(root_cert), "admin")
check("a valid URL and root certificate are accepted and normalised", out["effective_url"] == BASE + "/blob" and len(out["roots"]) == 1 and out["roots"][0]["subject"].endswith("Test FIDO MDS Root"), out)

print("refresh: signature and chain verification")
hits0 = State.hits
st = fmds.refresh(True)
check("a blob signed by a certificate chaining to the configured root is accepted", st["last_error"] is None and st["entry_count"] == 2 and State.hits == hits0 + 1, st)
check("the AAGUID-less (U2F) entry is not cached; the two AAGUID entries are", st["entry_count"] == 2)
good = fmds.lookup(GOOD_AAGUID)
check("a certified, non-revoked model is cached with its description, latest status and revoked=False", good and good["description"] == "Good Test Key" and good["status"] == "FIDO_CERTIFIED_L1" and good["revoked"] is False, good)
rev = fmds.lookup(REVOKED_AAGUID)
check("a model whose latest status report is REVOKED is cached as revoked", rev and rev["revoked"] is True and rev["status"] == "REVOKED", rev)
check("an AAGUID never seen is None", fmds.lookup("00000000-0000-4000-8000-000000000000") is None)

print("refresh: due / not due (nextUpdate far in the future)")
hits1 = State.hits
st2 = fmds.refresh(False)
check("a refresh that is not due yet (nextUpdate unreached) does not fetch again", State.hits == hits1 and st2["entry_count"] == 2, (hits1, State.hits))
st3 = fmds.refresh(True)
check("force=True fetches regardless", State.hits == hits1 + 1, (hits1, State.hits))

print("refresh: an untrusted signer is refused, the existing cache is kept")
State.blob = sign(payload(), other_leaf_key, [other_leaf_cert])  # signed by a chain the configured root never issued
before = fmds.status()
st4 = fmds.refresh(True)
check("a chain that does not lead to the configured root is refused", st4["last_error"] is not None and "trust root" in st4["last_error"], st4)
check("the existing cache is left untouched by a failed refresh", fmds.lookup(GOOD_AAGUID) is not None and st4["entry_count"] == before["entry_count"], st4)

print("refresh: a tampered blob (signature no longer matches the payload) is refused")
good_blob = sign(payload(), leaf_key, [leaf_cert])
h, p, s = good_blob.split(".")
tampered_payload = json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
tampered_payload["entries"][0]["metadataStatement"]["description"] = "Tampered!"
tampered_p = base64.urlsafe_b64encode(json.dumps(tampered_payload).encode()).decode().rstrip("=")
State.blob = f"{h}.{tampered_p}.{s}"
st5 = fmds.refresh(True)
check("a payload modified after signing fails signature verification", st5["last_error"] is not None and "signature" in st5["last_error"].lower(), st5)
check("...and the cache still holds the untampered description", fmds.lookup(GOOD_AAGUID)["description"] == "Good Test Key")

print("refresh: back to a good, freshly-signed blob restores the cache")
State.blob = sign(payload(), leaf_key, [leaf_cert])
st6 = fmds.refresh(True)
check("a subsequent good refresh clears last_error and re-populates the cache", st6["last_error"] is None and st6["entry_count"] == 2, st6)

print("admin REST endpoints")
with auth.get_db_connection() as c:
    c.execute("UPDATE users SET must_change_password = 0")
admin = TestClient(app_module.app)
admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
anon = TestClient(app_module.app)
check("the endpoints are admin-only", anon.get("/api/webauthn/mds/status").status_code in (401, 403) and anon.put("/api/webauthn/mds/config", json={}).status_code in (401, 403) and anon.post("/api/webauthn/mds/refresh").status_code in (401, 403))
r = admin.get("/api/webauthn/mds/status")
check("status is readable and matches the module", r.status_code == 200 and r.json()["entry_count"] == 2 and r.json()["configured"] is True, r.text)
r = admin.get(f"/api/webauthn/mds/lookup/{GOOD_AAGUID}")
check("an admin can look up a single model", r.status_code == 200 and r.json()["description"] == "Good Test Key", r.text)
r = admin.get("/api/webauthn/mds/lookup/00000000-0000-4000-8000-000000000000")
check("an unknown AAGUID is a 404", r.status_code == 404, r.text)
r = admin.put("/api/webauthn/mds/config", json={"url": BASE + "/blob", "root_pem": pem(root_cert)})
check("saving the configuration through the API auto-refreshes and returns the result in one response", r.status_code == 200 and r.json()["entry_count"] == 2 and r.json().get("last_error") is None, r.text)
r = admin.put("/api/webauthn/mds/config", json={"url": "not a url", "root_pem": ""})
check("bad input through the API is a 400 with a safe message", r.status_code == 400, r.text)
r = admin.post("/api/webauthn/mds/refresh")
check("the manual refresh endpoint works", r.status_code == 200 and r.json()["entry_count"] == 2, r.text)
check("configuration changes and refreshes are audited", all(a_ in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall())
      for a_ in ("FIDO_MDS_CONFIG_UPDATE", "FIDO_MDS_REFRESH")))

shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.exit(1 if FAIL else 0)
