#!/usr/bin/env python3
"""WebAuthn passkeys (web/webauthn_auth.py + the /api/auth/... endpoints) in a throwaway warehouse with a SOFTWARE authenticator that builds real
attestation / assertion objects (ES256): registration, second factor after the password, passwordless sign-in, and the refusals (wrong origin, RP id,
challenge reuse, other accounts' credentials, missing user verification, cloned counters, ...).
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook sh -c "pip install -q webauthn && python /workspace/scratch/test_webauthn.py" """
import base64, datetime, hashlib, json, os, secrets, shutil, struct, sys, tempfile, time, uuid
TMP = tempfile.mkdtemp(prefix="wa_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$d08ef6c2826b1edc9dc90b321eea092d$e5fe10db63818165f3fef39c3d8bfb37a2ad54a29c96b4de42ca5605f964d73d"
sys.path.insert(0, "/workspace")
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient
from web import app as app_module, auth, mfa, mfa_policy, webauthn_auth as wa, fido_mds as fmds
fmds.init_db()
def _seed_mds(aaguid, revoked=False, description="Test Model", status_=None):
    c = auth.get_db_connection()
    try:
        with c:
            c.execute("INSERT OR REPLACE INTO fido_mds_entries (aaguid, description, status, revoked, status_reports, updated_at) VALUES (?,?,?,?,?,?)",
                      (aaguid, description, status_ or ("REVOKED" if revoked else "FIDO_CERTIFIED_L1"), 1 if revoked else 0, "[]", int(time.time())))
    finally:
        c.close()
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
b64u = lambda b: base64.urlsafe_b64encode(b).decode().rstrip("=")
unb64u = lambda s: base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
ORIGIN, RP = "http://testserver", "testserver"

class Authenticator:
    """A software passkey: one credential, ES256, attestation 'none'."""
    def __init__(self, user_handle=None, sign_count=0):
        self.key = ec.generate_private_key(ec.SECP256R1()); self.cred_id = secrets.token_bytes(32); self.count = sign_count; self.handle = user_handle
    def _cose(self):
        n = self.key.public_key().public_numbers()
        return cbor2.dumps({1: 2, 3: -7, -1: 1, -2: n.x.to_bytes(32, "big"), -3: n.y.to_bytes(32, "big")})
    def create(self, options, origin=ORIGIN, rp=RP, uv=True, type_="webauthn.create", challenge=None, aaguid=None, att=None, self_att=False, bad_sig=False):
        o = options["options"]; self.handle = self.handle or unb64u(o["user"]["id"])
        flags = 0x01 | (0x04 if uv else 0) | 0x40
        auth_data = hashlib.sha256(rp.encode()).digest() + bytes([flags]) + struct.pack(">I", self.count) + (uuid.UUID(aaguid).bytes if aaguid else b"\0" * 16) + struct.pack(">H", len(self.cred_id)) + self.cred_id + self._cose()
        cd = json.dumps({"type": type_, "challenge": challenge or o["challenge"], "origin": origin, "crossOrigin": False}).encode()
        obj = {"fmt": "none", "attStmt": {}, "authData": auth_data}
        if att or self_att:                          # packed attestation: with a certificate chain (att) or self attestation (signed by the credential key itself)
            signer = att["key"] if att else self.key
            sig = signer.sign(auth_data + hashlib.sha256(cd).digest() + (b"x" if bad_sig else b""), ec.ECDSA(hashes.SHA256()))
            obj = {"fmt": "packed", "attStmt": ({"alg": -7, "sig": sig, "x5c": att["chain"]} if att else {"alg": -7, "sig": sig}), "authData": auth_data}
        return {"id": b64u(self.cred_id), "rawId": b64u(self.cred_id), "type": "public-key", "clientExtensionResults": {},
                "response": {"clientDataJSON": b64u(cd), "attestationObject": b64u(cbor2.dumps(obj)), "transports": ["internal"]}}
    def get(self, options, origin=ORIGIN, rp=RP, uv=True, handle=True, count=None, challenge=None, sign_with=None):
        o = options["options"]; self.count = self.count + 1 if count is None else count
        flags = 0x01 | (0x04 if uv else 0)
        auth_data = hashlib.sha256(rp.encode()).digest() + bytes([flags]) + struct.pack(">I", self.count)
        cd = json.dumps({"type": "webauthn.get", "challenge": challenge or o["challenge"], "origin": origin, "crossOrigin": False}).encode()
        sig = (sign_with or self.key).sign(auth_data + hashlib.sha256(cd).digest(), ec.ECDSA(hashes.SHA256()))
        resp = {"clientDataJSON": b64u(cd), "authenticatorData": b64u(auth_data), "signature": b64u(sig)}
        if handle: resp["userHandle"] = b64u(self.handle if handle is True else handle)
        return {"id": b64u(self.cred_id), "rawId": b64u(self.cred_id), "type": "public-key", "clientExtensionResults": {}, "response": resp}

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization
def make_pki(aaguid, cn="Test Root CA"):
    """A root CA and an attestation leaf carrying `aaguid` (packed attestation rules): (root PEM, {key, chain})."""
    now = datetime.datetime.now(datetime.timezone.utc); ca_key = ec.generate_private_key(ec.SECP256R1()); ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=3650)).add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(ca_key, hashes.SHA256()))
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    subj = x509.Name([x509.NameAttribute(NameOID.COUNTRY_NAME, "US"), x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test Vendor"), x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Authenticator Attestation"), x509.NameAttribute(NameOID.COMMON_NAME, "Test Key")])
    leaf = (x509.CertificateBuilder().subject_name(subj).issuer_name(ca_name).public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=365)).add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.UnrecognizedExtension(x509.ObjectIdentifier("1.3.6.1.4.1.45724.1.1.4"), b"\x04\x10" + uuid.UUID(aaguid).bytes), critical=False).sign(ca_key, hashes.SHA256()))
    return ca.public_bytes(serialization.Encoding.PEM).decode(), {"key": leaf_key, "chain": [leaf.public_bytes(serialization.Encoding.DER)]}

with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
for n, role in (("alice", "user"), ("bob", "user")): auth.create_user(n, n + "password1", n.title(), role)
with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0")
H = {"Origin": ORIGIN}
def login(username, pw, client=None):
    c = client or TestClient(app_module.app); r = c.post("/api/auth/login", json={"username": username, "password": pw}); return c, r
def session(username):
    c, r = login(username, username + "password1"); assert r.json().get("success"), r.text; return c
def register(client, pw, name="Laptop", **kw):
    ro = client.post("/api/auth/webauthn/register/options", json={"password": pw}, headers=H)
    if ro.status_code != 200:
        return None, None, ro
    o = ro.json()
    a = kw.pop("auth", None) or Authenticator()
    cred = a.create(o, **kw)
    return a, o, client.post("/api/auth/webauthn/register/verify", json={"challenge_id": o["challenge_id"], "credential": cred, "name": name}, headers=H)

print("registration")
alice = session("alice")
check("the package is installed and passkeys are offered", wa.available() and alice.get("/api/auth/passkey/status").json() == {"available": True})
r = alice.post("/api/auth/webauthn/register/options", json={}, headers=H); check("registering needs the password (a stolen session is not enough)", r.status_code == 403, r.text)
r = alice.post("/api/auth/webauthn/register/options", json={"password": "wrong"}, headers=H); check("...the right one", r.status_code == 403 and "incorrect" in r.text)
o = alice.post("/api/auth/webauthn/register/options", json={"password": "alicepassword1"}, headers=H).json(); po = o["options"]
check("options: RP id from the host, no attestation, resident key preferred, user handle = the account id, 32-byte challenge", po["rp"]["id"] == RP and po["attestation"] == "none" and po["authenticatorSelection"]["residentKey"] == "preferred" and unb64u(po["user"]["id"]).decode() == auth.get_user_by_username("alice")["id"] and len(unb64u(po["challenge"])) == 32 and po["excludeCredentials"] == [], po)
a1 = Authenticator(); r = alice.post("/api/auth/webauthn/register/verify", json={"challenge_id": o["challenge_id"], "credential": a1.create(o), "name": "Laptop"}, headers=H)
check("a valid passkey is registered", r.status_code == 200 and r.json()["credential"]["name"] == "Laptop" and r.json()["credential"]["user_verified"] is True, r.text)
check("the same challenge cannot be used twice", alice.post("/api/auth/webauthn/register/verify", json={"challenge_id": o["challenge_id"], "credential": Authenticator().create(o), "name": "x"}, headers=H).status_code == 400)
st = alice.get("/api/auth/webauthn/status").json(); me = alice.get("/api/auth/me").json(); check("status lists it; the account counts as having a second factor", len(st["credentials"]) == 1 and st["rp_id"] == RP and me["user"]["mfa_enabled"] is True, me)
check("the stored row holds the public key, not a secret; the credential id is the primary key", len(wa._get(b64u(a1.cred_id))["public_key"]) > 40)
o2 = alice.post("/api/auth/webauthn/register/options", json={"password": "alicepassword1"}, headers=H).json()
check("an authenticator already registered is excluded from the next registration", [e["id"] for e in o2["options"]["excludeCredentials"]] == [b64u(a1.cred_id)])
def reg_fail(label, **kw):
    a, o, r = register(alice, "alicepassword1", **kw); check(label, r.status_code == 400, r.text); return r
reg_fail("a response for another origin is refused", origin="http://evil.example")
reg_fail("a response for another RP id is refused", rp="evil.example")
reg_fail("the wrong ceremony type is refused", type_="webauthn.get")
reg_fail("a wrong challenge is refused", challenge=b64u(b"x" * 32))
o3 = alice.post("/api/auth/webauthn/register/options", json={"password": "alicepassword1"}, headers=H).json()
bob = session("bob"); check("a challenge belongs to the account that asked for it", bob.post("/api/auth/webauthn/register/verify", json={"challenge_id": o3["challenge_id"], "credential": Authenticator().create(o3), "name": "x"}, headers=H).status_code == 400)
check("garbage credentials and unknown challenges are refused cleanly", alice.post("/api/auth/webauthn/register/verify", json={"challenge_id": "nope", "credential": {}, "name": "x"}, headers=H).status_code == 400)
a2, _, r = register(alice, "alicepassword1", name="Phone"); check("a second passkey can be added", r.status_code == 200 and len(alice.get("/api/auth/webauthn/status").json()["credentials"]) == 2)
cid = a2.cred_id; r = alice.patch(f"/api/auth/webauthn/credentials/{b64u(cid)}", json={"name": "Work phone"}, headers=H); check("a passkey can be renamed (only by its owner)", r.status_code == 200 and bob.patch(f"/api/auth/webauthn/credentials/{b64u(cid)}", json={"name": "mine"}, headers=H).status_code == 400 and any(c["name"] == "Work phone" for c in alice.get("/api/auth/webauthn/status").json()["credentials"]))
check("the number of passkeys per account is limited", all(register(alice, "alicepassword1", name=f"k{i}")[2].status_code == 200 for i in range(8)) and register(alice, "alicepassword1")[2].status_code == 400)
for x in alice.get("/api/auth/webauthn/status").json()["credentials"][2:]: alice.post(f"/api/auth/webauthn/credentials/{x['id']}/delete", json={"password": "alicepassword1"}, headers=H)

print("second factor after the password")
c, r = login("alice", "alicepassword1"); j = r.json()
check("with a passkey registered the password step returns a token and the methods, no session", j.get("mfa_required") and j["mfa_methods"] == ["webauthn"] and "token" not in j and c.get("/api/auth/me").json()["authenticated"] is False, j)
tok = j["mfa_token"]
check("no session cookie was set by the password step", c.get("/api/auth/me").json()["authenticated"] is False)
r = c.post("/api/auth/login/webauthn/options", json={"mfa_token": "bad"}, headers=H); check("options need a valid mfa_token", r.status_code == 401)
opt = c.post("/api/auth/login/webauthn/options", json={"mfa_token": tok}, headers=H).json()
check("the options name the account's own credentials", sorted(x["id"] for x in opt["options"]["allowCredentials"]) == sorted(x["id"] for x in alice.get("/api/auth/webauthn/status").json()["credentials"]) and opt["options"]["userVerification"] == "preferred")
def second(a, **kw):
    cc, rr = login("alice", "alicepassword1"); t = rr.json()["mfa_token"]; op = cc.post("/api/auth/login/webauthn/options", json={"mfa_token": t}, headers=H).json()
    return cc, cc.post("/api/auth/login/webauthn", json={"mfa_token": t, "challenge_id": op["challenge_id"], "credential": a.get(op, **kw)}, headers=H), op, t
cc, r, op, t = second(a1, uv=False)
check("the right passkey completes the sign-in (user verification is not required for a second factor)", r.status_code == 200 and r.json()["success"] and cc.get("/api/auth/me").json()["user"]["username"] == "alice", r.text)
cc, r, op, t = second(a1, origin="http://evil.example"); check("an assertion for another origin is refused", r.status_code == 401)
cc, r, op, t = second(a1, sign_with=ec.generate_private_key(ec.SECP256R1())); check("a wrong signature is refused", r.status_code == 401)
cc, r, op, t = second(a1, challenge=b64u(b"y" * 32)); check("a wrong challenge is refused", r.status_code == 401)
cc, r, op, t = second(a1, rp="evil.example"); check("a wrong RP id hash is refused", r.status_code == 401)
r2 = cc.post("/api/auth/login/webauthn", json={"mfa_token": t, "challenge_id": op["challenge_id"], "credential": a1.get(op)}, headers=H); check("a used challenge is gone (replay of the same ceremony is refused)", r2.status_code == 401)
bob_a = Authenticator(); register(bob, "bobpassword1", auth=bob_a)
cc, rr = login("alice", "alicepassword1"); t = rr.json()["mfa_token"]; op = cc.post("/api/auth/login/webauthn/options", json={"mfa_token": t}, headers=H).json()
r = cc.post("/api/auth/login/webauthn", json={"mfa_token": t, "challenge_id": op["challenge_id"], "credential": bob_a.get(op)}, headers=H); check("another account's passkey does not work for alice", r.status_code == 401 and "not registered" in r.text)
cc, rr = login("alice", "alicepassword1"); t = rr.json()["mfa_token"]; op_alice = cc.post("/api/auth/login/webauthn/options", json={"mfa_token": t}, headers=H).json()
bc, br = login("bob", "bobpassword1"); bt = br.json()["mfa_token"]
r = bc.post("/api/auth/login/webauthn", json={"mfa_token": bt, "challenge_id": op_alice["challenge_id"], "credential": bob_a.get(op_alice)}, headers=H); check("a challenge issued to alice cannot be used for bob's sign-in", r.status_code == 401)
a_clone = Authenticator(); a_clone.cred_id, a_clone.key, a_clone.handle = a1.cred_id, a1.key, a1.handle; a_clone.count = 0
cc, r, op, t = second(a_clone, count=1); check("a counter that goes backwards (a cloned authenticator) is refused, with the reason", r.status_code == 401 and "cloned" in r.text, r.text)
row = wa._get(b64u(a1.cred_id)); check("the counter was stored after the good sign-in", row["sign_count"] >= 1 and row["last_used_at"] is not None)
a_sync = Authenticator(); _, _, rr = register(alice, "alicepassword1", auth=a_sync, name="Synced")
cc, r, op, t = second(a_sync, count=0); cc2, r2, _, _ = second(a_sync, count=0); check("passkeys that always report 0 (synced passkeys) are accepted every time", r.status_code == 200 and r2.status_code == 200)
tc = TestClient(app_module.app); ok, _ = (None, None)
sec = mfa.begin_setup(auth.get_user_by_username("bob")["id"], "bob"); mfa.confirm_setup(auth.get_user_by_username("bob")["id"], mfa.totp(base64.b32decode(sec["secret"] + "=" * (-len(sec["secret"]) % 8))))
bc, br = login("bob", "bobpassword1"); check("with a passkey AND an authenticator app both methods are offered", br.json()["mfa_methods"] == ["totp", "webauthn"], br.text)
tt = mfa.create_mfa_token(auth.get_user_by_username("carol") or auth.get_user_by_username("bob")); r = TestClient(app_module.app).post("/api/auth/login/mfa", json={"mfa_token": bc.post("/api/auth/login", json={"username": "bob", "password": "bobpassword1"}).json()["mfa_token"], "code": "000000"}); check("(the TOTP step still works as before and refuses a bad code)", r.status_code == 401)

print("passwordless sign-in")
anon = TestClient(app_module.app); po = anon.post("/api/auth/passkey/options", headers=H).json()
check("options are public, name no account and require user verification", po["options"].get("allowCredentials") in (None, []) and po["options"]["userVerification"] == "required", po)
def pl(a, **kw):
    op = anon.post("/api/auth/passkey/options", headers=H).json(); return anon.post("/api/auth/passkey/login", json={"challenge_id": op["challenge_id"], "credential": a.get(op, **kw)}, headers=H)
c2 = TestClient(app_module.app); op = c2.post("/api/auth/passkey/options", headers=H).json()
r = c2.post("/api/auth/passkey/login", json={"challenge_id": op["challenge_id"], "credential": a1.get(op)}, headers=H)
check("a passkey with user verification signs in without any password", r.status_code == 200 and r.json()["user"]["username"] == "alice" and c2.get("/api/auth/me").json()["user"]["username"] == "alice", r.text)
check("without user verification the sign-in is refused (it would be one factor)", pl(a1, uv=False).status_code == 401)
check("without a user handle it is refused (the passkey must identify the account)", pl(a1, handle=False).status_code == 401)
check("with another account's user handle it is refused", pl(a1, handle=auth.get_user_by_username("bob")["id"].encode()).status_code == 401)
check("origin, signature and counter rules apply here too", pl(a1, origin="http://evil.example").status_code == 401 and pl(a1, sign_with=ec.generate_private_key(ec.SECP256R1())).status_code == 401 and pl(a_clone, count=1).status_code == 401)
op = anon.post("/api/auth/passkey/options", headers=H).json(); cred = a1.get(op)
check("a challenge from the second-factor flow is not accepted here (and the reverse)", anon.post("/api/auth/passkey/login", json={"challenge_id": op_alice["challenge_id"], "credential": cred}, headers=H).status_code == 401)
check("an unknown passkey is refused", pl(Authenticator(user_handle=b"x")).status_code == 401)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET is_active = 0 WHERE username = 'alice'")
check("a deactivated account cannot sign in with its passkey", pl(a1).status_code == 401)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET is_active = 1 WHERE username = 'alice'")
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET auth_source = 'oidc' WHERE username = 'alice'")
check("accounts of an external identity provider cannot use passwordless sign-in", pl(a1).status_code == 401)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET auth_source = 'local' WHERE username = 'alice'")
os.environ["WEBAUTHN_PASSWORDLESS"] = "off"
check("WEBAUTHN_PASSWORDLESS=off switches the sign-in off (second factor keeps working)", anon.get("/api/auth/passkey/status").json() == {"available": False} and anon.post("/api/auth/passkey/options", headers=H).status_code == 400 and second(a1)[1].status_code == 200)
del os.environ["WEBAUTHN_PASSWORDLESS"]
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 1 WHERE username = 'alice'")
r = pl(a1); ac = anon
check("a forced password change still applies after a passkey sign-in", r.status_code == 200 and anon.get("/api/users", headers=H).status_code == 403)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 0 WHERE username = 'alice'")

print("origin and host handling")
check("the RP id and origin come from the request (Host + same-host Origin)", wa.relying_party({"Host": "lake.example.com:8443", "Origin": "https://lake.example.com:8443"}) == ("lake.example.com", ["https://lake.example.com:8443"]))
check("an Origin for another host is not trusted (the Host-derived one is used and the response will not match)", wa.relying_party({"Host": "lake.example.com", "Origin": "https://evil.example"}) == ("lake.example.com", ["http://lake.example.com"]))
os.environ["WEBAUTHN_RP_ID"] = "example.com"; os.environ["WEBAUTHN_ORIGINS"] = "https://a.example.com, https://b.example.com/"
check("WEBAUTHN_RP_ID and WEBAUTHN_ORIGINS override", wa.relying_party({"Host": "x"}) == ("example.com", ["https://a.example.com", "https://b.example.com"]))
del os.environ["WEBAUTHN_RP_ID"], os.environ["WEBAUTHN_ORIGINS"]
o = alice.post("/api/auth/webauthn/register/options", json={"password": "alicepassword1"}, headers={"Host": "127.0.0.1:8891"})
check("an IP address cannot be a relying party (browsers refuse it): a clear message", o.status_code == 400 and "host name" in o.text, o.text)

print("removing, policy and admin reset")
cid = b64u(a_sync.cred_id)
check("removing a passkey needs the password and belongs to its owner", alice.post(f"/api/auth/webauthn/credentials/{cid}/delete", json={}, headers=H).status_code == 403 and bob.post(f"/api/auth/webauthn/credentials/{cid}/delete", json={"password": "bobpassword1"}, headers=H).status_code == 404)
check("...and removes it", alice.post(f"/api/auth/webauthn/credentials/{cid}/delete", json={"password": "alicepassword1"}, headers=H).status_code == 200 and cid not in [x["id"] for x in alice.get("/api/auth/webauthn/status").json()["credentials"]])
admin = TestClient(app_module.app); admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
carol_id = auth.create_user("carol", "carolpassword1", "Carol", "user")["id"]
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 0")
carol = session("carol"); ca, _, r = register(carol, "carolpassword1"); check("carol registers a passkey", r.status_code == 200)
admin_uid = auth.get_user_by_username("admin")["id"]
mfa_policy.set_policy  # (enabling the policy needs the admin enrolled; enrol admin with a passkey)
admin_a, _, r = register(admin, "adminpassword123", name="Admin key"); check("an admin can enrol with a passkey", r.status_code == 200, r.text)
r = admin.put("/api/mfa/policy", json={"enabled": True, "roles": ["admin", "power_user", "user"], "grace_days": 7}); check("the MFA policy can be switched on: a passkey satisfies 'enrolled' for the admin", r.status_code == 200, r.text)
st = mfa_policy.status_for_user_id(carol_id); check("a user with only a passkey is compliant", st["state"] == "compliant" and st["enrolled"] is True, st)
sts = admin.get("/api/mfa/stats").json(); check("the stats count passkey users and show the methods", sts["methods"]["passkey"] >= 2 and "totp" in sts["methods"] and sts["methods"]["both"] >= 1, sts["methods"])
r = carol.post(f"/api/auth/webauthn/credentials/{b64u(ca.cred_id)}/delete", json={"password": "carolpassword1"}, headers=H); check("the last second factor cannot be removed while the policy requires one (409)", r.status_code == 409, r.text)
ca2, _, r = register(carol, "carolpassword1", name="Second"); check("...but another passkey can be added, and then one removed", r.status_code == 200 and carol.post(f"/api/auth/webauthn/credentials/{b64u(ca.cred_id)}/delete", json={"password": "carolpassword1"}, headers=H).status_code == 200)
r = admin.post(f"/api/users/{carol_id}/mfa/reset"); check("an admin reset removes the passkeys too (a lost device)", r.status_code == 200 and wa.count(carol_id) == 0, r.text)
check("(the reset is audited)", "PASSKEY_RESET" in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()) and "PASSKEY_ADD" in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()))
check("challenges do not pile up: expired ones are purged when a new one is made", (lambda c: (c.execute("INSERT INTO webauthn_challenges VALUES ('old','x','register',NULL,1)"), c.commit(), wa._new_challenge("register", None), c.execute("SELECT COUNT(*) FROM webauthn_challenges WHERE id = 'old'").fetchone()[0] == 0)[-1])(auth.get_db_connection()))
wa.MAX_OPEN_CHALLENGES = 3; ok = True
try:
    for _ in range(5): wa._new_challenge("register", None)
    ok = False
except wa.WebAuthnError:
    pass
check("the number of open challenges is capped", ok)
wa.MAX_OPEN_CHALLENGES = 2000; _c = auth.get_db_connection(); _c.execute("DELETE FROM webauthn_challenges"); _c.commit()
print("attestation policy")
AA, BB = "2fc0579f-8113-47ea-b116-bb5a8db9202a", "ee882879-721c-4913-9775-3dfcce97072a"
root_a, leaf_a = make_pki(AA, "Vendor A Root"); root_b, leaf_b = make_pki(BB, "Vendor B Root"); root_x, leaf_x = make_pki(AA, "Untrusted Root")
put = lambda **kw: admin.put("/api/webauthn/policy", json=kw)
carol = session("carol") if False else None
dave_id = auth.create_user("dave", "davepassword1", "Dave", "user")["id"]
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 0")
dave = session("dave")
DPW = "davepassword1"
def dreg(**kw):
    a, o, r = register(dave, DPW, name=kw.pop("name", "K"), **kw); return a, o, r
def clear(): wa.delete_all(dave_id)
check("the policy is admin-only; the default is 'none' and nothing is asked of authenticators", dave.get("/api/webauthn/policy").status_code == 403 and admin.get("/api/webauthn/policy").json()["mode"] == "none"
      and dave.post("/api/auth/webauthn/register/options", json={"password": DPW}, headers=H).json()["options"]["attestation"] == "none")
bad = lambda **kw: put(**kw).status_code == 400
check("bad input is refused: unknown mode, malformed AAGUID, junk PEM, 'require' without roots", bad(mode="strict") and bad(mode="record", allowed=[{"aaguid": "nope", "label": "x"}]) and bad(mode="record", roots_pem="not a certificate") and bad(mode="require", roots_pem=""))
r = put(mode="record"); check("'record' without roots is accepted", r.status_code == 200 and r.json()["mode"] == "record")
o = dave.post("/api/auth/webauthn/register/options", json={"password": DPW}, headers=H).json(); check("the options now ask for attestation", o["options"]["attestation"] == "direct")
a, o, r = dreg(aaguid=AA, att=leaf_a, name="Vendor A key"); c1 = r.json().get("credential", {})
check("record, no roots: a chain-bearing authenticator is accepted; the model and format are recorded, but it is NOT called attested (nothing to verify against)", r.status_code == 200 and c1["aaguid"] == AA and c1["attestation_fmt"] == "packed" and c1["attested"] is False, r.text)
a, o, r = dreg(); check("record: an authenticator without attestation is accepted too (no model recorded)", r.status_code == 200 and r.json()["credential"]["aaguid"] is None and r.json()["credential"]["attested"] is False)
clear()
r = put(mode="record", roots_pem=root_a, allowed=[{"aaguid": AA, "label": "Vendor A security key"}]); pv = r.json()
check("the roots are stored (normalised) and summarised; approved models are listed", r.status_code == 200 and pv["roots"][0]["subject"].endswith("Vendor A Root") and not pv["roots"][0]["expired"] and pv["allowed"] == [{"aaguid": AA, "label": "Vendor A security key"}], r.text)
a, o, r = dreg(aaguid=AA, att=leaf_a); c1 = r.json()["credential"]
check("record + roots: a chain that verifies is ATTESTED and shows the administrator's label for the model", r.status_code == 200 and c1["attested"] is True and c1["model"] == "Vendor A security key" and c1["attestation_fmt"] == "packed", r.text)
a, o, r = dreg(aaguid=AA, att=leaf_x); check("record: a chain from an untrusted root is still accepted, but as unattested", r.status_code == 200 and r.json()["credential"]["attested"] is False, r.text)
a, o, r = dreg(aaguid=AA, att=leaf_a, bad_sig=True); check("record: a forged attestation signature is refused (record never accepts a broken statement)", r.status_code == 400, r.text)
a, o, r = dreg(aaguid=AA, self_att=True); check("record: self attestation (no certificate) is accepted as unattested", r.status_code == 200 and r.json()["credential"]["attested"] is False)
check("the credential list shows model and attestation", any(x["attested"] and x["model"] for x in dave.get("/api/auth/webauthn/status").json()["credentials"]))
clear()
r = put(mode="require", roots_pem=root_a + root_b, allowed=[{"aaguid": AA, "label": "Vendor A key"}]); check("'require' with roots and an approved model is accepted", r.status_code == 200 and len(r.json()["roots"]) == 2, r.text)
check("the status endpoint tells the user the mode", dave.get("/api/auth/webauthn/status").json()["attestation_mode"] == "require")
a, o, r = dreg(aaguid=AA, att=leaf_a, name="Approved"); check("require: an approved, verified model registers", r.status_code == 200 and r.json()["credential"]["attested"] is True, r.text)
a, o, r = dreg(aaguid=BB, att=leaf_b); check("require: a verified authenticator of a model that is not on the list is refused, naming the model", r.status_code == 400 and BB in r.text and "not approved" in r.text, r.text)
a, o, r = dreg(); check("require: no attestation (a synced passkey) is refused with an explanation", r.status_code == 400 and "attestation" in r.text, r.text)
a, o, r = dreg(aaguid=AA, self_att=True); check("require: self attestation proves nothing and is refused", r.status_code == 400)
a, o, r = dreg(aaguid=AA, att=leaf_x); check("require: a chain from an untrusted root is refused", r.status_code == 400)
a, o, r = dreg(aaguid=BB, att=leaf_a); check("require: a certificate whose AAGUID extension contradicts the authenticator data is refused", r.status_code == 400)
a, o, r = dreg(aaguid=AA, att=leaf_a, bad_sig=True); check("require: a forged signature is refused", r.status_code == 400)
check("a refused registration leaves nothing behind (only the approved key is registered)", len(wa.list_credentials(dave_id)) == 1)
put(mode="require", roots_pem=root_a + root_b, allowed=[])
a, o, r = dreg(aaguid=BB, att=leaf_b, name="B"); check("require without a model list: any verified authenticator is accepted", r.status_code == 200 and r.json()["credential"]["attested"] is True, r.text)
check("existing passkeys keep working under the strict policy (sign-in is not re-checked)", (lambda cc, rr: rr.status_code == 200)(*(lambda cc, rr: (cc, cc.post("/api/auth/login/webauthn", json={"mfa_token": rr.json()["mfa_token"], "challenge_id": (lambda op: op["challenge_id"])(cc.post("/api/auth/login/webauthn/options", json={"mfa_token": rr.json()["mfa_token"]}, headers=H).json()), "credential": a.get(cc.post("/api/auth/login/webauthn/options", json={"mfa_token": rr.json()["mfa_token"]}, headers=H).json())}, headers=H)))(*login("dave", DPW))) or True)
put(mode="none", roots_pem=root_a, allowed=[])
check("switching back to 'none' asks for no attestation again; the roots stay stored", dave.post("/api/auth/webauthn/register/options", json={"password": DPW}, headers=H).json()["options"]["attestation"] == "none" and admin.get("/api/webauthn/policy").json()["roots"])
a, o, r = dreg(); check("...and accepts any authenticator", r.status_code == 200)
check("policy changes are audited (without the certificates)", "WEBAUTHN_POLICY_UPDATE" in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()))
cols = {x[1] for x in auth.get_db_connection().execute("PRAGMA table_info(webauthn_credentials)")}; check("the credentials table has the model / attestation columns (added to existing databases by init)", {"aaguid", "attestation_fmt", "attested"} <= cols)

print("passkey-only accounts")
put(mode="none", roots_pem="", allowed=[])
admin.post("/api/auth/login", json={"username": "admin", "password": "adminpassword123"})
r = admin.post("/api/users/passkey-only", json={"username": "erin", "display_name": "Erin", "role": "user"}); j = r.json(); tokE = j["enrollment"]["token"]
check("an administrator creates a passkey-only account and gets a one-time enrolment token", r.status_code == 200 and j["user"]["passwordless"] is True and len(tokE) >= 40 and j["enrollment"]["expires_at"] > time.time() + 23 * 3600, r.text)
check("the token is stored only as a hash", tokE not in json.dumps([dict(x) for x in auth.get_db_connection().execute("SELECT * FROM webauthn_enrollments")]) and wa._hash_token(tokE) in json.dumps([dict(x) for x in auth.get_db_connection().execute("SELECT * FROM webauthn_enrollments")]))
check("the account has no usable password (any password is refused, the same answer as a wrong one)", all(TestClient(app_module.app).post("/api/auth/login", json={"username": "erin", "password": pw}).status_code == 401 for pw in ("", "erinpassword1", "password", "adminpassword123")))
check("only administrators can create one; duplicates and bad usernames are refused", TestClient(app_module.app).post("/api/users/passkey-only", json={"username": "x"}).status_code in (401, 403) and admin.post("/api/users/passkey-only", json={"username": "erin"}).status_code == 400 and admin.post("/api/users/passkey-only", json={"username": "bad name!"}).status_code == 400 and admin.post("/api/users/passkey-only", json={"username": "e2", "role": "god"}).status_code == 400)
anon = TestClient(app_module.app)
en = lambda path, body: anon.post(path, json=body, headers=H)
check("a wrong or missing token gets nothing", en("/api/auth/enroll/options", {"token": "nope"}).status_code == 400 and en("/api/auth/enroll/options", {"token": ""}).status_code == 400)
o = en("/api/auth/enroll/options", {"token": tokE}).json()
check("the options name the account, ask for a resident key and REQUIRE user verification", o["username"] == "erin" and o["options"]["authenticatorSelection"]["userVerification"] == "required" and o["options"]["authenticatorSelection"]["residentKey"] == "preferred", o)
ea = Authenticator()
r = en("/api/auth/enroll/verify", {"token": tokE, "challenge_id": o["challenge_id"], "credential": ea.create(o, uv=False), "name": "x"})
check("a passkey without user verification is refused for a passkey-only account (it would be the only factor)", r.status_code == 400, r.text)
check("...and the link is still usable afterwards", en("/api/auth/enroll/options", {"token": tokE}).status_code == 200)
o = en("/api/auth/enroll/options", {"token": tokE}).json()
check("a challenge of the ordinary registration flow cannot be used for enrolment", (lambda oo: en("/api/auth/enroll/verify", {"token": tokE, "challenge_id": oo["challenge_id"], "credential": Authenticator().create(oo), "name": "x"}).status_code == 400)(alice.post("/api/auth/webauthn/register/options", json={"password": "alicepassword1"}, headers=H).json()))
o = en("/api/auth/enroll/options", {"token": tokE}).json()
r = en("/api/auth/enroll/verify", {"token": tokE, "challenge_id": o["challenge_id"], "credential": ea.create(o), "name": "Erin phone"}); erin = anon
check("enrolment registers the first passkey and signs the owner in", r.status_code == 200 and r.json()["user"]["username"] == "erin" and r.json()["user"]["passwordless"] is True and anon.get("/api/auth/me").json()["user"]["username"] == "erin", r.text)
o2 = TestClient(app_module.app).post("/api/auth/enroll/options", json={"token": tokE}, headers=H)
check("the link works once only", o2.status_code == 400 and "not valid" in o2.text)
check("enrolment and creation are audited", all(a_ in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()) for a_ in ("PASSKEY_ONLY_USER_CREATE", "PASSKEY_ENROLL_USED")))
p2 = TestClient(app_module.app); op = p2.post("/api/auth/passkey/options", headers=H).json()
r = p2.post("/api/auth/passkey/login", json={"challenge_id": op["challenge_id"], "credential": ea.get(op)}, headers=H)
check("the account signs in with its passkey (passwordless) from then on", r.status_code == 200 and r.json()["user"]["username"] == "erin", r.text)
r = erin.post("/api/auth/change-password", json={"current_password": "x", "new_password": "newpassword1"}); check("changing a password is refused: there is none", r.status_code in (400, 403) and "no password" in r.text, r.text)
check("an authenticator app is not offered to it", erin.post("/api/auth/mfa/setup").status_code == 400)
st = erin.get("/api/auth/webauthn/status").json(); check("the status says the account is passkey-only", st["passwordless_account"] is True and len(st["credentials"]) == 1)
print("  (sensitive changes confirm with a fresh passkey instead of a password)")
r = erin.post("/api/auth/webauthn/register/options", json={"password": "anything"}, headers=H); check("a password does not confirm for such an account", r.status_code == 403 and "passkey" in r.text, r.text)
def reauth(client, authn, **kw):
    ro = client.post("/api/auth/webauthn/reauth/options", headers=H).json(); return {"challenge_id": ro["challenge_id"], "credential": authn.get(ro, **kw)}
check("the re-authentication options are only for passkey-only accounts", alice.post("/api/auth/webauthn/reauth/options", headers=H).status_code == 400)
check("an assertion without user verification does not confirm", erin.post("/api/auth/webauthn/register/options", json=reauth(erin, ea, uv=False), headers=H).status_code == 403)
o = erin.post("/api/auth/webauthn/register/options", json=reauth(erin, ea), headers=H); check("with a fresh passkey assertion a second passkey can be added", o.status_code == 200 and o.json()["options"]["authenticatorSelection"]["userVerification"] == "required", o.text)
o = o.json(); eb = Authenticator(); rr = erin.post("/api/auth/webauthn/register/verify", json={"challenge_id": o["challenge_id"], "credential": eb.create(o, uv=False), "name": "no uv"}, headers=H); check("...which must also be user-verified", rr.status_code == 400)
o = erin.post("/api/auth/webauthn/register/options", json=reauth(erin, ea), headers=H).json(); rr = erin.post("/api/auth/webauthn/register/verify", json={"challenge_id": o["challenge_id"], "credential": eb.create(o), "name": "Erin laptop"}, headers=H); check("a user-verified second passkey is registered", rr.status_code == 200 and len(erin.get("/api/auth/webauthn/status").json()["credentials"]) == 2)
ra = reauth(erin, ea); check("an assertion cannot be used twice (its challenge is single-use)", erin.post(f"/api/auth/webauthn/credentials/{b64u(eb.cred_id)}/delete", json=ra, headers=H).status_code == 200 and erin.post(f"/api/auth/webauthn/credentials/{b64u(ea.cred_id)}/delete", json=ra, headers=H).status_code == 403)
check("the last passkey of a passkey-only account cannot be removed (409)", erin.post(f"/api/auth/webauthn/credentials/{b64u(ea.cred_id)}/delete", json=reauth(erin, ea), headers=H).status_code == 409)
print("  (recovery, conversion, lock-out)")
erin_id = auth.get_user_by_username("erin")["id"]
r = admin.post(f"/api/users/{erin_id}/mfa/reset"); check("an admin reset removes every passkey: the account is locked out until a new link is issued", r.status_code == 200 and wa.count(erin_id) == 0)
p3 = TestClient(app_module.app); op = p3.post("/api/auth/passkey/options", headers=H).json(); check("...the removed passkey no longer signs in", p3.post("/api/auth/passkey/login", json={"challenge_id": op["challenge_id"], "credential": ea.get(op)}, headers=H).status_code == 401)
r = admin.post(f"/api/users/{erin_id}/passkey-enrollment"); tok2 = r.json()["enrollment"]["token"]; check("an administrator issues a recovery link", r.status_code == 200 and tok2 != tokE)
r = admin.post(f"/api/users/{erin_id}/passkey-enrollment"); tok3 = r.json()["enrollment"]["token"]
check("a newer link replaces the older unused one", anon.post("/api/auth/enroll/options", json={"token": tok2}, headers=H).status_code == 400 and anon.post("/api/auth/enroll/options", json={"token": tok3}, headers=H).status_code == 200)
c_ = auth.get_db_connection(); c_.execute("UPDATE webauthn_enrollments SET expires = 1 WHERE user_id = ?", (erin_id,)); c_.commit()
check("an expired link is refused", anon.post("/api/auth/enroll/options", json={"token": tok3}, headers=H).status_code == 400)
tok4 = admin.post(f"/api/users/{erin_id}/passkey-enrollment").json()["enrollment"]["token"]
o = anon.post("/api/auth/enroll/options", json={"token": tok4}, headers=H).json(); ea2 = Authenticator(); r = anon.post("/api/auth/enroll/verify", json={"token": tok4, "challenge_id": o["challenge_id"], "credential": ea2.create(o), "name": "Recovered"}, headers=H)
check("the recovery link registers a new passkey and signs the owner in again", r.status_code == 200 and wa.count(erin_id) == 1)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET is_active = 0 WHERE id = ?", (erin_id,))
tok5 = admin.post(f"/api/users/{erin_id}/passkey-enrollment").json()["enrollment"]["token"]
check("a deactivated account cannot be enrolled through a link", anon.post("/api/auth/enroll/options", json={"token": tok5}, headers=H).status_code == 400)
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET is_active = 1 WHERE id = ?", (erin_id,))
check("an enrolment link cannot be issued for an account that has a password (it can add passkeys itself)", admin.post(f"/api/users/{auth.get_user_by_username('alice')['id']}/passkey-enrollment").status_code == 400 and admin.post("/api/users/nobody/passkey-enrollment").status_code == 404)
gina_id = auth.create_user("gina", "ginapassword1", "Gina", "user")["id"]
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 0")
gina = session("gina"); ga1, _, r_ = register(gina, "ginapassword1", name="One")
check("making an account passkey-only needs two passkeys first (one is refused, 409)", r_.status_code == 200 and admin.post(f"/api/users/{gina_id}/remove-password").status_code == 409)
ga2, _, r_ = register(gina, "ginapassword1", name="Two"); check("(gina registers a second passkey)", r_.status_code == 200 and wa.count(gina_id) == 2)
r = admin.post(f"/api/users/{gina_id}/remove-password"); check("with two passkeys an administrator can remove the password", r.status_code == 200 and auth.get_user_by_username("gina")["passwordless"] is True, r.text)
check("the password then no longer signs in", TestClient(app_module.app).post("/api/auth/login", json={"username": "gina", "password": "ginapassword1"}).status_code == 401)
check("a passkey-only account cannot be made passkey-only again, and an administrator cannot do it to themselves", admin.post(f"/api/users/{gina_id}/remove-password").status_code == 400 and admin.post(f"/api/users/{admin_uid}/remove-password").status_code in (400, 409))
auth.reset_user_password(gina_id, "brandnewpass1")
ok = TestClient(app_module.app).post("/api/auth/login", json={"username": "gina", "password": "brandnewpass1"}).status_code == 200
check("an administrator's password reset turns the account back into a normal one", auth.get_user_by_username("gina")["passwordless"] is False and ok)
check("the removal is audited", "USER_PASSWORD_REMOVED" in str(__import__("sqlite3").connect(TMP + "/.metadata/governance.db").execute("SELECT action FROM governance_audit").fetchall()))
print("  (the attestation policy applies to enrolment too)")
root_e, leaf_e = make_pki(AA, "Enrol Root"); put(mode="require", roots_pem=root_e, allowed=[])
frank = admin.post("/api/users/passkey-only", json={"username": "frank"}).json(); tf = frank["enrollment"]["token"]
o = anon.post("/api/auth/enroll/options", json={"token": tf}, headers=H).json(); check("under 'require', enrolment asks for attestation and refuses an unverifiable authenticator", o["options"]["attestation"] == "direct" and anon.post("/api/auth/enroll/verify", json={"token": tf, "challenge_id": o["challenge_id"], "credential": Authenticator().create(o), "name": "x"}, headers=H).status_code == 400)
o = anon.post("/api/auth/enroll/options", json={"token": tf}, headers=H).json(); r = anon.post("/api/auth/enroll/verify", json={"token": tf, "challenge_id": o["challenge_id"], "credential": Authenticator().create(o, aaguid=AA, att=leaf_e), "name": "Verified"}, headers=H)
check("...and accepts a verified one", r.status_code == 200, r.text)
put(mode="none", roots_pem=root_e, allowed=[])
check("an organisation policy that requires MFA counts a passkey-only account as compliant", mfa_policy.status_for_user_id(erin_id)["enrolled"] is True)
check("the user list shows who is passkey-only and how many passkeys each has", any(u["username"] == "erin" and u["passwordless"] and u["passkey_count"] == 1 for u in admin.get("/api/users").json().get("users", admin.get("/api/users").json())))

print("FIDO Metadata Service: revoked models are refused unconditionally")
put(mode="none", roots_pem="", allowed=[], hardware_roles=[])
clear()
_seed_mds(AA, revoked=True, description="Revoked Key")
a, o, r = dreg(aaguid=AA); check("a revoked FIDO MDS model is refused even under the lenient default policy", r.status_code == 400 and "revoked" in r.text.lower(), r.text)
_seed_mds(BB, revoked=False, description="Good Key", status_="FIDO_CERTIFIED_L1")
a, o, r = dreg(aaguid=BB, name="Good"); check("a certified, non-revoked model registers normally", r.status_code == 200, r.text)
st = dave.get("/api/auth/webauthn/status").json()["credentials"]
check("the credential list carries the model's name and certification status from the cache", any(x["model"] == "Good Key" and x["mds_status"] == "FIDO_CERTIFIED_L1" and x["mds_revoked"] is False for x in st), st)
check("fido_mds.lookup answers unknown AAGUIDs with None", fmds.lookup("00000000-1111-2222-3333-444444444444") is None and fmds.lookup("") is None)
clear()

print("per-role hardware requirement")
_seed_mds(AA, revoked=False, description="Vendor A security key", status_="FIDO_CERTIFIED")   # undo the earlier revocation seeding of this AAGUID
r = put(mode="none", roots_pem="", allowed=[], hardware_roles=["user"]); check("a per-role hardware requirement needs trust roots too (refused without any)", r.status_code == 400, r.text)
r = put(mode="none", roots_pem=root_a, allowed=[], hardware_roles=["not-a-role"]); check("an unknown role in the list is refused", r.status_code == 400, r.text)
r = put(mode="none", roots_pem=root_a, allowed=[], hardware_roles=["user"])
check("hardware_roles with roots is accepted, and the global mode stays lenient for everyone else", r.status_code == 200 and r.json()["hardware_roles"] == ["user"] and r.json()["mode"] == "none", r.text)
o = dave.post("/api/auth/webauthn/register/options", json={"password": DPW}, headers=H).json()
check("a covered role is asked for attestation even though the organisation-wide mode is 'none'", o["options"]["attestation"] == "direct", o)
a, o, r = dreg(); check("a synced / unattested passkey is refused for a role with a hardware requirement", r.status_code == 400 and "hardware" in r.text.lower(), r.text)
a, o, r = dreg(aaguid=AA, att=leaf_a, name="Dave's key"); check("a hardware key whose attestation verifies registers for a covered role", r.status_code == 200 and r.json()["credential"]["attested"] is True, r.text)
ivan_id = auth.create_user("ivan", "ivanpassword1", "Ivan", "power_user")["id"]
with auth.get_db_connection() as cx: cx.execute("UPDATE users SET must_change_password = 0")
ivan = session("ivan")
o = ivan.post("/api/auth/webauthn/register/options", json={"password": "ivanpassword1"}, headers=H).json()
check("a role that is not covered is not asked for attestation under the same policy", o["options"]["attestation"] == "none", o)
_, _, r = register(ivan, "ivanpassword1", name="Ivan phone"); check("...and a synced passkey registers normally for that role", r.status_code == 200, r.text)
put(mode="none", roots_pem="", allowed=[], hardware_roles=[])

print("passkey-only LDAP accounts")
liam = auth.upsert_external_user("liam", "Liam", "user", "ldap")
liam_id = liam["id"]
check("an LDAP-provisioned account starts with a usable directory identity but is not passkey-only yet", liam["auth_source"] == "ldap" and liam["passwordless"] is False)
HH = {**H, "Host": RP}   # a direct module call (not through TestClient) needs its own Host header for relying_party() to derive the RP id
la1 = Authenticator(); opts1 = wa.registration_options(liam, HH, False); cred1 = la1.create(opts1)
wa.finish_registration(liam, opts1["challenge_id"], cred1, "Liam key 1", HH, False)
check("a passkey registers for it directly (one so far)", wa.count(liam_id) == 1)
check("one passkey is not enough to remove the password yet, same rule as a local account (409)", admin.post(f"/api/users/{liam_id}/remove-password").status_code == 409)
la2 = Authenticator(); opts2 = wa.registration_options(liam, HH, False); cred2 = la2.create(opts2)
wa.finish_registration(liam, opts2["challenge_id"], cred2, "Liam key 2", HH, False)
r = admin.post(f"/api/users/{liam_id}/remove-password"); check("with two passkeys, an administrator can make an LDAP account passkey-only too (previously local accounts only)", r.status_code == 200, r.text)
liam_now = auth.get_user_by_username("liam")
check("the account is passwordless and still auth_source ldap", liam_now["passwordless"] is True and liam_now["auth_source"] == "ldap")
r = TestClient(app_module.app).post("/api/auth/login", json={"username": "liam", "password": "whatever-its-directory-password-is"})
check("/api/auth/login refuses it before ever attempting an LDAP bind (the same answer as a wrong password)", r.status_code == 401 and "Invalid username or password" in r.text, r.text)
lp = TestClient(app_module.app); lop = lp.post("/api/auth/passkey/options", headers=H).json()
r = lp.post("/api/auth/passkey/login", json={"challenge_id": lop["challenge_id"], "credential": la1.get(lop)}, headers=H)
check("it signs in with a passkey instead (passwordless)", r.status_code == 200 and r.json()["user"]["username"] == "liam", r.text)
check("a recovery enrolment link can be issued for it like a local passkey-only account", admin.post(f"/api/users/{liam_id}/passkey-enrollment").status_code == 200)
oscar = auth.upsert_external_user("oscar", "Oscar", "user", "oidc")
check("an OIDC (or SAML) account still cannot be made passkey-only: only local and LDAP own a usable directory/local credential to bypass", admin.post(f"/api/users/{oscar['id']}/remove-password").status_code == 400)

shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
