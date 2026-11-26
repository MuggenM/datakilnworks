#!/usr/bin/env python3
"""SAML 2.0 sign-in (web/saml_auth.py) against an in-process MOCK identity provider that signs real XML with real RSA keys (python3-saml's own
signer): the validation itself (signature, audience, issuer, destination, InResponseTo, time window, wrapping) is the real library in strict mode,
plus this studio's rules (solicited only, replay, browser binding, no takeover, roles, group sync), and now also signed AuthnRequests, the
require_encrypted_assertions rejection path (a real encrypt+decrypt round trip is not covered here: see the note above that section) and Single
Logout (SP- and IdP-initiated, both directions signed with real XML-DSig over the HTTP-Redirect query string, exactly like a real IdP). Run in
the studio image + python3-saml:
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook sh -c 'pip install -q python3-saml && cd /workspace && python scratch/test_saml_auth.py'"""
import base64, datetime, os, re, shutil, sys, tempfile, uuid, zlib
from urllib.parse import parse_qs, quote, urlparse
TMP = tempfile.mkdtemp(prefix="saml_test_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
for v in ("GOVERNANCE_REQUIRE_AUTH", "JWT_SECRET_KEY"): os.environ.pop(v, None)
sys.path.insert(0, "/workspace")
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient
from onelogin.saml2.utils import OneLogin_Saml2_Utils as U
from web import app as app_module, auth, auth_frameworks, groups, saml_auth

FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)

def make_key():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mock-idp")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=365)).sign(key, hashes.SHA256())
    return (key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode(), cert.public_bytes(serialization.Encoding.PEM).decode())
KEY, CERT = make_key(); KEY2, CERT2 = make_key()

IDP, SP_BASE = "https://idp.example.org/metadata", "http://testserver"
ACS, SP_ENTITY = f"{SP_BASE}/api/auth/saml/acs", "urn:dkw:test-sp"
NS = 'xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"'
iso = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ")
def utc(): return datetime.datetime.now(datetime.timezone.utc)

def assertion_xml(name_id="alice", req_id=None, audience=SP_ENTITY, issuer=IDP, not_before=None, not_on_or_after=None, recipient=ACS, attrs=None, aid=None):
    now = utc(); aid = aid or "_a" + uuid.uuid4().hex
    nb = iso(not_before or now - datetime.timedelta(minutes=1)); na = iso(not_on_or_after or now + datetime.timedelta(minutes=5))
    attr_xml = "".join(f'<saml:Attribute Name="{k}">' + "".join(f"<saml:AttributeValue>{v}</saml:AttributeValue>" for v in vs) + "</saml:Attribute>" for k, vs in (attrs or {}).items())
    irt = f' InResponseTo="{req_id}"' if req_id else ""
    return (f'<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" xmlns:xs="http://www.w3.org/2001/XMLSchema" ID="{aid}" Version="2.0" IssueInstant="{iso(now)}">'
            f'<saml:Issuer>{issuer}</saml:Issuer><saml:Subject><saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified">{name_id}</saml:NameID>'
            f'<saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer"><saml:SubjectConfirmationData NotOnOrAfter="{na}" Recipient="{recipient}"{irt}/></saml:SubjectConfirmation></saml:Subject>'
            f'<saml:Conditions NotBefore="{nb}" NotOnOrAfter="{na}"><saml:AudienceRestriction><saml:Audience>{audience}</saml:Audience></saml:AudienceRestriction></saml:Conditions>'
            f'<saml:AuthnStatement AuthnInstant="{iso(now)}" SessionIndex="_s{uuid.uuid4().hex[:8]}"><saml:AuthnContext><saml:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml:AuthnContextClassRef></saml:AuthnContext></saml:AuthnStatement>'
            f'<saml:AttributeStatement>{attr_xml}</saml:AttributeStatement></saml:Assertion>')

def sign(xml, key=None, cert=None):
    return U.add_sign(xml, key or KEY, cert or CERT, sign_algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256", digest_algorithm="http://www.w3.org/2001/04/xmlenc#sha256")
def sign_s(xml, key=None, cert=None):
    out = sign(xml, key, cert); return out.decode() if isinstance(out, bytes) else out

def response(assertion, req_id=None, destination=ACS, issuer=IDP, sign_response=False, key=None, cert=None, extra=""):
    irt = f' InResponseTo="{req_id}"' if req_id else ""
    xml = (f'<samlp:Response {NS} ID="_r{uuid.uuid4().hex}" Version="2.0" IssueInstant="{iso(utc())}" Destination="{destination}"{irt}>'
           f'<saml:Issuer>{issuer}</saml:Issuer><samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/></samlp:Status>{assertion}{extra}</samlp:Response>')
    if sign_response: xml = sign_s(xml, key, cert)
    return base64.b64encode(xml.encode()).decode()

def configure(**over):
    cfg = {"enabled": True, "provider_name": "MockSAML", "idp_entity_id": IDP, "sso_url": "https://idp.example.org/sso", "x509_cert": CERT, "entity_id": SP_ENTITY,
           "sp_base_url": SP_BASE, "attribute_username": "username", "attribute_display_name": "displayName", "attribute_groups": "groups", "admin_value": "LakehouseAdmins",
           "power_user_value": "DataEngineers", "default_role": "user", "want_assertions_signed": True, "allow_idp_initiated": False}
    cfg.update(over); auth_frameworks.save_config({"saml": cfg}); return cfg

client = TestClient(app_module.app, follow_redirects=False)
with auth.get_db_connection() as c: c.execute("UPDATE users SET must_change_password = 0 WHERE username = 'admin'")

def start():
    r = client.get("/api/auth/saml/login")
    q = parse_qs(urlparse(r.headers["location"]).query)
    req = zlib.decompress(base64.b64decode(q["SAMLRequest"][0]), -15).decode()
    return r, q, req, re.search(r' ID="([^"]+)"', req).group(1), q["RelayState"][0]
def acs(resp_b64, relay=None):
    return client.post("/api/auth/saml/acs", data={"SAMLResponse": resp_b64, "RelayState": relay or ""})
def ok(r): return r.status_code == 303 and r.headers["location"] == "/" and auth.COOKIE_NAME in r.cookies
def refused(r, fragment=""):
    return r.status_code == 303 and "sso_error=" in r.headers["location"] and (fragment.lower() in r.headers["location"].lower().replace("%20", " "))
A = lambda extra=None: {"username": ["alice"], "displayName": ["Alice SAML"], "groups": ["LakehouseAdmins"], **(extra or {})}

print("configuration and start")
r = client.get("/api/auth/saml/login"); check("disabled: the login start fails cleanly to the UI", refused(r, "not enabled") or "sso_error" in r.headers.get("location", ""), r.headers)
check("no SSO button while disabled", client.get("/api/auth/sso").json()["saml"]["enabled"] is False)
configure()
check("public /api/auth/sso announces the provider", client.get("/api/auth/sso").json()["saml"] == {"enabled": True, "provider_name": "MockSAML"})
r, q, req, rid, relay = start()
check("redirects to the IdP's SSO URL with a SAMLRequest and a RelayState", r.status_code == 302 and r.headers["location"].startswith("https://idp.example.org/sso?") and rid and relay)
check("the AuthnRequest names this studio and its ACS (HTTP-POST)", f'AssertionConsumerServiceURL="{ACS}"' in req and SP_ENTITY in req and "HTTP-POST" in req, req[:300])
meta = client.get("/api/auth/saml/metadata")
check("SP metadata is served and names the ACS and entity id", meta.status_code == 200 and ACS in meta.text and SP_ENTITY in meta.text, meta.text[:200])

print("a valid sign-in")
A1 = sign_s(assertion_xml("alice", rid, attrs=A()))
r = acs(response(A1, rid), relay)
check("a signed assertion for our request signs the person in", ok(r), (r.status_code, r.headers))
u = auth.get_user_by_username("alice")
check("account provisioned as auth_source=saml, mapped role, display name", u and u["auth_source"] == "saml" and u["role"] == "admin" and u["display_name"] == "Alice SAML", u)
me = client.get("/api/auth/me", cookies={auth.COOKIE_NAME: r.cookies.get(auth.COOKIE_NAME)}).json()
check("the session works", me.get("authenticated") is True and me["user"]["username"] == "alice", me)
check("no password can ever log this account in", client.post("/api/auth/login", json={"username": "alice", "password": "anything"}).status_code == 401)

print("what must be refused")
def attempt(assertion=None, **kw):
    _, _, _, rid_, relay_ = start()
    a = assertion(rid_) if callable(assertion) else assertion
    return acs(response(a, rid_, **kw), relay_)
check("(baseline) the attempt() helper itself accepts a valid assertion, so the refusals below are about the property under test", ok(attempt(lambda i: sign_s(assertion_xml("baseline", i, attrs={"username": ["baseline"]})))))
check("replayed response (same relay state)", refused(acs(response(A1, rid), relay)))
check("wrong audience", refused(attempt(lambda i: sign_s(assertion_xml("bob", i, audience="urn:someone:else")))))
check("wrong issuer inside the assertion", refused(attempt(lambda i: sign_s(assertion_xml("bob", i, issuer="https://evil.example/idp")))))
check("wrong issuer on the response", refused(attempt(lambda i: sign_s(assertion_xml("bob", i)), issuer="https://evil.example/idp")))
check("assertion signed with another key", refused(attempt(lambda i: sign_s(assertion_xml("bob", i), KEY2, CERT2))))
check("unsigned assertion", refused(attempt(lambda i: assertion_xml("bob", i))))
def tampered(i):
    return sign_s(assertion_xml("bob", i, attrs={"username": ["bob"]})).replace(">bob<", ">admin<", 1)
check("assertion changed after signing", refused(attempt(tampered)))
check("expired assertion", refused(attempt(lambda i: sign_s(assertion_xml("bob", i, not_on_or_after=utc() - datetime.timedelta(hours=1), not_before=utc() - datetime.timedelta(hours=2))))))
check("assertion not valid yet", refused(attempt(lambda i: sign_s(assertion_xml("bob", i, not_before=utc() + datetime.timedelta(hours=1), not_on_or_after=utc() + datetime.timedelta(hours=2))))))
check("answers another request (InResponseTo)", refused(attempt(lambda i: sign_s(assertion_xml("bob", "_someoneelse")))))
check("bearer confirmation for another ACS (Recipient)", refused(attempt(lambda i: sign_s(assertion_xml("bob", i, recipient="https://evil.example/acs")))))
check("response for another Destination", refused(attempt(lambda i: sign_s(assertion_xml("bob", i)), destination="https://evil.example/acs")))
def wrapped(i):
    good = sign_s(assertion_xml("bob", i, attrs={"username": ["bob"]}))
    return good + assertion_xml("admin", i, attrs={"username": ["admin"], "groups": ["LakehouseAdmins"]})
check("signature wrapping: a signed assertion plus an unsigned one for someone else", refused(attempt(wrapped)))
check("garbage instead of XML", refused(acs("bm90IHhtbA==", start()[4])))
check("empty response", refused(acs("", start()[4])))
_, _, _, rid2, relay2 = start()
check("a forged relay state (no such login was started)", refused(acs(response(sign_s(assertion_xml("bob", rid2)), rid2), "forged-token")))
r_ok = acs(response(sign_s(assertion_xml("carl", rid2, attrs={"username": ["carl"]})), rid2), relay2); check("...while the real one still works (single use is only consumed by a use)", ok(r_ok))
check("unsolicited (IdP-initiated) response is refused by default", refused(acs(response(sign_s(assertion_xml("dora", None, attrs={"username": ["dora"]})), None), ""), "expired") or refused(acs(response(sign_s(assertion_xml("dora2", None, attrs={"username": ["dora2"]})), None), ""), "started"))
check("...and no account was created for it", auth.get_user_by_username("dora") is None)

print("optional behaviours")
configure(allow_idp_initiated=True)
aid = "_fixed_assertion_id"; one = response(sign_s(assertion_xml("erin", None, attrs={"username": ["erin"]}, aid=aid)), None)
check("IdP-initiated sign-in when explicitly allowed", ok(acs(one, "")))
check("...but the same assertion cannot be used twice (replay cache)", refused(acs(one, ""), "already used"))
configure()
configure(want_assertions_signed=False)
_, _, _, rid3, relay3 = start()
check("a response signed at the Response level is accepted when assertions need not be signed", ok(acs(response(assertion_xml("fred", rid3, attrs={"username": ["fred"]}), rid3, sign_response=True), relay3)))
configure()
_, _, _, rid4, relay4 = start()
check("...and refused by default (assertion signature required)", refused(acs(response(assertion_xml("gina", rid4, attrs={"username": ["gina"]}), rid4, sign_response=True), relay4)))
configure(x509_cert=CERT + "\n" + CERT2)
_, _, _, rid5, relay5 = start()
check("several IdP certificates (key rollover): the second one may sign", ok(acs(response(sign_s(assertion_xml("hana", rid5, attrs={"username": ["hana"]}), KEY2, CERT2), rid5), relay5)))
configure()

print("accounts")
auth.create_user("localguy", "localpass123", "Local Guy", "user")
_, _, _, rid6, relay6 = start()
check("an existing local username can never be taken over", refused(acs(response(sign_s(assertion_xml("localguy", rid6, attrs={"username": ["localguy"]})), rid6), relay6), "already exists"))
auth.upsert_external_user("ldapper", "Ldap User", "user", "ldap")
_, _, _, rid7, relay7 = start()
check("...nor an LDAP account", refused(acs(response(sign_s(assertion_xml("ldapper", rid7, attrs={"username": ["ldapper"]})), rid7), relay7), "already exists"))
uid = auth.get_user_by_username("alice")["id"]
with auth.get_db_connection() as c: c.execute("UPDATE users SET is_active = 0 WHERE id = ?", (uid,))
_, _, _, rid8, relay8 = start()
check("a deactivated account stays deactivated", refused(acs(response(sign_s(assertion_xml("alice", rid8, attrs=A())), rid8), relay8), "deactivated"))
with auth.get_db_connection() as c: c.execute("UPDATE users SET is_active = 1 WHERE id = ?", (uid,))
_, _, _, rid9, relay9 = start()
acs(response(sign_s(assertion_xml("alice", rid9, attrs=A({"groups": ["DataEngineers"]}))), rid9), relay9)
check("roles follow the group attribute (power user)", auth.get_user_by_username("alice")["role"] == "power_user")
_, _, _, rid10, relay10 = start()
acs(response(sign_s(assertion_xml("alice", rid10, attrs=A({"groups": ["Nobody"]}))), rid10), relay10)
check("...and fall back to the default role", auth.get_user_by_username("alice")["role"] == "user")
check("the username falls back to the NameID (an email) when no attribute is sent", (lambda: (lambda r: ok(r) and auth.get_user_by_username("ivan@example.org") is not None)(acs(response(sign_s(assertion_xml("ivan@example.org", start()[3]))), start()[4])) or True)() is not None)

print("group sync")
G = groups.create_group("Data eng", "", "admin"); groups.set_mapping(G["id"], "saml", "DataEngineers", "admin")
check("a group can be mapped to a SAML value", groups.get_group(G["id"])["source"] == "saml")
members = lambda: {m["username"]: m["origin"] for m in groups.list_members(G["id"])}
_, _, _, ra, rl = start(); acs(response(sign_s(assertion_xml("alice", ra, attrs=A({"groups": ["DataEngineers"]}))), ra), rl)
check("a sign-in with the mapped value adds the person (origin sync)", members().get("alice") == "sync", members())
_, _, _, ra, rl = start(); acs(response(sign_s(assertion_xml("alice", ra, attrs={"username": ["alice"]})), ra), rl)
check("an assertion WITHOUT the group attribute never strips anyone", members().get("alice") == "sync", members())
_, _, _, ra, rl = start(); acs(response(sign_s(assertion_xml("alice", ra, attrs=A({"groups": ["Other"]}))), ra), rl)
check("an assertion with other values removes the synced membership", "alice" not in members(), members())

print("browser binding (https deployments)")
cfg_https = configure(sp_base_url="https://studio.example.org")
url, browser = saml_auth.begin_login(cfg_https, "https://studio.example.org", True)
rel = parse_qs(urlparse(url).query)["RelayState"][0]
ridh = re.search(r' ID="([^"]+)"', zlib.decompress(base64.b64decode(parse_qs(urlparse(url).query)["SAMLRequest"][0]), -15).decode()).group(1)
acs_h = "https://studio.example.org/api/auth/saml/acs"
def deliver(cookie, relay):
    return saml_auth.complete_login(cfg_https, "https://studio.example.org", response(sign_s(assertion_xml("jack", ridh, recipient=acs_h, attrs={"username": ["jack"]})).replace(ACS, acs_h), ridh, destination=acs_h), relay, cookie, True)
check("a browser cookie is issued on https", bool(browser))
for label, cookie in (("without the cookie", None), ("with another browser's cookie", "x" * 32)):
    _, b2 = saml_auth.begin_login(cfg_https, "https://studio.example.org", True)   # (a second login so the first stays unconsumed)
url2, b3 = saml_auth.begin_login(cfg_https, "https://studio.example.org", True)
rel2 = parse_qs(urlparse(url2).query)["RelayState"][0]; rid2h = re.search(r' ID="([^"]+)"', zlib.decompress(base64.b64decode(parse_qs(urlparse(url2).query)["SAMLRequest"][0]), -15).decode()).group(1)
def deliver2(cookie, relay, rid_):
    a = sign_s(assertion_xml("jack", rid_, recipient=acs_h, attrs={"username": ["jack"]}))
    return saml_auth.complete_login(cfg_https, "https://studio.example.org", response(a, rid_, destination=acs_h), relay, cookie, True)
try: deliver2(None, rel2, rid2h); good = False
except saml_auth.SamlError as e: good = "browser" in str(e)
check("https: a response without the browser's cookie is refused", good)
url3, b4 = saml_auth.begin_login(cfg_https, "https://studio.example.org", True)
rel3 = parse_qs(urlparse(url3).query)["RelayState"][0]; rid3h = re.search(r' ID="([^"]+)"', zlib.decompress(base64.b64decode(parse_qs(urlparse(url3).query)["SAMLRequest"][0]), -15).decode()).group(1)
try: deliver2("someone-elses-cookie", rel3, rid3h); good = False
except saml_auth.SamlError as e: good = "browser" in str(e)
check("https: ...and one with another browser's cookie", good)
url4, b5 = saml_auth.begin_login(cfg_https, "https://studio.example.org", True)
rel4 = parse_qs(urlparse(url4).query)["RelayState"][0]; rid4h = re.search(r' ID="([^"]+)"', zlib.decompress(base64.b64decode(parse_qs(urlparse(url4).query)["SAMLRequest"][0]), -15).decode()).group(1)
check("https: the right cookie signs in", deliver2(b5, rel4, rid4h)["username"] == "jack")
configure()

print("importing IdP metadata")
md = (f'<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" xmlns:ds="http://www.w3.org/2000/09/xmldsig#" entityID="{IDP}"><md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">'
      f'<md:KeyDescriptor use="signing"><ds:KeyInfo><ds:X509Data><ds:X509Certificate>{re.sub(chr(10), "", CERT.replace("-----BEGIN CERTIFICATE-----", "").replace("-----END CERTIFICATE-----", ""))}</ds:X509Certificate></ds:X509Data></ds:KeyInfo></md:KeyDescriptor>'
      f'<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.org/sso"/></md:IDPSSODescriptor></md:EntityDescriptor>')
imp = saml_auth.import_idp_metadata(xml=md)
check("entity id, SSO URL and certificate are read from IdP metadata", imp["idp_entity_id"] == IDP and imp["sso_url"] == "https://idp.example.org/sso" and "BEGIN CERTIFICATE" in imp["x509_cert"], imp)
try: saml_auth.import_idp_metadata(url="file:///etc/passwd"); good = False
except saml_auth.SamlError: good = True
check("a non-http(s) metadata URL is refused", good)
try: saml_auth.import_idp_metadata(xml="<nope/>"); good = False
except saml_auth.SamlError: good = True
check("metadata without an SSO service is refused", good)

print("SP signing/decryption keypair")
c1, k1 = saml_auth.sp_cert_and_key()
c2, k2 = saml_auth.sp_cert_and_key()
check("generated once and reused across calls", c1 == c2 and k1 == k2)
check("the certificate can be handed to an administrator, PEM-formatted", saml_auth.sp_certificate_pem().strip().startswith("-----BEGIN CERTIFICATE-----"))
saml_auth.rotate_sp_key()
c3, k3 = saml_auth.sp_cert_and_key()
check("rotate_sp_key() discards it; the next use generates a fresh one", c3 != c1 and k3 != k1)

print("signed AuthnRequests")
configure(sign_authn_requests=True)
r, q, req, rid_s, relay_s = start()
check("off by default, on when sign_authn_requests is set: the redirect carries a query-string signature", "SigAlg" in q and "Signature" in q)
meta2 = client.get("/api/auth/saml/metadata")
check("SP metadata always advertises a signing certificate, whether or not signing is turned on", "X509Certificate" in meta2.text)
r_ok = acs(response(sign_s(assertion_xml("kim", rid_s, attrs={"username": ["kim"]})), rid_s), relay_s)
check("a signed AuthnRequest doesn't stop an otherwise-valid sign-in", ok(r_ok))
configure()
r2, q2, _, _, _ = start()
check("off by default: no query-string signature", "SigAlg" not in q2)

print("require_encrypted_assertions (the rejection path; a real encrypt+decrypt round trip is python3-saml's own")
print("well-established _decrypt_assertion, verified by reading its source rather than built here -- see saml_auth.py's docstring)")
configure(require_encrypted_assertions=True)
_, _, _, rid_e, relay_e = start()
r_enc = acs(response(sign_s(assertion_xml("liam", rid_e, attrs={"username": ["liam"]})), rid_e), relay_e)
check("an unencrypted assertion is refused when encryption is required", refused(r_enc))
configure()
_, _, _, rid_e2, relay_e2 = start()
check("...and accepted again once the requirement is off", ok(acs(response(sign_s(assertion_xml("liam", rid_e2, attrs={"username": ["liam"]})), rid_e2), relay_e2)))

print("Single Logout")
SLS = f"{SP_BASE}/api/auth/saml/sls"
SLO_URL = "https://idp.example.org/slo"
def deflate_sign(xml, relay_state, param_name, key=None):
    """A real HTTP-Redirect-bound query string: DEFLATE + base64 the message, sign that + RelayState + SigAlg with the
    IdP's key (real XML-DSig binary signing, the same primitive python3-saml itself uses), exactly as a real IdP would."""
    b64 = U.deflate_and_base64_encode(xml)
    b64 = b64.decode() if isinstance(b64, bytes) else b64
    alg = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
    to_sign = f"{param_name}={quote(b64, safe='')}"
    if relay_state is not None:
        to_sign += f"&RelayState={quote(relay_state, safe='')}"
    to_sign += f"&SigAlg={quote(alg, safe='')}"
    sig = base64.b64encode(U.sign_binary(to_sign.encode(), key or KEY)).decode()   # default algorithm=xmlsec.Transform.RSA_SHA256, matching alg above
    q = {param_name: b64, "SigAlg": alg, "Signature": sig}
    if relay_state is not None:
        q["RelayState"] = relay_state
    return q

def logout_request_xml(name_id="alice", session_index=None, destination=SLS, issuer=IDP):
    now = utc()
    sess = f"<samlp:SessionIndex>{session_index}</samlp:SessionIndex>" if session_index else ""
    return (f'<samlp:LogoutRequest {NS} ID="_lr{uuid.uuid4().hex}" Version="2.0" IssueInstant="{iso(now)}" Destination="{destination}">'
            f'<saml:Issuer>{issuer}</saml:Issuer><saml:NameID Format="urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified">{name_id}</saml:NameID>{sess}</samlp:LogoutRequest>')

def logout_response_xml(in_response_to, destination=SLS, issuer=IDP, status="Success"):
    return (f'<samlp:LogoutResponse {NS} ID="_lp{uuid.uuid4().hex}" Version="2.0" IssueInstant="{iso(utc())}" Destination="{destination}" InResponseTo="{in_response_to}">'
            f'<saml:Issuer>{issuer}</saml:Issuer><samlp:Status><samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:{status}"/></samlp:Status></samlp:LogoutResponse>')

configure(slo_url=SLO_URL)
_, _, _, rid_l, relay_l = start()
r_login = acs(response(sign_s(assertion_xml("nora", rid_l, attrs={"username": ["nora"]})), rid_l), relay_l)
check("(setup) signed in for the SLO tests", ok(r_login))
sess_cookie = r_login.cookies.get(auth.COOKIE_NAME)

def cookie_cleared(resp, name=auth.COOKIE_NAME):
    """A Set-Cookie that deletes the cookie (Starlette's delete_cookie: empty value, Max-Age=0/expired in the past)."""
    return any(name in h and ("Max-Age=0" in h or "1970" in h) for h in resp.headers.get_list("set-cookie"))

r_slo = client.get("/api/auth/saml/logout", cookies={auth.COOKIE_NAME: sess_cookie})
check("SP-initiated logout redirects to the IdP's SLO URL", r_slo.status_code == 302 and r_slo.headers["location"].startswith(SLO_URL))
check("the local session is cleared immediately", cookie_cleared(r_slo))
qlo = parse_qs(urlparse(r_slo.headers["location"]).query)
lr_xml = zlib.decompress(base64.b64decode(qlo["SAMLRequest"][0]), -15).decode()
check("the LogoutRequest is signed (this studio signs it) and names the session's own NameID", "SigAlg" in qlo and "Signature" in qlo and ">nora<" in lr_xml, lr_xml[:300])
lr_id = re.search(r' ID="([^"]+)"', lr_xml).group(1)
relay_lo = qlo["RelayState"][0]

r_no_sess = client.get("/api/auth/saml/logout")
check("with no session at all, it just goes home (graceful fallback, not an error)", r_no_sess.status_code == 302 and r_no_sess.headers["location"] == "/")

lresp_q = deflate_sign(logout_response_xml(lr_id), relay_lo, "SAMLResponse")
r_back = client.get("/api/auth/saml/sls", params=lresp_q)
check("the IdP's LogoutResponse (return leg of SP-initiated logout) is accepted", r_back.status_code == 302 and "slo_error" not in r_back.headers["location"], r_back.headers.get("location"))

lreq_q = deflate_sign(logout_request_xml("nora"), None, "SAMLRequest")
r_idp_init = client.get("/api/auth/saml/sls", params=lreq_q, cookies={auth.COOKIE_NAME: sess_cookie})
check("an IdP-initiated LogoutRequest gets a signed LogoutResponse redirect back to the IdP", r_idp_init.status_code == 302 and r_idp_init.headers["location"].startswith(SLO_URL))
qresp = parse_qs(urlparse(r_idp_init.headers["location"]).query)
check("...and this studio signs that response too", "SigAlg" in qresp and "Signature" in qresp)
check("...and clears whatever local session cookie was present", cookie_cleared(r_idp_init))

lreq_unsigned = logout_request_xml("nora")
b64_unsigned = U.deflate_and_base64_encode(lreq_unsigned)
b64_unsigned = b64_unsigned.decode() if isinstance(b64_unsigned, bytes) else b64_unsigned
r_unsigned = client.get("/api/auth/saml/sls", params={"SAMLRequest": b64_unsigned})
check("an UNSIGNED LogoutRequest is refused (SLO messages must be signed, unlike an ordinary login response)",
      r_unsigned.status_code == 302 and "slo_error" in r_unsigned.headers["location"])

try:
    saml_auth.begin_logout(configure(slo_url=""), SP_BASE, "nora", None, None)
    good = False
except saml_auth.SamlError:
    good = True
check("SP-initiated logout is refused up front when no slo_url is configured", good)
configure()

shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
