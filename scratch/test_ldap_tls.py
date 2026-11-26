#!/usr/bin/env python3
"""
LDAP TLS (web/ldap_auth.tls_settings / _connect, web/auth_frameworks.test_ldap_connection) against a REAL throwaway lldap serving
LDAPS with a certificate from a throwaway CA. Run on the HOST; it starts and removes its own containers and network:

    /usr/bin/python3 scratch/test_ldap_tls.py

Checks: the server certificate is verified (pasted CA accepted, no CA = refused, wrong hostname = refused), tls_skip_verify is
the explicit escape hatch, a bad CA is a clean error, the connectivity test agrees with the sign-in, and StartTLS is negotiated
BEFORE the bind (it used to bind first, sending the password in cleartext).
"""
import os, subprocess, sys, tempfile, textwrap, time

HERE = os.path.dirname(os.path.abspath(__file__)); REPO = os.path.dirname(HERE)
IMAGE = os.getenv("CI_IMAGE", "localspark-lakehouse-notebook"); LLDAP = os.getenv("LLDAP_IMAGE", "nitnelave/lldap:latest")
NET, SRV = "ldtls_net", "ldtls_lldap"
work = tempfile.mkdtemp(prefix="ldtls_"); os.chmod(work, 0o755)


def sh(*a, check=True, **kw):
    return subprocess.run(list(a), check=check, capture_output=True, text=True, **kw)


def cleanup():
    sh("docker", "rm", "-f", SRV, check=False); sh("docker", "network", "rm", NET, check=False)


GEN = textwrap.dedent("""
    import datetime, ipaddress
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    now = datetime.datetime.now(datetime.timezone.utc)
    def name(cn): return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    def pem(k): return k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    for tag in ("ca", "other"):
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca = (x509.CertificateBuilder().subject_name(name(f"dkw test {tag} CA")).issuer_name(name(f"dkw test {tag} CA"))
              .public_key(ca_key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(hours=1))
              .not_valid_after(now + datetime.timedelta(days=2)).add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
              .sign(ca_key, hashes.SHA256()))
        open(f"/w/{tag}.pem", "wb").write(ca.public_bytes(serialization.Encoding.PEM))
        if tag == "ca":
            key = ec.generate_private_key(ec.SECP256R1())
            cert = (x509.CertificateBuilder().subject_name(name("ldaps.test")).issuer_name(ca.subject).public_key(key.public_key())
                    .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(hours=1))
                    .not_valid_after(now + datetime.timedelta(days=2))
                    .add_extension(x509.SubjectAlternativeName([x509.DNSName("ldaps.test")]), critical=False).sign(ca_key, hashes.SHA256()))
            open("/w/server.crt", "wb").write(cert.public_bytes(serialization.Encoding.PEM)); open("/w/server.key", "wb").write(pem(key))
    """)

CHECKS = textwrap.dedent("""
    import os, sys
    sys.path.insert(0, "/workspace")
    os.environ["WAREHOUSE_DIR"] = "/tmp/wh"; os.makedirs("/tmp/wh/.metadata", exist_ok=True)
    from web import ldap_auth, auth_frameworks
    FAIL = []
    def check(n, c, d=""):
        print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else "")); FAIL.append(n) if not c else None
    CA, OTHER = open("/w/ca.pem").read(), open("/w/other.pem").read()
    base = {"server_host": "ldaps.test", "server_port": 6360, "encryption": "ssl"}
    BIND, PW = "uid=admin,ou=people,dc=example,dc=com", "adminpassword1"
    def bind(**cfg):
        try:
            ldap_auth._connect({**base, **cfg}, user=BIND, password=PW).unbind(); return "ok"
        except ldap_auth.LdapError as e:
            return str(e)
    check("LDAPS with the directory's CA pasted binds", bind(ca_cert_pem=CA) == "ok", bind(ca_cert_pem=CA))
    r = bind(); check("LDAPS without that CA is refused (certificate not trusted)", r != "ok" and "certificate" in r.lower(), r)
    r = bind(ca_cert_pem=OTHER); check("LDAPS with an unrelated CA is refused", r != "ok", r)
    r = bind(ca_cert_pem=CA, server_host="ldap-other"); check("the right CA but a host name the certificate does not cover is refused", r != "ok", r)
    check("tls_skip_verify is the explicit escape hatch", bind(tls_skip_verify=True, server_host="ldap-other") == "ok")
    r = bind(ca_cert_pem="-----BEGIN CERTIFICATE-----\\nnot base64\\n-----END CERTIFICATE-----")
    check("a broken CA certificate is a clean error, not a crash", r != "ok", r)
    t = auth_frameworks.test_ldap_connection({**base, "ca_cert_pem": CA}); check("Test connection passes with the CA", t["success"] and t["tls"], t)
    t = auth_frameworks.test_ldap_connection(base); check("Test connection fails without it, like the sign-in", not t["success"], t)
    check("plain LDAP is unchanged", bind(server_port=3890, encryption="none") == "ok")

    # StartTLS order (lldap has no StartTLS): record what _connect does with the connection
    calls = []
    class FakeConn:
        result = {"description": "success"}
        def __init__(self, *a, **kw): pass
        def open(self): calls.append("open")
        def start_tls(self): calls.append("start_tls"); return True
        def bind(self): calls.append("bind"); return True
    real = ldap_auth.Connection; ldap_auth.Connection = FakeConn
    try:
        ldap_auth._connect({**base, "server_port": 389, "encryption": "starttls", "ca_cert_pem": CA}, user=BIND, password=PW)
    finally:
        ldap_auth.Connection = real
    check("StartTLS is negotiated before the bind (the password never goes out in cleartext)", calls == ["open", "start_tls", "bind"], calls)
    class Refuses(FakeConn):
        def start_tls(self): calls.append("start_tls"); return False
    calls.clear(); ldap_auth.Connection = Refuses
    try:
        r = "ok"; ldap_auth._connect({**base, "server_port": 389, "encryption": "starttls"}, user=BIND, password=PW)
    except ldap_auth.LdapError as e:
        r = str(e)
    finally:
        ldap_auth.Connection = real
    check("a server refusing StartTLS never gets the bind", r != "ok" and "bind" not in calls, (r, calls))
    print("ALL PASSED" if not FAIL else f"{len(FAIL)} FAILED"); sys.exit(1 if FAIL else 0)
    """)

try:
    cleanup()
    sh("docker", "run", "--rm", "-v", f"{work}:/w", IMAGE, "python", "-c", GEN)
    for f in os.listdir(work):
        os.chmod(os.path.join(work, f), 0o644)
    sh("docker", "network", "create", NET)
    sh("docker", "run", "-d", "--name", SRV, "--network", NET, "--network-alias", "ldaps.test", "--network-alias", "ldap-other",
       "-v", f"{work}:/certs:ro", "-e", "LLDAP_JWT_SECRET=throwaway-jwt-secret-0123456789", "-e", "LLDAP_KEY_SEED=throwaway-seed-0123456789",
       "-e", "LLDAP_LDAP_BASE_DN=dc=example,dc=com", "-e", "LLDAP_LDAP_USER_PASS=adminpassword1",
       "-e", "LLDAP_LDAPS_OPTIONS__ENABLED=true", "-e", "LLDAP_LDAPS_OPTIONS__PORT=6360",
       "-e", "LLDAP_LDAPS_OPTIONS__CERT_FILE=/certs/server.crt", "-e", "LLDAP_LDAPS_OPTIONS__KEY_FILE=/certs/server.key", LLDAP)
    for _ in range(60):
        if "6360" in sh("docker", "logs", SRV, check=False).stdout + sh("docker", "logs", SRV, check=False).stderr:
            break
        time.sleep(1)
    time.sleep(2)
    r = subprocess.run(["docker", "run", "--rm", "--network", NET, "-v", f"{REPO}/web:/workspace/web:ro", "-v", f"{work}:/w:ro",
                        IMAGE, "python", "-c", CHECKS], capture_output=True, text=True)
    print("\n".join(l for l in (r.stdout + r.stderr).splitlines() if "[PASS]" in l or "[FAIL]" in l or "PASSED" in l or "FAILED" in l or "Traceback" in l or "Error" in l))
    code = r.returncode
finally:
    cleanup()
    subprocess.run(["rm", "-rf", work])
sys.exit(code if "code" in dir() else 1)
