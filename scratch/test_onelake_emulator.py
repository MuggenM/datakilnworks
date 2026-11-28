#!/usr/bin/env python3
"""OneLake mounts (web/onelake.py) against the Entra + Fabric emulators of the CI services stack (profile `fabric`): a real
token from a non-Microsoft authority, real TLS verified against a pasted CA (no `allow invalid certificates` anywhere in the
studio's code path), the ADLS Gen2 listing and delta-rs reads of a table the seed wrote through OneLake's data plane.
    CI_SERVICES=fabric ci/run_test.sh onelake_emulator
The stack gives `entra.localhost` and the real OneLake host names as network aliases, so no /etc/hosts line is needed.
Runs as root in a throwaway container: trusting a CA changes this container's system trust store (that is the mechanism)."""
import glob
import logging
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _services  # noqa: E402

AUTHORITY = os.environ.get("ENTRA_AUTHORITY", "")            # no default: the stack's env file sets every value below
if not _services.require_or_skip("Entra emulator", *_services.host_port(AUTHORITY, 8443)):
    sys.exit(0)
if not _services.require_or_skip("OneLake endpoint", "onelake.dfs.fabric.microsoft.com", 443):
    sys.exit(0)
TENANT, CLIENT_ID, SECRET = os.environ["FABRIC_TENANT_ID"], os.environ["FABRIC_CLIENT_ID"], os.environ["FABRIC_CLIENT_SECRET"]
WS, LH, TABLE = os.environ["FABRIC_WORKSPACE"], os.environ["FABRIC_LAKEHOUSE"], os.environ.get("FABRIC_TABLE", "orders")
WS_ID, LH_ID = os.environ["FABRIC_WORKSPACE_ID"], os.environ["FABRIC_LAKEHOUSE_ID"]
CA = open(os.environ["FABRIC_CA_PEM_FILE"]).read()

os.environ["WAREHOUSE_DIR"] = tempfile.mkdtemp(prefix="onelake_emu_")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from web import onelake  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -> {str(detail)[:400]}" if detail and not cond else ""))
    if not cond:
        FAIL.append(name)


class Grab(logging.Handler):
    def __init__(self):
        super().__init__(); self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


grab = Grab(); logging.getLogger().addHandler(grab); logging.getLogger().setLevel(logging.INFO)


def mount(catalog_id, ca, workspace=WS, lakehouse=LH, timeout=90):
    """mount_onelake_catalog in a thread with a deadline: (catalog or exception, seconds, finished in time)."""
    out = {}
    def run():
        try:
            out["v"] = onelake.mount_onelake_catalog(workspace, lakehouse, TENANT, CLIENT_ID, SECRET, catalog_id=catalog_id,
                                                     authority=AUTHORITY, ca_cert_pem=ca)
        except Exception as e:      # noqa: BLE001 - the test inspects it
            out["v"] = e
    t0 = time.time(); th = threading.Thread(target=run, daemon=True); th.start(); th.join(timeout)
    return out.get("v"), time.time() - t0, not th.is_alive()


print("a wrong CA: refused with a TLS error, quickly")
from cryptography import x509                                    # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec          # noqa: E402
from cryptography.x509.oid import NameOID                        # noqa: E402
import datetime                                                  # noqa: E402
k = ec.generate_private_key(ec.SECP256R1()); n = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "not the emulator")])
now = datetime.datetime.now(datetime.timezone.utc)
wrong = x509.CertificateBuilder().subject_name(n).issuer_name(n).public_key(k.public_key()).serial_number(1) \
    .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=1)) \
    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(k, hashes.SHA256())
grab.lines.clear()
v, secs, done = mount("ol_wrong", wrong.public_bytes(serialization.Encoding.PEM).decode())
check("a mount trusting the wrong CA fails", done and isinstance(v, Exception), (v, secs))
check(f"...within 30 s, no hang (took {secs:.0f} s; the SDK's retries made it ~2 minutes)", done and secs < 30, secs)
tls = [l for l in grab.lines if "CERTIFICATE_VERIFY_FAILED" in l or "certificate verify failed" in l.lower() or "UnknownIssuer" in l or "invalid peer certificate" in l.lower()]
check("...and the reason is the certificate (TLS), not a timeout", bool(tls), grab.lines[-5:])
onelake.unmount_onelake_catalog("ol_wrong")

print("the emulators' CA: mount, list, metadata, read")
v, secs, done = mount("ol_emu", CA)
check("mount_onelake_catalog with the Entra authority and the pasted CA succeeds", done and isinstance(v, onelake.OneLakeCatalog), v)
cat = v if isinstance(v, onelake.OneLakeCatalog) else None
key_file = onelake._cert_file("ol_emu")
check("the CA is installed for this catalog (trust_ca_cert)", os.path.exists(key_file) and open(key_file).read() == CA)
if cat:
    check("list_tables lists the seeded table", TABLE in cat.list_tables(force_refresh=True), cat.list_tables())
    md = cat.get_table_metadata(TABLE)
    check("get_table_metadata has its columns", [c["name"] for c in md["columns"]] == ["id", "item", "amount"], md.get("columns"))
    df = cat.read_table(TABLE)
    check("read_table returns the seeded rows", sorted(df["id"].tolist()) == [1, 2, 3, 4, 5] and set(df["item"]) == {"anvil", "bellows", "crucible", "tongs", "kiln"}, str(df.head()))
    check("read_table with a limit", len(cat.read_table(TABLE, limit=2)) == 2)

print("by GUID path (workspace and Lakehouse ids)")
v, secs, done = mount("ol_guid", CA, workspace=WS_ID, lakehouse=LH_ID)
check("a mount by workspace id + Lakehouse id succeeds", done and isinstance(v, onelake.OneLakeCatalog), v)
if isinstance(v, onelake.OneLakeCatalog):
    df = v.read_table(TABLE)
    check("read_table by GUID path returns the same rows", sorted(df["id"].tolist()) == [1, 2, 3, 4, 5], str(df.head()))
    onelake.unmount_onelake_catalog("ol_guid")

print("unmount")
check("unmount_onelake_catalog removes the catalog", onelake.unmount_onelake_catalog("ol_emu") and onelake.get_onelake_catalog("ol_emu") is None)
check("...and the trusted CA (the trust_ca_cert file is gone)", not os.path.exists(key_file) and not glob.glob(onelake._cert_file("ol_*")))

print()
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS")
sys.stdout.flush()
os._exit(1 if FAIL else 0)
