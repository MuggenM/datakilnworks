"""Seed of the fabric profile (runs in the studio image: it needs deltalake). Through the emulators' real APIs: a token from
the Entra emulator, a workspace + Lakehouse from the Fabric control plane, and one Delta table `orders` written through
abfss:// (OneLake's data plane, the same path the studio's mount reads). Then the two self-signed certificates the studio
must trust (Entra's and the OneLake endpoint's; fabric-emulator makes a new key pair at every start, so they are read
here, after start) go to out/fabric/ca.pem, and the values to out/fabric.env. The credentials are fabric-emulator's own
documented quickstart ones."""
import os
import ssl
import time

import pyarrow as pa
import requests
import urllib3
from deltalake import DeltaTable, write_deltalake

urllib3.disable_warnings()
TENANT = "6f89cf12-978b-4d23-ac18-9ef0c127cf87"
CLIENT_ID, CLIENT_SECRET = "00d88624-f0d7-46f6-a641-6232c2608928", "daemon-app-secret"
ENTRA = "https://entra.localhost:8443"         # the authority HOST: azure-identity appends the tenant itself
FABRIC = "https://fabric:9443"                       # the control plane, host-routed: Host api.fabric.microsoft.com
WORKSPACE, LAKEHOUSE, TABLE = "dkwci", "lake", "orders"


def token(scope):
    r = requests.post(f"{ENTRA}/{TENANT}/oauth2/v2.0/token", verify=False, timeout=20, data={
        "grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "scope": scope})
    r.raise_for_status()
    return r.json()["access_token"]


def api(method, path, **kw):
    h = {"Authorization": "Bearer " + token("https://api.fabric.microsoft.com/.default"), "Host": "api.fabric.microsoft.com"}
    r = requests.request(method, FABRIC + path, headers=h, verify=False, timeout=20, **kw)
    r.raise_for_status()
    return r.json()


capacity = api("GET", "/v1/capacities")["value"][0]["id"]
ws = api("POST", "/v1/workspaces", json={"displayName": WORKSPACE, "capacityId": capacity})["id"]
lh = api("POST", f"/v1/workspaces/{ws}/lakehouses", json={"displayName": LAKEHOUSE})["id"]

opts = {"bearer_token": token("https://storage.azure.com/.default"), "use_fabric_endpoint": "true",
        "azure_allow_invalid_certificates": "true"}           # the seed only; the test trusts the certificates instead
url = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE}.Lakehouse/Tables/{TABLE}"
rows = pa.table({"id": pa.array([1, 2, 3, 4, 5], pa.int64()), "item": ["anvil", "bellows", "crucible", "tongs", "kiln"],
                 "amount": pa.array([120.5, 35.0, 64.25, 18.0, 990.0], pa.float64())})
for attempt in range(10):                            # a fresh Lakehouse's folders may take a moment to exist
    try:
        write_deltalake(url, rows, storage_options=opts)
        break
    except Exception:
        if attempt == 9:
            raise
        time.sleep(1)
assert DeltaTable(url, storage_options=opts).to_pyarrow_table().num_rows == 5

os.makedirs("/out/fabric", exist_ok=True)
pems = [ssl.get_server_certificate((h, p)) for h, p in (("entra.localhost", 8443), ("onelake.dfs.fabric.microsoft.com", 443))]
with open("/out/fabric/ca.pem", "w") as f:
    f.write("".join(p if p.endswith("\n") else p + "\n" for p in pems))
with open("/out/fabric.env", "w") as f:
    f.write(f"ENTRA_AUTHORITY={ENTRA}\nFABRIC_TENANT_ID={TENANT}\nFABRIC_CLIENT_ID={CLIENT_ID}\nFABRIC_CLIENT_SECRET={CLIENT_SECRET}\n"
            f"FABRIC_WORKSPACE={WORKSPACE}\nFABRIC_LAKEHOUSE={LAKEHOUSE}\nFABRIC_WORKSPACE_ID={ws}\nFABRIC_LAKEHOUSE_ID={lh}\n"
            f"FABRIC_TABLE={TABLE}\nFABRIC_CA_PEM_FILE=/services/fabric/ca.pem\n")
print(f"workspace {ws}, lakehouse {lh}, table {TABLE} with {rows.num_rows} rows")
