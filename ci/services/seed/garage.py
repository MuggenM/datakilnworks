"""Seed of the s3 profile: assigns the single node's layout and creates key `test` (allowed to create buckets) through
Garage's admin API (v1), then writes out/s3.env. The Garage image has no shell, so this runs in a plain Python image."""
import json, time, urllib.request

ADMIN, TOKEN = "http://garage:3903", "dkwci-test-admin-token"


def call(method, path, body=None):
    req = urllib.request.Request(ADMIN + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw.strip() else None


node = call("GET", "/v1/status")["node"]
call("POST", "/v1/layout", [{"id": node, "zone": "dc1", "capacity": 1 << 30, "tags": []}])
call("POST", "/v1/layout/apply", {"version": 1})
key = call("POST", "/v1/key", {"name": "test"})
call("POST", "/v1/key?id=" + key["accessKeyId"], {"allow": {"createBucket": True}})
for _ in range(60):                             # the S3 API answers once the layout is live
    try:
        if call("GET", "/v1/health")["status"] == "healthy":
            break
    except Exception:
        pass
    time.sleep(1)
else:
    raise SystemExit("garage did not become healthy")
ep, kid, sec = "garage:3900", key["accessKeyId"], key["secretAccessKey"]
with open("/out/s3.env", "w") as f:
    f.write(f"GARAGE_ENDPOINT={ep}\nTEST_S3_ENDPOINT={ep}\nTEST_S3_ACCESS_KEY={kid}\nTEST_S3_SECRET_KEY={sec}\n"
            f"S3_ENDPOINT={ep}\nS3_ACCESS_KEY={kid}\nS3_SECRET_KEY={sec}\n")
