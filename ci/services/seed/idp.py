"""Seed of the idp profile: Keycloak imports the realm itself (seed/keycloak-realm.json, --import-realm); this checks that the
realm's OIDC discovery and SAML descriptor are served on the stack's network, then writes out/idp.env."""
import json, time, urllib.request

KC = "http://keycloak:8080"
for _ in range(60):
    try:
        with urllib.request.urlopen(f"{KC}/realms/dkw/.well-known/openid-configuration", timeout=5) as r:
            issuer = json.load(r)["issuer"]
        with urllib.request.urlopen(f"{KC}/realms/dkw/protocol/saml/descriptor", timeout=5) as r:
            assert b"EntityDescriptor" in r.read()
        break
    except Exception:
        time.sleep(1)
else:
    raise SystemExit("realm dkw is not served by keycloak")
print("realm dkw is up; issuer", issuer)
with open("/out/idp.env", "w") as f:
    f.write(f"KC_URL={KC}\nKC_REALM=dkw\nKC_DESCRIPTOR={KC}/realms/dkw/protocol/saml/descriptor\n"
            "KC_OIDC_CLIENT_ID=dkw-oidc\nKC_OIDC_CLIENT_SECRET=dkwci-test-oidc-secret-not-a-real-one\n")
