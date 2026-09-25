"""
Authentication Frameworks & Single Sign-On (SSO) Engine for Databricks Local Studio.
Supports:
- LDAP / Active Directory (with StartTLS / LDAPS and group-to-role mappings)
- OAuth2 / OpenID Connect (OIDC) Discovery & SSO
- SAML 2.0 Identity Provider Configuration
- Local Admin Emergency Fallback & User Auto-provisioning
"""

import os
import json
import socket
import ssl
import time
import logging
from typing import Dict, Any, Optional
from urllib.parse import urlparse

logger = logging.getLogger("localspark.auth_frameworks")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
AUTH_CONFIG_FILE = os.path.join(METADATA_DIR, "auth_config.json")

DEFAULT_AUTH_FRAMEWORK_CONFIG: Dict[str, Any] = {
    "primary_framework": "local",  # local | ldap | oidc | saml
    "allow_local_fallback": True,
    "auto_provision_users": True,
    "ldap": {
        "enabled": False,
        "server_host": "ldap.company.internal",
        "server_port": 389,
        "encryption": "none",  # none | starttls | ssl
        "base_dn": "dc=company,dc=internal",
        "bind_dn": "cn=lakehouse_svc,ou=Services,dc=company,dc=internal",
        "bind_password": "",
        "user_search_filter": "(&(objectClass=user)(sAMAccountName={username}))",
        "user_search_base": "ou=Employees,dc=company,dc=internal",
        "group_search_base": "ou=Groups,dc=company,dc=internal",
        "admin_group": "cn=DataAdmins,ou=Groups,dc=company,dc=internal",
        "power_user_group": "cn=DataEngineers,ou=Groups,dc=company,dc=internal",
        "user_group": "",       # optional: explicit group for the `user` role (else default_role applies)
        "sync_group": "",       # optional: only members of this group may log in / be synced; empty = everyone
        "default_role": "user"
    },
    "oidc": {
        "enabled": False,
        "provider_name": "Microsoft Entra ID / Okta",
        "issuer_url": "https://login.microsoftonline.com/common/v2.0",
        "client_id": "",
        "client_secret": "",
        "scopes": "openid email profile groups",
        "redirect_uri": "http://localhost:8891/api/auth/oidc/callback",
        "username_claim": "preferred_username",   # falls back to a verified `email` claim
        "admin_claim": "groups",
        "admin_value": "LakehouseAdmins",
        "power_user_value": "DataEngineers",
        "default_role": "user"
    },
    "saml": {
        "enabled": False,
        "idp_metadata_url": "",
        "entity_id": "urn:databricks:local:studio",
        "sso_url": "",
        "x509_cert": "",
        "attribute_username": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
        "attribute_email": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
        "provider_name": "SAML SSO",
        "idp_entity_id": "",             # the IdP's issuer: responses from any other issuer are refused
        "sp_base_url": "",               # the studio's public URL (ACS = <it>/api/auth/saml/acs); empty = derived from the request
        "attribute_display_name": "",
        "attribute_groups": "groups",    # the assertion attribute that carries group / role values
        "admin_value": "",
        "power_user_value": "",
        "want_assertions_signed": True,
        "allow_idp_initiated": False,    # off: only responses to a sign-in this studio started are accepted
        "default_role": "user"
    }
}


def load_raw_config() -> Dict[str, Any]:
    """Loads stored auth configuration from JSON file or returns defaults."""
    if os.path.exists(AUTH_CONFIG_FILE):
        try:
            with open(AUTH_CONFIG_FILE, "r") as f:
                data = json.load(f)
                config = json.loads(json.dumps(DEFAULT_AUTH_FRAMEWORK_CONFIG))
                # Deep merge top-level keys
                for k, v in data.items():
                    if isinstance(v, dict) and k in config:
                        config[k].update(v)
                    else:
                        config[k] = v
                return config
        except Exception as e:
            logger.warning(f"Error loading auth config from {AUTH_CONFIG_FILE}: {e}")
    return json.loads(json.dumps(DEFAULT_AUTH_FRAMEWORK_CONFIG))


def mask_secret(val: Optional[str]) -> str:
    """Masks secret values for safe API exposure."""
    return "••••••••" if val else ""


def get_public_config() -> Dict[str, Any]:
    """Returns configuration with sensitive passwords and secrets masked."""
    config = load_raw_config()
    if config.get("ldap", {}).get("bind_password"):
        config["ldap"]["bind_password"] = mask_secret(config["ldap"]["bind_password"])
    if config.get("oidc", {}).get("client_secret"):
        config["oidc"]["client_secret"] = mask_secret(config["oidc"]["client_secret"])
    return config


def save_config(new_config: Dict[str, Any]) -> bool:
    """Saves updated auth configuration, preserving existing secrets if masked."""
    try:
        os.makedirs(METADATA_DIR, exist_ok=True)
        current = load_raw_config()

        # Preserve LDAP bind password if unchanged mask
        if new_config.get("ldap", {}).get("bind_password") == "••••••••":
            new_config["ldap"]["bind_password"] = current.get("ldap", {}).get("bind_password", "")

        # Preserve OIDC client secret if unchanged mask
        if new_config.get("oidc", {}).get("client_secret") == "••••••••":
            new_config["oidc"]["client_secret"] = current.get("oidc", {}).get("client_secret", "")

        merged = json.loads(json.dumps(DEFAULT_AUTH_FRAMEWORK_CONFIG))
        for k, v in new_config.items():
            if isinstance(v, dict) and k in merged:
                merged[k].update(v)
            else:
                merged[k] = v

        with open(AUTH_CONFIG_FILE, "w") as f:
            json.dump(merged, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save auth config: {e}")
        return False


def test_ldap_connection(ldap_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Tests network connectivity and TLS negotiation to configured LDAP server."""
    host = ldap_cfg.get("server_host", "").strip()
    port = int(ldap_cfg.get("server_port", 389))
    encryption = ldap_cfg.get("encryption", "none").lower()

    if not host:
        return {"success": False, "message": "LDAP server host is required"}

    # Handle protocol in host if user supplied ldap:// or ldaps://
    if "://" in host:
        parsed = urlparse(host)
        host = parsed.hostname or host
        if parsed.port:
            port = parsed.port
        elif parsed.scheme == "ldaps":
            port = 636
            encryption = "ssl"

    start_time = time.perf_counter()
    sock = None
    try:
        sock = socket.create_connection((host, port), timeout=4.0)
        conn_time = round((time.perf_counter() - start_time) * 1000, 2)

        tls_info = None
        if encryption == "ssl" or port == 636:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            ssock = context.wrap_socket(sock, server_hostname=host)
            tls_info = ssock.version()
            ssock.close()
        else:
            sock.close()

        msg = f"Connected successfully to LDAP endpoint {host}:{port} in {conn_time}ms"
        if tls_info:
            msg += f" (Encrypted with {tls_info})"

        return {
            "success": True,
            "latency_ms": conn_time,
            "tls": bool(tls_info),
            "message": msg
        }
    except Exception as e:
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        return {
            "success": False,
            "message": f"Connection failed to {host}:{port} - {str(e)}"
        }


def test_oidc_discovery(oidc_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Tests fetching OpenID Connect Discovery document from the configured issuer URL."""
    issuer = oidc_cfg.get("issuer_url", "").strip().rstrip("/")
    if not issuer:
        return {"success": False, "message": "OIDC Issuer URL is required"}

    discovery_url = f"{issuer}/.well-known/openid-configuration"
    start_time = time.perf_counter()

    try:
        import requests
        resp = requests.get(discovery_url, timeout=5.0, verify=False)
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        if resp.status_code == 200:
            doc = resp.json()
            return {
                "success": True,
                "latency_ms": duration_ms,
                "issuer": doc.get("issuer", issuer),
                "authorization_endpoint": doc.get("authorization_endpoint"),
                "token_endpoint": doc.get("token_endpoint"),
                "userinfo_endpoint": doc.get("userinfo_endpoint"),
                "message": f"Verified OIDC Provider Discovery in {duration_ms}ms"
            }
        else:
            return {
                "success": False,
                "message": f"Discovery endpoint returned HTTP {resp.status_code} ({discovery_url})"
            }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to reach OIDC discovery endpoint: {str(e)}"
        }
