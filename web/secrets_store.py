"""
Per-install secrets persisted under $WAREHOUSE_DIR/.metadata (mode 0600).

Kept dependency-free so the studio, compute workers and Ray actors, which share the warehouse volume,
can all import it and agree on the same value without pulling in the auth database.
"""

import os
import secrets

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")


def load_or_create_secret(filename: str, nbytes: int = 32) -> str:
    """Returns the secret stored in `filename`, creating it atomically on first use (O_EXCL, so racing processes agree)."""
    path = os.path.join(os.getenv("WAREHOUSE_DIR", WAREHOUSE_DIR), ".metadata", filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_hex(nbytes))
    except FileExistsError:
        pass
    # A racing creator may not have finished writing yet; wait briefly for a non-empty value.
    for _ in range(50):
        with open(path, "r") as f:
            value = f.read().strip()
        if value:
            return value
        import time
        time.sleep(0.02)
    raise RuntimeError(f"Secret file {path} is empty")


def _fernet(filename: str = "connections.key"):
    import base64
    from cryptography.fernet import Fernet
    return Fernet(base64.urlsafe_b64encode(bytes.fromhex(load_or_create_secret(filename))))


def encrypt_json(value, filename: str = "connections.key") -> str:
    """Encrypts a JSON-serialisable value with the install's key (Fernet; the key file is created on first use, mode 0600)."""
    import json
    return _fernet(filename).encrypt(json.dumps(value).encode("utf-8")).decode("ascii")


def decrypt_json(token: str, filename: str = "connections.key"):
    import json
    return json.loads(_fernet(filename).decrypt(token.encode("ascii")).decode("utf-8"))
