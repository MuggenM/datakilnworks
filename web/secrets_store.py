"""
Per-install secrets: where they live, and how they get there.

Every key file of the install (and the accounts database, auth.db) is resolved by `secret_path(name)`:

  DKW_SECRETS_DIR           the signing / encryption keys only the studio needs: jwt_secret (session signing), connections.key and
                            mfa.key (Fernet), sharing.key (Delta Sharing link HMAC), sandbox_gateway_key, saml_sp.key/.crt,
                            llm_config.json (LLM API keys) and auth.db (accounts, password hashes, passkeys, sessions)
  DKW_COMPUTE_SECRETS_DIR   the two values the compute nodes need as well: compute_token (studio <-> worker) and governance_salt
                            (the hash masks must give the same result on every node)

Unset, both fall back to `$WAREHOUSE_DIR/.metadata` (the old location, still what a plain `python` run or a test script gets). Docker
Compose puts them on their own named volumes (`dkw-secrets` for the studio and init only, `dkw-compute-secrets` also for the compute
nodes), so a copy, snapshot or backup of the warehouse volume no longer contains the keys that sign sessions or decrypt stored
credentials, and a compute node (which runs arbitrary SQL against the warehouse volume) no longer holds them at all.

Moving to a new directory is automatic and one-way: on first use a file still in `.metadata` is copied (mode 0600, O_EXCL so racing
processes agree), checked, then removed from `.metadata`, which keeps a `<name>.moved` marker naming the new directory. A process that
later resolves a secret to `.metadata` again (the variable got lost from a service definition) finds the marker and refuses
(SecretMovedError) instead of silently generating a NEW key: that would sign everybody out, make every stored connection credential
and TOTP secret undecryptable and, for auth.db, start an empty accounts database that bootstraps a fresh admin.

Kept dependency-free so the studio, compute workers and Ray actors can all import it without pulling in the auth database.
"""

import os
import secrets
import sqlite3
import time

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")

COMPUTE_SHARED = frozenset({"compute_token", "governance_salt"})
# Every file this module manages (web/init.py migrates all of them before the studio starts; anything else stays in .metadata).
STUDIO_SECRETS = ("jwt_secret", "connections.key", "mfa.key", "sharing.key", "sandbox_gateway_key", "saml_sp.key", "saml_sp.crt",
                  "llm_config.json", "auth.db")
MOVED_SUFFIX = ".moved"


class SecretMovedError(RuntimeError):
    """A secret was moved out of .metadata, but this process resolves it to .metadata again (its DKW_*SECRETS_DIR is not set)."""


def legacy_dir() -> str:
    return os.path.join(os.getenv("WAREHOUSE_DIR", WAREHOUSE_DIR), ".metadata")


def secrets_dir(name: str = "") -> str:
    """The directory `name` lives in (see the module docstring). Created 0700 on first use."""
    env = "DKW_COMPUTE_SECRETS_DIR" if name in COMPUTE_SHARED else "DKW_SECRETS_DIR"
    d = (os.getenv(env) or "").strip() or legacy_dir()
    if not os.path.isdir(d):
        os.makedirs(d, mode=0o700, exist_ok=True)
    return d


def _same_dir(a: str, b: str) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return os.path.realpath(a) == os.path.realpath(b)


def secret_path(name: str) -> str:
    """Absolute path of secret `name`, moving it out of .metadata first when its directory is elsewhere. Raises SecretMovedError."""
    target_dir = secrets_dir(name)
    legacy = legacy_dir()
    target = os.path.join(target_dir, name)
    if _same_dir(target_dir, legacy):
        marker = target + MOVED_SUFFIX
        if not os.path.exists(target) and os.path.exists(marker):
            try:
                where = open(marker, encoding="utf-8").read().strip()
            except OSError:
                where = "another directory"
            env = "DKW_COMPUTE_SECRETS_DIR" if name in COMPUTE_SHARED else "DKW_SECRETS_DIR"
            raise SecretMovedError(f"The secret '{name}' was moved to {where}, but {env} is not set for this process. Set {env}={where} "
                                   f"(and mount that volume) instead of letting a new key be generated.")
        return target
    old = os.path.join(legacy, name)
    if os.path.exists(old):
        _migrate(name, old, target, target_dir)
    return target


def _migrate(name: str, old: str, target: str, target_dir: str) -> None:
    if name.endswith(".db"):
        _migrate_sqlite(old, target)
    else:
        with open(old, "rb") as f:
            data = f.read()
        try:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
        except FileExistsError:
            pass                                    # a racing process copied it first (or it was moved before): compare below
        for _ in range(50):                         # the racing writer may still be writing
            with open(target, "rb") as f:
                now = f.read()
            if now:
                break
            time.sleep(0.02)
        if now != data:
            raise RuntimeError(f"Both {old} and {target} exist with different contents. Keep the right one and delete the other "
                               f"(the key in use is the one the running studio was started with).")
    _leave_marker(old, target_dir)


def _migrate_sqlite(old: str, target: str) -> None:
    """auth.db: copied with SQLite's backup API (consistent even with a WAL), then the old file and its -wal/-shm are removed."""
    if os.path.exists(target):
        raise RuntimeError(f"Both {old} and {target} exist. The accounts database must exist once: keep the one in use and delete the other.")
    tmp = target + ".migrating"
    if os.path.exists(tmp):
        os.remove(tmp)
    src = sqlite3.connect(old, timeout=30)
    try:
        dst = sqlite3.connect(tmp)
        try:
            src.backup(dst)
            ok = dst.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            dst.close()
    finally:
        src.close()
    if not ok:
        os.remove(tmp)
        raise RuntimeError(f"Copy of {old} failed its integrity check; nothing was moved.")
    os.chmod(tmp, 0o600)
    os.replace(tmp, target)
    for suffix in ("-wal", "-shm", "-journal"):
        try:
            os.remove(old + suffix)
        except FileNotFoundError:
            pass


def _leave_marker(old: str, target_dir: str) -> None:
    with open(old + MOVED_SUFFIX, "w", encoding="utf-8") as f:
        f.write(target_dir + "\n")
    try:
        os.remove(old)
    except FileNotFoundError:
        pass


def migrate_all() -> list:
    """Moves every managed secret that is still in .metadata to its directory (web/init.py, before the studio starts). Returns the names moved."""
    moved = []
    for name in STUDIO_SECRETS + tuple(sorted(COMPUTE_SHARED)):
        old = os.path.join(legacy_dir(), name)
        had = os.path.exists(old)
        secret_path(name)
        if had and not os.path.exists(old):
            moved.append(name)
    return moved


def locations() -> dict:
    """Where the secrets are, for the init report (paths only, never contents)."""
    return {"secrets": secrets_dir(""), "compute_secrets": secrets_dir("compute_token"), "legacy": legacy_dir()}


def load_or_create_secret(filename: str, nbytes: int = 32) -> str:
    """Returns the secret stored in `filename`, creating it atomically on first use (O_EXCL, so racing processes agree)."""
    path = secret_path(filename)
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
