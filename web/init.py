"""One-shot initialisation of a Data Kiln Works deployment: `python -m web.init`.

Run it BEFORE the studio starts: as the `datakilnworks-init` service in Docker Compose (the studio waits for it to succeed) or as an
`initContainer` in Kubernetes (same image, different command). It does the setup work that is otherwise hidden in the studio's start-up, but as a
separate step with a clear result:

  * a bad or missing bootstrap-admin setting becomes one readable message and a failed init (Compose: the studio does not start; Kubernetes: the pod
    shows `Init:Error`) instead of a crash loop of the studio;
  * every SQLite database is created / migrated once, before the studio and its background loops open them;
  * the dbt project directory is seeded from the template (only files that are missing, nothing is ever overwritten);
  * the warehouse, metadata and notebook directories are created and checked for write access, with a hint when the volume has the wrong owner;
  * optional: wait until other services answer (`--wait-for URL`, `INIT_WAIT_FOR`);
  * warnings for optional packages that are missing from the image (features that would fail later, e.g. after an image built before a
    requirements change).

Every step is idempotent, so it is safe on every start, on every replica and after a restart; the studio still performs the same steps itself, so
skipping the init step changes nothing but the quality of the failure. A file lock on the warehouse keeps two concurrent inits (two pods, two
runs) from migrating at the same time. Exit code 0 = ready (warnings allowed), 1 = do not start the studio.

    python -m web.init [--json] [--wait-for URL[,URL...]] [--wait-timeout SECONDS] [--check-only]

Environment: WAREHOUSE_DIR, NOTEBOOKS_DIR, DBT_PROJECT_DIR (as the studio), INIT_ADMIN_* (only used when there is no account yet),
INIT_WAIT_FOR, INIT_WAIT_TIMEOUT, INIT_LOCK_TIMEOUT.
"""
import argparse
import contextlib
import importlib
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional

OK, WARN, FAIL = "ok", "warn", "FAIL"


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.lines: List[str] = []

    def emit(self, record):
        self.lines.append(record.getMessage())


class Report:
    def __init__(self):
        self.items: List[Dict[str, str]] = []

    def add(self, status: str, step: str, message: str) -> None:
        self.items.append({"status": status, "step": step, "message": message})

    @property
    def failed(self) -> bool:
        return any(i["status"] == FAIL for i in self.items)

    @property
    def warnings(self) -> int:
        return sum(1 for i in self.items if i["status"] == WARN)


def _dirs() -> Dict[str, str]:
    return {"warehouse": os.getenv("WAREHOUSE_DIR", "/workspace/warehouse"), "notebooks": os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks"),
            "dbt project": os.getenv("DBT_PROJECT_DIR", "/workspace/dbt_project")}


# ---------------------------------------------------------------- steps

def step_directories(rep: Report) -> None:
    """Create the directories and prove they are writable (a volume owned by another uid is the classic first-deployment failure)."""
    for name, path in _dirs().items():
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, f".init_write_test_{os.getpid()}")
            with open(probe, "w") as f:
                f.write("x")
            os.remove(probe)
            rep.add(OK, name, f"{path} exists and is writable")
        except OSError as exc:
            uid = os.getuid() if hasattr(os, "getuid") else "?"
            rep.add(FAIL, name, f"{path} is not writable ({exc.strerror or exc}). The init runs as uid {uid}: give the volume to that user "
                                f"(Kubernetes: securityContext.fsGroup; Docker: chown the host directory) or run with the volume's owner.")
    if not any(i["status"] == FAIL for i in rep.items):
        os.makedirs(os.path.join(_dirs()["warehouse"], ".metadata"), exist_ok=True)


@contextlib.contextmanager
def _lock(timeout: float):
    """An exclusive lock on the warehouse so that concurrent inits (several pods, a re-run) migrate one after the other."""
    import fcntl
    path = os.path.join(_dirs()["warehouse"], ".metadata", ".init.lock")
    fh = open(path, "a+")
    end = time.time() + timeout
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if time.time() > end:
                fh.close()
                raise TimeoutError(f"another init has held the warehouse lock for more than {int(timeout)} s")
            time.sleep(0.5)
    try:
        yield
    finally:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        finally:
            fh.close()


def _database_steps() -> List[tuple]:
    """(label, importable callable path). Each creates or migrates its tables; all are idempotent."""
    return [("governance (tags, policies, audit)", "web.governance.store:init_governance_db"), ("query history", "web.audit:init_history_db"),
            ("alerts", "web.alerts:init_alerts_db"), ("experiments", "web.experiments:init_experiments_db"), ("prompt playground", "web.playground:init_playground_db"),
            ("lineage", "web.lineage:init_lineage_db"), ("recent items", "web.recents:init_recents_db"), ("model serving", "web.serving:init_serving_db"),
            ("job runs", "web.workflow:init_runs_db"), ("MFA policy", "web.mfa_policy:init_policy_db"), ("IP allowlist", "web.ip_allowlist:init_db"),
            ("connections", "web.connections:_db"), ("streams", "web.streaming:_db"), ("groups", "web.groups:_conn"), ("SCIM", "web.scim:_conn"),
            ("Auto-Loader", "web.autoloader:init_autoloader_db")]


def _resolve(path: str) -> Callable[[], Any]:
    mod, _, fn = path.partition(":")
    return getattr(importlib.import_module(mod), fn)


def step_bootstrap_and_databases(rep: Report) -> None:
    """The accounts database first, and with it the bootstrap administrator (only when there is no account at all)."""
    handler = _Collect()
    logging.getLogger().addHandler(handler)
    try:
        try:
            from web import auth                            # importing it already initialises the accounts database (and the bootstrap admin)
            auth.init_auth_db()
            with auth.get_db_connection() as c:
                n = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            rep.add(OK, "accounts", f"users database ready ({n} account(s))")
        except SystemExit:
            why = " ".join(l for l in handler.lines if "INIT_ADMIN" in l or "admin" in l.lower())[:600] or "the bootstrap administrator is not configured"
            rep.add(FAIL, "bootstrap admin", why + " Set INIT_ADMIN_USERNAME and INIT_ADMIN_PASSWORD_HASH (python -m web.auth hash-password).")
            return
    except Exception as exc:
        rep.add(FAIL, "accounts", f"could not prepare the accounts database: {type(exc).__name__}: {str(exc)[:200]}")
        return
    finally:
        logging.getLogger().removeHandler(handler)
    for label, path in _database_steps():
        try:
            res = _resolve(path)()
            if hasattr(res, "close"):                   # a connection returned by a `_db()` / `_conn()` helper
                res.close()
            rep.add(OK, f"database: {label}", "ready")
        except Exception as exc:
            rep.add(FAIL, f"database: {label}", f"{type(exc).__name__}: {str(exc)[:200]}")


def step_seed(rep: Report) -> None:
    try:
        from web.dbt_service import ensure_project
        seeded = ensure_project()["seeded"]
        rep.add(OK, "dbt project", f"seeded {len(seeded)} file(s) from the template" if seeded else "already present (left untouched)")
    except Exception as exc:
        rep.add(FAIL, "dbt project", f"{type(exc).__name__}: {str(exc)[:200]}")
    try:
        from web.volumes import ensure_default_volumes
        ensure_default_volumes()
        rep.add(OK, "default volumes", "ready")
    except Exception as exc:
        rep.add(WARN, "default volumes", f"{type(exc).__name__}: {str(exc)[:200]}")
    try:
        from web.workspace import init_workspace_directories
        init_workspace_directories()
        rep.add(OK, "notebook workspace", "folders ready")
    except Exception as exc:
        rep.add(WARN, "notebook workspace", f"{type(exc).__name__}: {str(exc)[:200]}")


_OPTIONAL = [("dbt.adapters.duckdb", "dbt runs and the Git sync's `dbt parse` check", "dbt-duckdb"), ("confluent_kafka", "streaming ingestion (Kafka / Redpanda)", "confluent-kafka"),
             ("fastavro", "Avro messages with a Schema Registry", "fastavro"), ("grpc_tools", "Protobuf messages with a Schema Registry", "grpcio-tools>=1.73,<1.77"),
             ("boto3", "S3 Auto-Loader sources", "boto3"), ("paramiko", "SFTP connections", "paramiko"), ("onelogin.saml2", "SAML sign-in", "python3-saml"),
             ("duckrun", "the Delta engine", "duckrun")]


def step_environment(rep: Report) -> None:
    import shutil
    rep.add(OK if shutil.which("git") else WARN, "git", "found" if shutil.which("git") else "not installed: Git sync will be unavailable")
    for mod, feature, pkg in _OPTIONAL:
        try:
            importlib.import_module(mod)
        except Exception:
            rep.add(WARN, f"package {pkg}", f"missing from this image: {feature} will not work. Rebuild the image (docker compose build / a newer tag).")
    if not (os.getenv("COMPUTE_TOKEN") or "").strip() and os.getenv("KUBERNETES_SERVICE_HOST"):
        rep.add(WARN, "COMPUTE_TOKEN", "not set: on Kubernetes the studio and its compute nodes are separate pods and need the same token (a Secret)")


def step_wait(rep: Report, urls: List[str], timeout: float) -> None:
    """Wait until each URL answers with any HTTP status below 500 (the service is up; whether the path exists does not matter)."""
    for url in urls:
        end = time.time() + timeout
        last = ""
        while True:
            try:
                urllib.request.urlopen(url, timeout=3)
                rep.add(OK, f"wait: {url}", "answers")
                break
            except urllib.error.HTTPError as exc:
                if exc.code < 500:
                    rep.add(OK, f"wait: {url}", f"answers (HTTP {exc.code})")
                    break
                last = f"HTTP {exc.code}"
            except Exception as exc:
                last = type(exc).__name__
            if time.time() > end:
                rep.add(FAIL, f"wait: {url}", f"no answer after {int(timeout)} s ({last})")
                break
            time.sleep(2)


# ---------------------------------------------------------------- main

def run(wait_for: Optional[List[str]] = None, wait_timeout: float = 120.0, check_only: bool = False) -> Report:
    rep = Report()
    step_directories(rep)
    if rep.failed:
        return rep
    if not check_only:
        try:
            with _lock(float(os.getenv("INIT_LOCK_TIMEOUT", "180"))):
                step_bootstrap_and_databases(rep)
                if not rep.failed:
                    step_seed(rep)
        except TimeoutError as exc:
            rep.add(FAIL, "lock", str(exc))
    step_environment(rep)
    if wait_for and not rep.failed:
        step_wait(rep, wait_for, wait_timeout)
    return rep


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m web.init", description=__doc__.split("\n\n")[0])
    ap.add_argument("--json", action="store_true", help="machine-readable result")
    ap.add_argument("--wait-for", default=os.getenv("INIT_WAIT_FOR", ""), help="comma-separated URLs to wait for (INIT_WAIT_FOR)")
    ap.add_argument("--wait-timeout", type=float, default=float(os.getenv("INIT_WAIT_TIMEOUT", "120")))
    ap.add_argument("--check-only", action="store_true", help="only check the directories and the environment; change nothing")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.ERROR, format="%(message)s")
    rep = run([u.strip() for u in args.wait_for.split(",") if u.strip()], args.wait_timeout, args.check_only)
    if args.json:
        print(json.dumps({"ok": not rep.failed, "warnings": rep.warnings, "steps": rep.items}, indent=2))
    else:
        for i in rep.items:
            print(f"[{i['status']:>4}] {i['step']}: {i['message']}")
        print(("init FAILED: the studio must not start." if rep.failed else f"init finished: ready ({rep.warnings} warning(s))."))
    return 1 if rep.failed else 0


if __name__ == "__main__":
    sys.exit(main())
