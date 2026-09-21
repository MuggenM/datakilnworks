"""
Who may execute notebook code in the Studio, and where (governance trust boundary).

Notebook kernels are ordinary Python processes: in the studio container they can read the warehouse files directly, so column
masking cannot be enforced inside them. Users a masking policy applies to therefore never get such a kernel. Depending on
GOVERNANCE_NOTEBOOK_EXECUTION they either run in the notebook sandbox or cannot run notebooks at all:

  sandbox (default)  exempt principals run in the studio's own kernels; principals a masking policy applies to run in the
                     sandbox container (sandbox/worker.py), whose kernels see data only through the governed SQL endpoint.
                     When the sandbox is not running they cannot execute, exactly as in `exempt` mode.
  exempt             only principals not subject to a masking policy may run cells / notebooks
  all                everyone runs in the studio's kernels (masking is then not enforced for notebook code)
"""

import os
from typing import Any, Dict, Optional

MODES = ("sandbox", "exempt", "all")
LOCAL, SANDBOX = "local", "sandbox"


def execution_mode() -> str:
    mode = os.getenv("GOVERNANCE_NOTEBOOK_EXECUTION", "sandbox").strip().lower()
    return mode if mode in MODES else "sandbox"


def execution_route(user: Optional[Dict[str, Any]]) -> Optional[str]:
    """`local`, `sandbox`, or None when this user may not execute notebooks right now."""
    mode = execution_mode()
    if mode == "all":
        return LOCAL
    from web.governance import gateway
    if not gateway.is_subject(user):
        return LOCAL
    if mode == "sandbox":
        from web import sandbox_client
        if sandbox_client.available():
            return SANDBOX
    return None


def execution_allowed(user: Optional[Dict[str, Any]]) -> bool:
    return execution_route(user) is not None


def execution_denied_message() -> str:
    if execution_mode() == "sandbox":
        return ("Running notebooks is not available right now: masking policies apply to you, so your code must run in the "
                "notebook sandbox, and the sandbox is not running. Ask an administrator to start the notebook-sandbox "
                "service. You can still open and edit notebooks, and use the SQL editor to query masked data.")
    return ("Running notebooks is not available while masking policies apply to you: notebook code can read the "
            "warehouse files directly, which masking cannot cover. You can still open and edit notebooks; use the SQL "
            "editor to query masked data.")
