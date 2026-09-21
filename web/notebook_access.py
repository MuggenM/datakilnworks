"""
Who may execute notebook code in the Studio (governance trust boundary).

Notebook kernels are ordinary Python processes: they can read the warehouse files directly, so column masking cannot be
enforced inside them. Until kernels are sandboxed, code execution is limited to principals that no masking policy applies
to; masked users can still open and edit notebooks, but not run them.

GOVERNANCE_NOTEBOOK_EXECUTION
  exempt (default)  only principals not subject to a masking policy may run cells / notebooks
  all               everyone who can open a notebook may run it (masking is then not enforced for notebook code)
"""

import os
from typing import Any, Dict, Optional

MODES = ("exempt", "all")


def execution_mode() -> str:
    mode = os.getenv("GOVERNANCE_NOTEBOOK_EXECUTION", "exempt").strip().lower()
    return mode if mode in MODES else "exempt"


def execution_allowed(user: Optional[Dict[str, Any]]) -> bool:
    """False only when masking policies apply to this user and execution is limited to exempt principals."""
    if execution_mode() == "all":
        return True
    from web.governance import gateway
    return not gateway.is_subject(user)


def execution_denied_message() -> str:
    return ("Running notebooks is not available while masking policies apply to you: notebook code can read the "
            "warehouse files directly, which masking cannot cover. You can still open and edit notebooks; use the SQL "
            "editor to query masked data.")
