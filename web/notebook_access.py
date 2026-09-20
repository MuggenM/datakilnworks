"""
Role gating for JupyterLab (governance trust boundary).

Notebook kernels talk to the warehouse directly, so they cannot honour per-user column masking. Optionally
restricting who receives the Jupyter URL/token keeps masked roles from reaching an unmasked path through the Studio.
This gates what the Studio *hands out*; the Jupyter port itself must still be protected with a non-default
JUPYTER_TOKEN and network controls (see README, "Governance trust boundary").
"""

import os
from typing import Any, Dict, Set

DEFAULT_TOKEN = "datakilnworks"


def restrict_notebooks() -> bool:
    return os.getenv("GOVERNANCE_RESTRICT_NOTEBOOKS", "false").strip().lower() in ("1", "true", "yes", "on")


def notebook_roles() -> Set[str]:
    raw = os.getenv("GOVERNANCE_NOTEBOOK_ROLES", "admin,power_user")
    return {r.strip() for r in raw.split(",") if r.strip()}


def notebooks_allowed(user: Dict[str, Any]) -> bool:
    return (not restrict_notebooks()) or (user or {}).get("role") in notebook_roles()


def access_payload(user: Dict[str, Any], jupyter_port: Any, jupyter_token: str) -> Dict[str, Any]:
    """What the UI needs to show or hide Jupyter entry points; the token only travels to allowed roles."""
    allowed = notebooks_allowed(user)
    return {
        "restricted": restrict_notebooks(),
        "allowed": allowed,
        "port": jupyter_port if allowed else None,
        "token": jupyter_token if allowed else "",
        "default_token_in_use": jupyter_token == DEFAULT_TOKEN,
    }
