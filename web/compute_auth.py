"""
Shared-secret authentication between the studio and its compute workers.

Workers execute arbitrary SQL, so an unauthenticated worker port is a full bypass of every governance control
(masking is applied by the studio before dispatch). The studio sends `X-Compute-Token` on every worker call and
workers reject requests without it. The token is COMPUTE_TOKEN if set, otherwise a per-install secret on the
shared warehouse volume.
"""

import hmac
import os
from typing import Dict

from web.secrets_store import load_or_create_secret

COMPUTE_TOKEN_HEADER = "X-Compute-Token"
PUBLIC_PATHS = ("/", "/health")


def get_compute_token() -> str:
    return os.getenv("COMPUTE_TOKEN") or load_or_create_secret("compute_token")


def compute_headers() -> Dict[str, str]:
    """Headers the studio attaches to every request it sends to a compute worker."""
    return {COMPUTE_TOKEN_HEADER: get_compute_token()}


def token_is_valid(presented: str) -> bool:
    return bool(presented) and hmac.compare_digest(presented, get_compute_token())
