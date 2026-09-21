"""
Platform Settings - LLM Endpoints & Cloud API Keys Management Service.
Provides secure storage, environment synchronization, key masking,
and live connectivity diagnostics for Ollama, LM Studio, OpenAI, Gemini, and Anthropic.
"""

import os
import json
import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import requests

logger = logging.getLogger("localspark.llm_settings")

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
LLM_CONFIG_FILE = os.path.join(METADATA_DIR, "llm_config.json")

DEFAULT_LLM_CONFIG = {
    "ollama_host": os.environ.get("OLLAMA_HOST", "http://10.0.2.2:11434"),
    "lmstudio_host": os.environ.get("LMSTUDIO_HOST", "http://10.0.2.2:1234"),
    "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
    "gemini_api_key": os.environ.get("GEMINI_API_KEY", ""),
    "anthropic_api_key": os.environ.get("ANTHROPIC_API_KEY", ""),
    "default_provider": "auto",
    "default_model": "",
    "auto_load_models": True,
    "updated_at": None,
    "updated_by": None
}


def mask_key(key: Optional[str]) -> str:
    """Returns a masked representation of an API key for safe UI display."""
    if not key:
        return ""
    key_str = str(key).strip()
    if len(key_str) <= 8:
        return "••••••••"
    return f"{key_str[:4]}••••••••{key_str[-4:]}"


def is_masked_or_empty(val: Optional[str]) -> bool:
    """Checks if a string is empty or contains masking characters."""
    if not val:
        return True
    return "••••" in val or "****" in val


def sync_environment_and_runtime(config: Dict[str, Any]):
    """
    Propagates current LLM settings to os.environ and hot-updates
    active runtime modules (genie, playground, copilot) without requiring restarts.
    """
    ollama_host = (config.get("ollama_host") or "").strip().rstrip("/")
    lmstudio_host = (config.get("lmstudio_host") or "").strip().rstrip("/")
    openai_key = (config.get("openai_api_key") or "").strip()
    gemini_key = (config.get("gemini_api_key") or "").strip()
    anthropic_key = (config.get("anthropic_api_key") or "").strip()

    if ollama_host:
        os.environ["OLLAMA_HOST"] = ollama_host
    elif "OLLAMA_HOST" in os.environ and not config.get("ollama_host"):
        os.environ.pop("OLLAMA_HOST", None)

    if lmstudio_host:
        os.environ["LMSTUDIO_HOST"] = lmstudio_host
        os.environ["LM_STUDIO_HOST"] = lmstudio_host
    elif "LMSTUDIO_HOST" in os.environ and not config.get("lmstudio_host"):
        os.environ.pop("LMSTUDIO_HOST", None)
        os.environ.pop("LM_STUDIO_HOST", None)

    if openai_key:
        os.environ["OPENAI_API_KEY"] = openai_key
    elif "OPENAI_API_KEY" in os.environ and not config.get("openai_api_key"):
        os.environ.pop("OPENAI_API_KEY", None)

    if gemini_key:
        os.environ["GEMINI_API_KEY"] = gemini_key
    elif "GEMINI_API_KEY" in os.environ and not config.get("gemini_api_key"):
        os.environ.pop("GEMINI_API_KEY", None)

    if anthropic_key:
        os.environ["ANTHROPIC_API_KEY"] = anthropic_key
    elif "ANTHROPIC_API_KEY" in os.environ and not config.get("anthropic_api_key"):
        os.environ.pop("ANTHROPIC_API_KEY", None)

    # Hot-update web.genie candidate hosts
    try:
        import web.genie as genie
        if ollama_host:
            if ollama_host in genie.OLLAMA_CANDIDATE_HOSTS:
                genie.OLLAMA_CANDIDATE_HOSTS.remove(ollama_host)
            genie.OLLAMA_CANDIDATE_HOSTS.insert(0, ollama_host)
        if lmstudio_host:
            if lmstudio_host in genie.LMSTUDIO_CANDIDATE_HOSTS:
                genie.LMSTUDIO_CANDIDATE_HOSTS.remove(lmstudio_host)
            genie.LMSTUDIO_CANDIDATE_HOSTS.insert(0, lmstudio_host)
    except Exception as e:
        logger.debug(f"Could not hot-sync web.genie: {e}")

    # Hot-update web.playground candidate hosts
    try:
        import web.playground as playground
        if ollama_host:
            if ollama_host in playground.OLLAMA_CANDIDATE_HOSTS:
                playground.OLLAMA_CANDIDATE_HOSTS.remove(ollama_host)
            playground.OLLAMA_CANDIDATE_HOSTS.insert(0, ollama_host)
        if lmstudio_host:
            if lmstudio_host in playground.LMSTUDIO_CANDIDATE_HOSTS:
                playground.LMSTUDIO_CANDIDATE_HOSTS.remove(lmstudio_host)
            playground.LMSTUDIO_CANDIDATE_HOSTS.insert(0, lmstudio_host)
    except Exception as e:
        logger.debug(f"Could not hot-sync web.playground: {e}")


def load_llm_config() -> Dict[str, Any]:
    """Loads LLM settings from disk, filling in defaults and syncing runtime environment."""
    os.makedirs(METADATA_DIR, exist_ok=True)
    config = DEFAULT_LLM_CONFIG.copy()

    if os.path.exists(LLM_CONFIG_FILE):
        try:
            with open(LLM_CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                config.update(saved)
        except Exception as e:
            logger.warning(f"Failed to read {LLM_CONFIG_FILE}, using defaults: {e}")

    sync_environment_and_runtime(config)
    return config


def get_masked_llm_config() -> Dict[str, Any]:
    """Returns platform LLM configuration with secrets masked for safe client transmission."""
    raw = load_llm_config()
    return {
        "ollama_host": raw.get("ollama_host") or "http://10.0.2.2:11434",
        "lmstudio_host": raw.get("lmstudio_host") or "http://10.0.2.2:1234",
        "openai_api_key_masked": mask_key(raw.get("openai_api_key")),
        "openai_api_key_set": bool(raw.get("openai_api_key")),
        "gemini_api_key_masked": mask_key(raw.get("gemini_api_key")),
        "gemini_api_key_set": bool(raw.get("gemini_api_key")),
        "anthropic_api_key_masked": mask_key(raw.get("anthropic_api_key")),
        "anthropic_api_key_set": bool(raw.get("anthropic_api_key")),
        "default_provider": raw.get("default_provider") or "auto",
        "default_model": raw.get("default_model") or "",
        "auto_load_models": raw.get("auto_load_models", True),
        "updated_at": raw.get("updated_at"),
        "updated_by": raw.get("updated_by")
    }


def save_llm_config(payload: Dict[str, Any], updated_by: str = "admin") -> Dict[str, Any]:
    """
    Saves updated LLM endpoints and API keys.
    Handles masked keys (preserves existing key if masked placeholder is passed).
    """
    os.makedirs(METADATA_DIR, exist_ok=True)
    current = load_llm_config()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Update endpoints
    if "ollama_host" in payload and payload["ollama_host"] is not None:
        current["ollama_host"] = str(payload["ollama_host"]).strip()

    if "lmstudio_host" in payload and payload["lmstudio_host"] is not None:
        current["lmstudio_host"] = str(payload["lmstudio_host"]).strip()

    # Update Cloud API keys - preserve existing if masked or untouched
    if "openai_api_key" in payload:
        new_val = payload["openai_api_key"]
        if new_val == "__CLEAR__":
            current["openai_api_key"] = ""
        elif new_val and not is_masked_or_empty(new_val):
            current["openai_api_key"] = str(new_val).strip()

    if "gemini_api_key" in payload:
        new_val = payload["gemini_api_key"]
        if new_val == "__CLEAR__":
            current["gemini_api_key"] = ""
        elif new_val and not is_masked_or_empty(new_val):
            current["gemini_api_key"] = str(new_val).strip()

    if "anthropic_api_key" in payload:
        new_val = payload["anthropic_api_key"]
        if new_val == "__CLEAR__":
            current["anthropic_api_key"] = ""
        elif new_val and not is_masked_or_empty(new_val):
            current["anthropic_api_key"] = str(new_val).strip()

    if "default_provider" in payload and payload["default_provider"]:
        current["default_provider"] = str(payload["default_provider"]).strip()

    if "default_model" in payload and payload["default_model"] is not None:
        current["default_model"] = str(payload["default_model"]).strip()

    if "auto_load_models" in payload:
        current["auto_load_models"] = bool(payload["auto_load_models"])

    current["updated_at"] = now_str
    current["updated_by"] = updated_by

    try:
        with open(LLM_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)
        try:
            os.chmod(LLM_CONFIG_FILE, 0o600)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to write {LLM_CONFIG_FILE}: {e}")
        raise RuntimeError(f"Could not write platform LLM settings: {e}")

    sync_environment_and_runtime(current)
    return get_masked_llm_config()


def reset_llm_config(updated_by: str = "admin") -> Dict[str, Any]:
    """Resets platform LLM configuration back to defaults/env vars."""
    if os.path.exists(LLM_CONFIG_FILE):
        try:
            os.remove(LLM_CONFIG_FILE)
        except Exception as e:
            logger.warning(f"Could not remove {LLM_CONFIG_FILE}: {e}")

    config = DEFAULT_LLM_CONFIG.copy()
    config["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    config["updated_by"] = updated_by
    sync_environment_and_runtime(config)
    return get_masked_llm_config()


def test_llm_connection(
    provider: str,
    host: Optional[str] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None
) -> Dict[str, Any]:
    """
    Performs live connectivity, authentication, and model discovery tests
    for a specified LLM provider.
    """
    config = load_llm_config()
    prov = (provider or "").lower().strip()

    # 1. OLLAMA
    if prov == "ollama":
        target_host = (host or config.get("ollama_host") or "http://10.0.2.2:11434").strip().rstrip("/")
        if not target_host.startswith("http"):
            target_host = f"http://{target_host}"

        t0 = time.time()
        try:
            res = requests.get(f"{target_host}/api/tags", timeout=3.5)
            latency_ms = round((time.time() - t0) * 1000, 1)

            if res.status_code == 200:
                data = res.json()
                models = [m.get("name", "") for m in data.get("models", []) if m.get("name")]
                return {
                    "success": True,
                    "provider": "ollama",
                    "host": target_host,
                    "latency_ms": latency_ms,
                    "models": models,
                    "count": len(models),
                    "message": f"Successfully connected to Ollama at {target_host} ({len(models)} model(s) detected)."
                }
            else:
                return {
                    "success": False,
                    "provider": "ollama",
                    "host": target_host,
                    "latency_ms": latency_ms,
                    "error": f"Ollama HTTP {res.status_code}: {res.text[:200]}",
                    "message": f"Ollama server responded with error code {res.status_code}."
                }
        except requests.exceptions.ConnectionError:
            return {
                "success": False,
                "provider": "ollama",
                "host": target_host,
                "latency_ms": None,
                "error": f"Connection refused at {target_host}",
                "message": f"Could not reach Ollama at {target_host}. Ensure Ollama is running (OLLAMA_HOST=0.0.0.0)."
            }
        except Exception as e:
            return {
                "success": False,
                "provider": "ollama",
                "host": target_host,
                "latency_ms": None,
                "error": str(e),
                "message": f"Error connecting to Ollama: {str(e)}"
            }

    # 2. LM STUDIO
    elif prov in ("lmstudio", "lm_studio"):
        target_host = (host or config.get("lmstudio_host") or "http://10.0.2.2:1234").strip().rstrip("/")
        if not target_host.startswith("http"):
            target_host = f"http://{target_host}"

        t0 = time.time()
        try:
            # First attempt native LM Studio /api/v0/models to check loaded state
            try:
                res = requests.get(f"{target_host}/api/v0/models", timeout=3.5)
            except Exception:
                res = None

            latency_ms = round((time.time() - t0) * 1000, 1)

            if res and res.status_code == 200:
                data = res.json()
                raw_models = data.get("data", [])
                loaded = [m["id"] for m in raw_models if m.get("state") == "loaded"]
                all_models = [m["id"] for m in raw_models if m.get("id")]
                return {
                    "success": True,
                    "provider": "lmstudio",
                    "host": target_host,
                    "latency_ms": latency_ms,
                    "models": all_models,
                    "loaded_models": loaded,
                    "count": len(all_models),
                    "message": f"Connected to LM Studio at {target_host} ({len(all_models)} model(s) available, {len(loaded)} currently loaded)."
                }

            # Fallback to OpenAI-compatible endpoint /v1/models
            res2 = requests.get(f"{target_host}/v1/models", timeout=3.5)
            latency_ms2 = round((time.time() - t0) * 1000, 1)

            if res2.status_code == 200:
                data2 = res2.json()
                models2 = [m.get("id", "") for m in data2.get("data", []) if m.get("id")]
                return {
                    "success": True,
                    "provider": "lmstudio",
                    "host": target_host,
                    "latency_ms": latency_ms2,
                    "models": models2,
                    "loaded_models": [],
                    "count": len(models2),
                    "message": f"Connected to LM Studio v1 API at {target_host} ({len(models2)} model(s) available)."
                }
            else:
                return {
                    "success": False,
                    "provider": "lmstudio",
                    "host": target_host,
                    "latency_ms": latency_ms2,
                    "error": f"LM Studio HTTP {res2.status_code}: {res2.text[:200]}",
                    "message": f"LM Studio responded with error status {res2.status_code}."
                }
        except requests.exceptions.ConnectionError:
            return {
                "success": False,
                "provider": "lmstudio",
                "host": target_host,
                "latency_ms": None,
                "error": f"Connection refused at {target_host}",
                "message": f"Could not reach LM Studio at {target_host}. Ensure LM Studio Local Server is started on port 1234."
            }
        except Exception as e:
            return {
                "success": False,
                "provider": "lmstudio",
                "host": target_host,
                "latency_ms": None,
                "error": str(e),
                "message": f"Error connecting to LM Studio: {str(e)}"
            }

    # 3. OPENAI
    elif prov == "openai":
        target_key = api_key if api_key and not is_masked_or_empty(api_key) else config.get("openai_api_key", "")
        if not target_key:
            return {
                "success": False,
                "provider": "openai",
                "latency_ms": None,
                "error": "No API key configured",
                "message": "OpenAI API key is missing. Please enter your API key to test."
            }

        t0 = time.time()
        try:
            headers = {"Authorization": f"Bearer {target_key}"}
            res = requests.get("https://api.openai.com/v1/models", headers=headers, timeout=5.0)
            latency_ms = round((time.time() - t0) * 1000, 1)

            if res.status_code == 200:
                data = res.json()
                all_ids = [m.get("id", "") for m in data.get("data", [])]
                chat_models = [m for m in all_ids if "gpt" in m or "o1" in m or "o3" in m][:8]
                return {
                    "success": True,
                    "provider": "openai",
                    "latency_ms": latency_ms,
                    "models": chat_models or ["gpt-4o", "gpt-4o-mini"],
                    "count": len(chat_models),
                    "message": f"OpenAI API key verified successfully ({latency_ms}ms latency)."
                }
            elif res.status_code == 401:
                return {
                    "success": False,
                    "provider": "openai",
                    "latency_ms": latency_ms,
                    "error": "HTTP 401 Unauthorized",
                    "message": "Authentication failed: Invalid OpenAI API key."
                }
            else:
                return {
                    "success": False,
                    "provider": "openai",
                    "latency_ms": latency_ms,
                    "error": f"HTTP {res.status_code}: {res.text[:200]}",
                    "message": f"OpenAI returned status {res.status_code}."
                }
        except Exception as e:
            return {
                "success": False,
                "provider": "openai",
                "latency_ms": None,
                "error": str(e),
                "message": f"Failed to connect to OpenAI API: {str(e)}"
            }

    # 4. GOOGLE GEMINI
    elif prov == "gemini":
        target_key = api_key if api_key and not is_masked_or_empty(api_key) else config.get("gemini_api_key", "")
        if not target_key:
            return {
                "success": False,
                "provider": "gemini",
                "latency_ms": None,
                "error": "No API key configured",
                "message": "Google Gemini API key is missing. Please enter your API key to test."
            }

        t0 = time.time()
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models?key={target_key}"
            res = requests.get(url, timeout=5.0)
            latency_ms = round((time.time() - t0) * 1000, 1)

            if res.status_code == 200:
                data = res.json()
                raw_models = [m.get("name", "").replace("models/", "") for m in data.get("models", [])]
                gemini_models = [m for m in raw_models if "gemini" in m][:6]
                return {
                    "success": True,
                    "provider": "gemini",
                    "latency_ms": latency_ms,
                    "models": gemini_models or ["gemini-1.5-flash", "gemini-1.5-pro"],
                    "count": len(gemini_models),
                    "message": f"Google Gemini API key verified successfully ({latency_ms}ms latency)."
                }
            elif res.status_code in (400, 403):
                return {
                    "success": False,
                    "provider": "gemini",
                    "latency_ms": latency_ms,
                    "error": f"HTTP {res.status_code}",
                    "message": "Authentication failed: Invalid Google Gemini API key or quota issue."
                }
            else:
                return {
                    "success": False,
                    "provider": "gemini",
                    "latency_ms": latency_ms,
                    "error": f"HTTP {res.status_code}: {res.text[:200]}",
                    "message": f"Google Gemini API returned status {res.status_code}."
                }
        except Exception as e:
            return {
                "success": False,
                "provider": "gemini",
                "latency_ms": None,
                "error": str(e),
                "message": f"Failed to connect to Google Gemini API: {str(e)}"
            }

    # 5. ANTHROPIC CLAUDE
    elif prov == "anthropic":
        target_key = api_key if api_key and not is_masked_or_empty(api_key) else config.get("anthropic_api_key", "")
        if not target_key:
            return {
                "success": False,
                "provider": "anthropic",
                "latency_ms": None,
                "error": "No API key configured",
                "message": "Anthropic API key is missing. Please enter your API key to test."
            }

        t0 = time.time()
        try:
            # Test Anthropic models endpoint
            url = "https://api.anthropic.com/v1/models"
            headers = {
                "x-api-key": target_key,
                "anthropic-version": "2023-06-01"
            }
            res = requests.get(url, headers=headers, timeout=5.0)
            latency_ms = round((time.time() - t0) * 1000, 1)

            if res.status_code == 200:
                data = res.json()
                models = [m.get("id") for m in data.get("data", []) if m.get("id")]
                return {
                    "success": True,
                    "provider": "anthropic",
                    "latency_ms": latency_ms,
                    "models": models or ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"],
                    "count": len(models),
                    "message": f"Anthropic Claude API key verified successfully ({latency_ms}ms latency)."
                }
            elif res.status_code == 401:
                return {
                    "success": False,
                    "provider": "anthropic",
                    "latency_ms": latency_ms,
                    "error": "HTTP 401 Unauthorized",
                    "message": "Authentication failed: Invalid Anthropic API key."
                }
            else:
                return {
                    "success": False,
                    "provider": "anthropic",
                    "latency_ms": latency_ms,
                    "error": f"HTTP {res.status_code}: {res.text[:200]}",
                    "message": f"Anthropic returned status {res.status_code}."
                }
        except Exception as e:
            return {
                "success": False,
                "provider": "anthropic",
                "latency_ms": None,
                "error": str(e),
                "message": f"Failed to connect to Anthropic API: {str(e)}"
            }

    else:
        return {
            "success": False,
            "provider": provider,
            "latency_ms": None,
            "error": f"Unknown provider: {provider}",
            "message": f"Provider '{provider}' is not supported for live connectivity test."
        }


# Initialize configuration and environment on module load
load_llm_config()
