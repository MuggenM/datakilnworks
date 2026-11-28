"""
Webhook Alerts - Generic webhook integration for monitoring and alerts.

Supports sending HTTP POST requests to any webhook endpoint with customizable
payloads, headers, and retry logic. Works with Discord, Teams, PagerDuty, etc.
"""

import os
import json
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime
import time

WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "/workspace/warehouse")
METADATA_DIR = os.path.join(WAREHOUSE_DIR, ".metadata")
WEBHOOK_CONFIG_FILE = os.path.join(METADATA_DIR, "webhook_config.json")
WEBHOOK_HISTORY_FILE = os.path.join(METADATA_DIR, "webhook_history.json")

# Webhook types with default payload templates
WEBHOOK_TYPES = {
    "generic": {
        "name": "Generic JSON",
        "content_type": "application/json",
        "template": {
            "title": "{title}",
            "message": "{message}",
            "timestamp": "{timestamp}",
            "data": "{data}"
        }
    },
    "discord": {
        "name": "Discord",
        "content_type": "application/json",
        "template": {
            "content": "{message}",
            "embeds": [{
                "title": "{title}",
                "description": "{message}",
                "color": "{color}",
                "timestamp": "{timestamp}",
                "footer": {
                    "text": "Databricks Local Studio"
                }
            }]
        }
    },
    "teams": {
        "name": "Microsoft Teams",
        "content_type": "application/json",
        "template": {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "{color}",
            "summary": "{title}",
            "sections": [{
                "activityTitle": "{title}",
                "activitySubtitle": "{timestamp}",
                "text": "{message}",
                "facts": "{facts}"
            }]
        }
    },
    "pagerduty": {
        "name": "PagerDuty",
        "content_type": "application/json",
        "template": {
            "routing_key": "{routing_key}",
            "event_action": "trigger",
            "payload": {
                "summary": "{title}",
                "severity": "{severity}",
                "source": "databricks-studio",
                "timestamp": "{timestamp}",
                "custom_details": "{data}"
            }
        }
    }
}

DEFAULT_WEBHOOK_CONFIG = {
    "enabled": False,
    "webhooks": [],
    "max_retries": 3,
    "retry_delay": 2,
    "timeout": 30
}


def load_config() -> Dict[str, Any]:
    """Load webhook configuration from file."""
    if os.path.exists(WEBHOOK_CONFIG_FILE):
        try:
            with open(WEBHOOK_CONFIG_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading webhook config: {e}")
            return DEFAULT_WEBHOOK_CONFIG.copy()
    return DEFAULT_WEBHOOK_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> bool:
    """Save webhook configuration to file."""
    try:
        os.makedirs(METADATA_DIR, exist_ok=True)
        with open(WEBHOOK_CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving webhook config: {e}")
        return False


def load_history(limit: int = 100) -> List[Dict[str, Any]]:
    """Load webhook execution history."""
    if os.path.exists(WEBHOOK_HISTORY_FILE):
        try:
            with open(WEBHOOK_HISTORY_FILE, "r") as f:
                history = json.load(f)
                return history[-limit:] if len(history) > limit else history
        except Exception as e:
            print(f"Error loading webhook history: {e}")
            return []
    return []


def save_history_entry(entry: Dict[str, Any]) -> bool:
    """Save a webhook execution history entry."""
    try:
        history = load_history(limit=1000)  # Keep last 1000 entries
        history.append(entry)

        os.makedirs(METADATA_DIR, exist_ok=True)
        with open(WEBHOOK_HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving webhook history: {e}")
        return False


def add_webhook(
    name: str,
    url: str,
    webhook_type: str = "generic",
    custom_headers: Optional[Dict[str, str]] = None,
    custom_template: Optional[Dict[str, Any]] = None,
    description: str = "",
    auth_type: str = "none",
    auth_token: str = ""
) -> Dict[str, Any]:
    """Add a new webhook."""
    config = load_config()

    # Check if webhook already exists
    for webhook in config.get("webhooks", []):
        if webhook["name"] == name:
            return {
                "success": False,
                "error": f"Webhook with name '{name}' already exists"
            }

    # Validate webhook type
    if webhook_type not in WEBHOOK_TYPES and not custom_template:
        return {
            "success": False,
            "error": f"Invalid webhook type '{webhook_type}'. Must provide custom_template for unknown types."
        }

    webhook = {
        "id": f"webhook_{len(config.get('webhooks', []))}_{int(datetime.now().timestamp())}",
        "name": name,
        "url": url,
        "type": webhook_type,
        "description": description,
        "custom_headers": custom_headers or {},
        "custom_template": custom_template,
        "auth_type": auth_type,  # none, bearer, api_key
        "auth_token": auth_token,
        "created_at": datetime.now().isoformat(),
        "enabled": True,
        "success_count": 0,
        "failure_count": 0,
        "last_triggered": None
    }

    if "webhooks" not in config:
        config["webhooks"] = []

    config["webhooks"].append(webhook)

    if save_config(config):
        return {
            "success": True,
            "webhook": webhook
        }
    else:
        return {
            "success": False,
            "error": "Failed to save configuration"
        }


def remove_webhook(webhook_id: str) -> Dict[str, Any]:
    """Remove a webhook."""
    config = load_config()

    webhooks = config.get("webhooks", [])
    original_count = len(webhooks)

    config["webhooks"] = [w for w in webhooks if w["id"] != webhook_id]

    if len(config["webhooks"]) < original_count:
        if save_config(config):
            return {"success": True}
        else:
            return {"success": False, "error": "Failed to save configuration"}

    return {"success": False, "error": "Webhook not found"}


def update_webhook(webhook_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    """Update a webhook configuration."""
    config = load_config()

    webhooks = config.get("webhooks", [])
    webhook = next((w for w in webhooks if w["id"] == webhook_id), None)

    if not webhook:
        return {
            "success": False,
            "error": "Webhook not found"
        }

    # Update allowed fields
    allowed_fields = ["name", "url", "type", "description", "custom_headers",
                     "custom_template", "enabled", "auth_type", "auth_token"]
    for field in allowed_fields:
        if field in updates:
            webhook[field] = updates[field]

    webhook["updated_at"] = datetime.now().isoformat()

    if save_config(config):
        return {
            "success": True,
            "webhook": webhook
        }
    else:
        return {
            "success": False,
            "error": "Failed to save configuration"
        }


def interpolate_template(template: Any, variables: Dict[str, Any]) -> Any:
    """Recursively interpolate variables in template."""
    if isinstance(template, str):
        # A value that is exactly one placeholder takes the variable as it is: Teams' MessageCard needs `facts` as a
        # LIST of {name, value}; as a JSON string inside the card it was ignored. Embedded in text, a structured value is
        # written as JSON (not Python's repr).
        for key, value in variables.items():
            if template == f"{{{key}}}" and not isinstance(value, str):
                return value
        result = template
        for key, value in variables.items():
            placeholder = f"{{{key}}}"
            if placeholder in result:
                result = result.replace(placeholder, value if isinstance(value, str) else json.dumps(value))
        return result
    elif isinstance(template, dict):
        return {k: interpolate_template(v, variables) for k, v in template.items()}
    elif isinstance(template, list):
        return [interpolate_template(item, variables) for item in template]
    else:
        return template


def send_webhook(
    webhook_id: str,
    title: str,
    message: str,
    data: Optional[Dict[str, Any]] = None,
    severity: str = "info",
    color: Optional[str] = None
) -> Dict[str, Any]:
    """Send a webhook notification."""
    config = load_config()

    if not config.get("enabled", False):
        return {
            "success": False,
            "error": "Webhook integration is not enabled"
        }

    # Find webhook
    webhook = next((w for w in config.get("webhooks", []) if w["id"] == webhook_id), None)

    if not webhook:
        return {
            "success": False,
            "error": "Webhook not found"
        }

    if not webhook.get("enabled", True):
        return {
            "success": False,
            "error": f"Webhook '{webhook['name']}' is disabled"
        }

    # Prepare variables for template interpolation
    severity_colors = {
        "critical": "15158332",  # Red for Discord
        "error": "15158332",
        "warning": "16776960",   # Yellow
        "info": "3447003",       # Blue
        "success": "3066993"     # Green
    }

    variables = {
        "title": title,
        "message": message,
        "timestamp": datetime.now().isoformat(),
        "severity": severity,
        "color": color or severity_colors.get(severity, "3447003"),
        "data": json.dumps(data) if data else "{}",
        "facts": [{"name": k, "value": str(v)} for k, v in (data or {}).items()],   # a list: see interpolate_template
        "routing_key": webhook.get("auth_token", "")
    }

    # Get payload template
    if webhook.get("custom_template"):
        payload_template = webhook["custom_template"]
    else:
        webhook_type = webhook.get("type", "generic")
        payload_template = WEBHOOK_TYPES.get(webhook_type, WEBHOOK_TYPES["generic"])["template"]

    # Interpolate variables into template
    payload = interpolate_template(payload_template, variables)

    # Prepare headers
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Databricks-Local-Studio/1.0"
    }

    # Add custom headers
    if webhook.get("custom_headers"):
        headers.update(webhook["custom_headers"])

    # Add authentication
    auth_type = webhook.get("auth_type", "none")
    if auth_type == "bearer" and webhook.get("auth_token"):
        headers["Authorization"] = f"Bearer {webhook['auth_token']}"
    elif auth_type == "api_key" and webhook.get("auth_token"):
        headers["X-API-Key"] = webhook["auth_token"]

    # Send request with retries
    max_retries = config.get("max_retries", 3)
    retry_delay = config.get("retry_delay", 2)
    timeout = config.get("timeout", 30)

    last_error = None
    start_time = time.time()

    for attempt in range(max_retries):
        try:
            response = requests.post(
                webhook["url"],
                json=payload,
                headers=headers,
                timeout=timeout
            )

            duration = time.time() - start_time

            # Log history
            history_entry = {
                "webhook_id": webhook_id,
                "webhook_name": webhook["name"],
                "title": title,
                "timestamp": datetime.now().isoformat(),
                "status_code": response.status_code,
                "success": 200 <= response.status_code < 300,
                "attempt": attempt + 1,
                "duration": round(duration, 3),
                "error": None if 200 <= response.status_code < 300 else response.text
            }
            save_history_entry(history_entry)

            if 200 <= response.status_code < 300:
                # Update webhook stats
                webhook["success_count"] = webhook.get("success_count", 0) + 1
                webhook["last_triggered"] = datetime.now().isoformat()
                save_config(config)

                return {
                    "success": True,
                    "status_code": response.status_code,
                    "duration": round(duration, 3),
                    "attempt": attempt + 1
                }
            else:
                last_error = f"HTTP {response.status_code}: {response.text}"

        except requests.exceptions.Timeout:
            last_error = f"Request timed out after {timeout}s"
        except requests.exceptions.RequestException as e:
            last_error = f"Connection error: {str(e)}"
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"

        # Wait before retry (except on last attempt)
        if attempt < max_retries - 1:
            time.sleep(retry_delay)

    # All retries failed
    duration = time.time() - start_time

    # Log failure
    history_entry = {
        "webhook_id": webhook_id,
        "webhook_name": webhook["name"],
        "title": title,
        "timestamp": datetime.now().isoformat(),
        "status_code": 0,
        "success": False,
        "attempt": max_retries,
        "duration": round(duration, 3),
        "error": last_error
    }
    save_history_entry(history_entry)

    # Update webhook stats
    webhook["failure_count"] = webhook.get("failure_count", 0) + 1
    save_config(config)

    return {
        "success": False,
        "error": last_error,
        "attempts": max_retries
    }


def test_webhook(webhook_id: str) -> Dict[str, Any]:
    """Test a webhook by sending a test notification."""
    return send_webhook(
        webhook_id=webhook_id,
        title="Test Notification",
        message="This is a test notification from Databricks Local Studio. If you're seeing this, your webhook is working correctly! 🎉",
        data={
            "test": True,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        severity="info"
    )


def get_webhooks() -> List[Dict[str, Any]]:
    """Get all configured webhooks."""
    config = load_config()
    return config.get("webhooks", [])


def get_webhook(webhook_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific webhook by ID."""
    webhooks = get_webhooks()
    return next((w for w in webhooks if w["id"] == webhook_id), None)


def get_webhook_types() -> Dict[str, Any]:
    """Get available webhook types with templates."""
    return WEBHOOK_TYPES


def clear_history() -> bool:
    """Clear webhook execution history."""
    try:
        if os.path.exists(WEBHOOK_HISTORY_FILE):
            os.remove(WEBHOOK_HISTORY_FILE)
        return True
    except Exception as e:
        print(f"Error clearing webhook history: {e}")
        return False
