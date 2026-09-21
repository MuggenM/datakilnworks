"""
Column-name heuristics that suggest governance tags.

Suggestions never change anything by themselves: an admin reviews them and applies the ones they accept (source
'suggested'). Only column *names* and types are inspected, never row values, so classification cannot leak data.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

# (phrases, tag_key, tag_value, confidence, reason). A phrase matches whole underscore-separated tokens.
RULES: List[Tuple[Tuple[str, ...], str, str, float, str]] = [
    (("email", "e_mail", "email_address", "mail_address"), "pii", "email", 0.95, "column name looks like an email address"),
    (("phone", "phone_number", "mobile", "msisdn", "telephone", "cell_phone", "tel"), "pii", "phone", 0.9, "column name looks like a phone number"),
    (("ssn", "social_security", "social_security_number", "national_id", "nino", "tax_id", "passport", "passport_number"),
     "pii", "ssn", 0.95, "column name looks like a government identifier"),
    (("first_name", "last_name", "full_name", "surname", "given_name", "family_name", "middle_name"),
     "pii", "name", 0.9, "column name looks like a person's name"),
    (("address", "street", "street_address", "postal_code", "postcode", "zip", "zip_code", "city_address"),
     "pii", "address", 0.75, "column name looks like a postal address"),
    (("dob", "birth_date", "birthdate", "date_of_birth", "birthday", "birth_year"),
     "pii", "dob", 0.9, "column name looks like a date of birth"),
    (("ip", "ip_address", "ip_addr", "ipv4", "ipv6", "client_ip", "remote_ip"), "pii", "ip", 0.8, "column name looks like an IP address"),
    (("iban", "credit_card", "card_number", "cc_number", "ccn", "pan", "account_number", "routing_number", "bank_account"),
     "pii", "financial", 0.9, "column name looks like a financial account identifier"),
    (("password", "passwd", "pwd", "secret", "api_key", "apikey", "access_token", "private_key", "credential", "credentials"),
     "sensitivity", "restricted", 0.9, "column name looks like a credential"),
    (("salary", "income", "compensation", "wage", "bonus", "net_worth"),
     "sensitivity", "confidential", 0.8, "column name looks like compensation data"),
    (("name",), "pii", "name", 0.35, "generic 'name' column (may or may not identify a person)"),
]

_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize_column_name(name: str) -> str:
    """`CustomerEmail` / `customer-email` / `Customer Email` -> `customer_email`."""
    s = _CAMEL_2.sub(r"\1_\2", _CAMEL_1.sub(r"\1_\2", name or ""))
    return _NON_ALNUM.sub("_", s.lower()).strip("_")


def classify_column(column: str, data_type: str = "") -> Optional[Dict[str, Any]]:
    """Best rule for one column, or None. Highest confidence wins; longer (more specific) phrases break ties."""
    padded = f"_{normalize_column_name(column)}_"
    best: Optional[Tuple[float, int, Dict[str, Any]]] = None
    for phrases, key, value, confidence, reason in RULES:
        for phrase in phrases:
            if f"_{phrase}_" in padded:
                # Text identifiers are what we can mask meaningfully; dampen matches on non-text columns.
                adjusted = confidence if not data_type or "CHAR" in data_type.upper() or "TEXT" in data_type.upper() \
                    or "STRING" in data_type.upper() or value in ("dob", "financial") else round(confidence * 0.75, 2)
                candidate = (adjusted, len(phrase), {"tag_key": key, "tag_value": value, "confidence": adjusted,
                                                     "reason": f"{reason} ('{phrase}')"})
                if best is None or candidate[:2] > best[:2]:
                    best = candidate
    return best[2] if best else None


def suggest_for_columns(columns: List[Dict[str, Any]], already_tagged: Dict[Tuple[str, str, str, str], set],
                        min_confidence: float = 0.5) -> List[Dict[str, Any]]:
    """
    `columns` come from catalog_meta.list_columns. `already_tagged` maps (catalog, schema, table, column) -> {tag keys}
    so a column that already carries the suggested tag key is not suggested again.
    """
    out = []
    for c in columns:
        hit = classify_column(c["column"], c.get("type", ""))
        if not hit or hit["confidence"] < min_confidence:
            continue
        key = (c["catalog"].lower(), c["schema"].lower(), c["table"].lower(), c["column"].lower())
        if hit["tag_key"] in already_tagged.get(key, set()):
            continue
        out.append({"catalog": c["catalog"], "schema_name": c["schema"], "table_name": c["table"], "column_name": c["column"],
                    "data_type": c.get("type", ""), **hit})
    out.sort(key=lambda s: (-s["confidence"], s["catalog"], s["schema_name"], s["table_name"], s["column_name"]))
    return out
