import os
import json
import hashlib
import sqlite3
import datetime
import logging
from typing import Optional, Dict, Any, List

from web.audit import get_db_connection

logger = logging.getLogger("localspark.recents")


def init_recents_db():
    """Ensures the recents table and indexes exist in history.db."""
    try:
        with get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS recents (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT 'admin',
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    subtitle TEXT DEFAULT '',
                    last_accessed_at TEXT NOT NULL,
                    access_count INTEGER NOT NULL DEFAULT 1,
                    is_pinned INTEGER NOT NULL DEFAULT 0,
                    metadata TEXT DEFAULT '{}'
                );
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_recents_user_type_item 
                ON recents(user_id, item_type, item_id);
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_recents_user_accessed 
                ON recents(user_id, last_accessed_at DESC);
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_recents_user_pinned 
                ON recents(user_id, is_pinned DESC, last_accessed_at DESC);
            """)
    except Exception as e:
        logger.error(f"Failed to initialize recents table: {e}")


# Initialize table on import
init_recents_db()


def _generate_recent_id(user_id: str, item_type: str, item_id: str) -> str:
    seed = f"{user_id}:{item_type}:{item_id}".encode("utf-8")
    return f"rec_{hashlib.sha256(seed).hexdigest()[:16]}"


def record_recent(
    item_type: str,
    item_id: str,
    title: str,
    subtitle: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    user_id: str = "admin"
) -> Dict[str, Any]:
    """
    Records or updates a recent item access event for a specific user.
    If the asset was previously accessed by this user, increments access_count
    and updates last_accessed_at while preserving pin status.
    """
    init_recents_db()
    uid = (user_id or "admin").strip().lower()
    itype = item_type.strip().lower()
    iid = item_id.strip()
    clean_title = (title or os.path.basename(iid) or iid).strip()
    clean_sub = (subtitle or "").strip()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_json = json.dumps(metadata or {})
    record_id = _generate_recent_id(uid, itype, iid)

    try:
        with get_db_connection() as conn:
            conn.execute("""
                INSERT INTO recents (
                    id, user_id, item_type, item_id, title, subtitle,
                    last_accessed_at, access_count, is_pinned, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, ?)
                ON CONFLICT(user_id, item_type, item_id) DO UPDATE SET
                    title = excluded.title,
                    subtitle = CASE WHEN excluded.subtitle != '' THEN excluded.subtitle ELSE recents.subtitle END,
                    last_accessed_at = excluded.last_accessed_at,
                    access_count = recents.access_count + 1,
                    metadata = CASE WHEN excluded.metadata != '{}' THEN excluded.metadata ELSE recents.metadata END;
            """, (record_id, uid, itype, iid, clean_title, clean_sub, now_str, meta_json))

            # Fetch updated row
            cursor = conn.execute("""
                SELECT id, user_id, item_type, item_id, title, subtitle,
                       last_accessed_at, access_count, is_pinned, metadata
                FROM recents WHERE user_id = ? AND item_type = ? AND item_id = ?
            """, (uid, itype, iid))
            row = cursor.fetchone()
            if row:
                res = dict(row)
                try:
                    res["metadata"] = json.loads(res["metadata"])
                except Exception:
                    res["metadata"] = {}
                res["is_pinned"] = bool(res["is_pinned"])
                return res
    except Exception as e:
        logger.warning(f"Failed to record recent item ({itype}: {iid}): {e}")

    return {
        "id": record_id,
        "user_id": uid,
        "item_type": itype,
        "item_id": iid,
        "title": clean_title,
        "subtitle": clean_sub,
        "last_accessed_at": now_str,
        "access_count": 1,
        "is_pinned": False,
        "metadata": metadata or {}
    }


def get_recents(
    user_id: str = "admin",
    item_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Retrieves recent items for a user, sorted by pinned status and access timestamp.
    Supports filtering by item_type and fuzzy title/subtitle searching.
    """
    init_recents_db()
    uid = (user_id or "admin").strip().lower()
    conditions = ["user_id = ?"]
    params: List[Any] = [uid]

    if item_type and item_type.lower() != "all":
        conditions.append("item_type = ?")
        params.append(item_type.strip().lower())

    if search and search.strip():
        term = f"%{search.strip()}%"
        conditions.append("(title LIKE ? OR subtitle LIKE ? OR item_id LIKE ?)")
        params.extend([term, term, term])

    where_clause = " WHERE " + " AND ".join(conditions)
    query = f"""
        SELECT id, user_id, item_type, item_id, title, subtitle,
               last_accessed_at, access_count, is_pinned, metadata
        FROM recents
        {where_clause}
        ORDER BY is_pinned DESC, last_accessed_at DESC
        LIMIT ?
    """
    params.append(max(1, min(limit, 200)))

    results = []
    try:
        with get_db_connection() as conn:
            cursor = conn.execute(query, tuple(params))
            for row in cursor.fetchall():
                item = dict(row)
                item["is_pinned"] = bool(item["is_pinned"])
                try:
                    item["metadata"] = json.loads(item["metadata"])
                except Exception:
                    item["metadata"] = {}
                results.append(item)
    except Exception as e:
        logger.error(f"Failed to fetch recents for user {uid}: {e}")

    return results


def toggle_pin_recent(item_type: str, item_id: str, user_id: str = "admin") -> Dict[str, Any]:
    """Toggles the pinned / favorite status of a recent item."""
    init_recents_db()
    uid = (user_id or "admin").strip().lower()
    itype = item_type.strip().lower()
    iid = item_id.strip()

    try:
        with get_db_connection() as conn:
            cursor = conn.execute("""
                SELECT is_pinned FROM recents 
                WHERE user_id = ? AND item_type = ? AND item_id = ?
            """, (uid, itype, iid))
            row = cursor.fetchone()
            if not row:
                return {"success": False, "error": "Item not found in recents"}

            new_pinned = 0 if row["is_pinned"] else 1
            conn.execute("""
                UPDATE recents SET is_pinned = ?
                WHERE user_id = ? AND item_type = ? AND item_id = ?
            """, (new_pinned, uid, itype, iid))

            return {
                "success": True,
                "item_type": itype,
                "item_id": iid,
                "is_pinned": bool(new_pinned)
            }
    except Exception as e:
        logger.error(f"Failed to toggle pin for recent ({itype}:{iid}): {e}")
        return {"success": False, "error": str(e)}


def delete_recent(item_type: str, item_id: str, user_id: str = "admin") -> bool:
    """Removes a single item from user's recents."""
    init_recents_db()
    uid = (user_id or "admin").strip().lower()
    itype = item_type.strip().lower()
    iid = item_id.strip()

    try:
        with get_db_connection() as conn:
            cursor = conn.execute("""
                DELETE FROM recents 
                WHERE user_id = ? AND item_type = ? AND item_id = ?
            """, (uid, itype, iid))
            return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Failed to delete recent ({itype}:{iid}): {e}")
        return False


def clear_recents(
    user_id: str = "admin",
    item_type: Optional[str] = None,
    include_pinned: bool = False
) -> int:
    """
    Clears recent items for a user. By default preserves pinned items unless include_pinned=True.
    Optionally filters by item_type. Returns number of deleted rows.
    """
    init_recents_db()
    uid = (user_id or "admin").strip().lower()
    conditions = ["user_id = ?"]
    params: List[Any] = [uid]

    if not include_pinned:
        conditions.append("is_pinned = 0")

    if item_type and item_type.lower() != "all":
        conditions.append("item_type = ?")
        params.append(item_type.strip().lower())

    where_clause = " WHERE " + " AND ".join(conditions)
    query = f"DELETE FROM recents {where_clause}"

    try:
        with get_db_connection() as conn:
            cursor = conn.execute(query, tuple(params))
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Failed to clear recents for user {uid}: {e}")
        return 0
