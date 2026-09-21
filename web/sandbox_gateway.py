"""
The only door sandboxed notebook kernels have to data: governed SQL, answered as an Arrow stream.

A kernel presents the token the studio minted for its user (web/sandbox_client.py). Every call re-resolves that user, so a
disabled account or a changed policy applies immediately, and the SQL goes through the same permission check, governance
rewrite and audit as the SQL editor. Kernels never receive a session cookie and every other studio route refuses them
(see the middleware in app.py), so this endpoint cannot be used to widen access.
"""

import asyncio
import io
import json
import logging
import time
from typing import Optional

import jwt
import pyarrow as pa
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel

from web import sandbox_client
from web.governance import gateway
from web.governance.enforce import GovernanceBlocked

logger = logging.getLogger("localspark.sandbox")
router = APIRouter(prefix="/api/sandbox", tags=["sandbox"])

MAX_ROWS_CEILING = 5_000_000


class SandboxSql(BaseModel):
    sql: str
    max_rows: Optional[int] = None


def _user_from_request(request: Request) -> dict:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Sandbox token required.")
    try:
        username = sandbox_client.verify_kernel_token(header[7:].strip())
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired sandbox token. Re-run the cell.")
    from web.auth import get_user_by_username
    user = get_user_by_username(username)
    if not user or user.get("is_active", 1) != 1:
        raise HTTPException(status_code=401, detail="This account is not active.")
    return dict(user)


def _run(sql: str, user: dict, max_rows: int):
    """Governed execution on a private cursor; returns (arrow table, masked-column payload)."""
    from web import app as app_module
    from web.audit import log_query
    from web.app import enforce_sql_permissions
    try:
        enforce_sql_permissions(sql, user, action="READ")
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    gov = gateway.govern_sql(sql, user, client="notebook")
    username = user.get("username", "anonymous")
    if gov.blocked:
        log_query(query_text=sql, duration_ms=0, rows_produced=0, status="FAILED", error_message=gov.blocked,
                  client="NOTEBOOK_SANDBOX", user=username)
        raise HTTPException(status_code=403, detail=gov.blocked)
    masked = gateway.masked_columns_payload(gov)
    started = time.perf_counter()
    cur = app_module.get_duckrun_conn().con.cursor()
    try:
        try:
            # Unqualified names (hr.employees) resolve in the default catalog, as they do in a regular notebook kernel.
            cur.execute('USE "warehouse"')
            cur.execute(gov.sql)
            if not cur.description:
                table = pa.table({})
            else:
                batches, rows = [], 0
                reader = cur.fetch_record_batch(rows_per_batch=65536)
                for batch in reader:
                    batches.append(batch)
                    rows += batch.num_rows
                    if rows > max_rows:
                        raise HTTPException(status_code=413, detail=(
                            f"The result has more than {max_rows:,} rows. Notebook sandboxes hold results in memory: "
                            "filter or aggregate in the query instead."))
                table = pa.Table.from_batches(batches, schema=reader.schema)
        except HTTPException:
            raise
        except Exception as exc:
            log_query(query_text=sql, duration_ms=(time.perf_counter() - started) * 1000, rows_produced=0, status="FAILED",
                      error_message=str(exc), client="NOTEBOOK_SANDBOX", user=username)
            raise HTTPException(status_code=400, detail=str(exc))
    finally:
        cur.close()
    log_query(query_text=sql, duration_ms=(time.perf_counter() - started) * 1000, rows_produced=table.num_rows,
              status="SUCCESS", client="NOTEBOOK_SANDBOX", user=username, masked_columns=len(masked))
    return table, masked


@router.post("/sql")
async def sandbox_sql(payload: SandboxSql, request: Request):
    user = _user_from_request(request)
    max_rows = min(payload.max_rows or MAX_ROWS_CEILING, MAX_ROWS_CEILING)
    table, masked = await asyncio.to_thread(_run, payload.sql, user, max_rows)
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    # Header values must be latin-1; the names come from the catalog, so escape rather than trust them.
    header = json.dumps([f"{m['table']}.{m['column']}:{m['mask']}" for m in masked], ensure_ascii=True)[:4000]
    return Response(content=sink.getvalue(), media_type="application/vnd.apache.arrow.stream",
                    headers={"X-Masked-Columns": header})
