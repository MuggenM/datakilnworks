import os
import re
import time
import uuid
import logging
import threading
from typing import Dict, Any, List, Optional

import hashlib

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_output
from jupyter_client import KernelManager

logger = logging.getLogger("notebook_runner")

NOTEBOOKS_DIR = os.getenv("NOTEBOOKS_DIR", "/workspace/notebooks")


def get_safe_path(rel_path: str) -> str:
    """Resolve and sanitize relative path against NOTEBOOKS_DIR to avoid traversal."""
    cleaned = rel_path.strip().lstrip("/\\")
    full_path = os.path.abspath(os.path.join(NOTEBOOKS_DIR, cleaned))
    real_base = os.path.abspath(NOTEBOOKS_DIR)
    if os.path.commonpath([full_path, real_base]) != real_base:
        raise ValueError(f"Access denied: path '{rel_path}' escapes notebook directory")
    return full_path


def clean_ansi(text: str) -> str:
    """Remove ANSI escape sequences from strings (e.g. traceback color codes)."""
    if not text:
        return ""
    return re.sub(r"\x1b\[[0-9;]*[mK]", "", text)


class KernelSession:
    """Manages an active IPython kernel for a specific notebook."""

    def __init__(self, notebook_rel_path: str, owner: str = "anonymous"):
        self.notebook_rel_path = notebook_rel_path
        self.owner = owner
        self.km: Optional[KernelManager] = None
        self.kc = None
        self.lock = threading.Lock()
        self.last_active = time.time()
        self.status = "stopped"
        self.execution_count = 0

    # Subclasses (the sandbox worker's kernels) change how the kernel process is created.
    kernel_name = "python3"

    def _start_kwargs(self) -> Dict[str, Any]:
        return {}

    def is_alive(self) -> bool:
        return self.km is not None and self.km.is_alive()

    def start(self, timeout: int = 15):
        with self.lock:
            if self.km is not None and self.km.is_alive():
                return
            logger.info(f"Starting kernel for notebook: {self.notebook_rel_path}")
            self.status = "starting"
            km = KernelManager(kernel_name=self.kernel_name)
            km.start_kernel(**self._start_kwargs())
            kc = km.client()
            kc.start_channels()
            kc.wait_for_ready(timeout=timeout)
            self.km = km
            self.kc = kc
            self.status = "idle"
            self.last_active = time.time()
            logger.info(f"Kernel ready for notebook: {self.notebook_rel_path}")

    def shutdown(self):
        with self.lock:
            if self.kc is not None:
                try:
                    self.kc.stop_channels()
                except Exception:
                    pass
                self.kc = None
            if self.km is not None:
                try:
                    self.km.shutdown_kernel(now=True)
                except Exception:
                    pass
                self.km = None
            self.status = "stopped"

    def restart(self, timeout: int = 15):
        self.shutdown()
        self.start(timeout=timeout)

    def execute_code(self, code: str, timeout: int = 120) -> Dict[str, Any]:
        """Executes code in the kernel and captures all IOPub outputs."""
        self.start()
        with self.lock:
            self.last_active = time.time()
            self.status = "busy"
            start_t = time.perf_counter()

            raw_outputs = []
            final_status = "ok"
            exec_count = None

            try:
                msg_id = self.kc.execute(code)

                while True:
                    try:
                        msg = self.kc.get_iopub_msg(timeout=timeout)
                    except Exception as e:
                        logger.warning(f"Kernel timeout or IOPub error: {e}")
                        final_status = "error"
                        raw_outputs.append({
                            "output_type": "error",
                            "ename": "TimeoutError",
                            "evalue": f"Cell execution timed out after {timeout} seconds",
                            "traceback": [f"TimeoutError: Cell execution timed out after {timeout} seconds"]
                        })
                        break

                    header = msg.get("header", {})
                    msg_type = header.get("msg_type")
                    parent_id = msg.get("parent_header", {}).get("msg_id")
                    content = msg.get("content", {})

                    if parent_id != msg_id:
                        continue

                    if msg_type == "status":
                        if content.get("execution_state") == "idle":
                            break

                    elif msg_type == "stream":
                        stream_name = content.get("name", "stdout")
                        text_val = content.get("text", "")
                        raw_outputs.append({
                            "output_type": "stream",
                            "name": stream_name,
                            "text": text_val
                        })

                    elif msg_type in ("execute_result", "display_data"):
                        data_dict = content.get("data", {})
                        if "execution_count" in content:
                            exec_count = content["execution_count"]
                            self.execution_count = exec_count

                        formatted_data = {}
                        if "text/plain" in data_dict:
                            formatted_data["text/plain"] = data_dict["text/plain"]
                        if "text/html" in data_dict:
                            formatted_data["text/html"] = data_dict["text/html"]
                        if "image/png" in data_dict:
                            formatted_data["image/png"] = data_dict["image/png"]
                        if "application/json" in data_dict:
                            formatted_data["application/json"] = data_dict["application/json"]

                        if not formatted_data and data_dict:
                            formatted_data["text/plain"] = str(data_dict)

                        raw_outputs.append({
                            "output_type": msg_type,
                            "data": formatted_data,
                            "metadata": content.get("metadata", {}),
                            "execution_count": content.get("execution_count")
                        })

                    elif msg_type == "error":
                        final_status = "error"
                        cleaned_tb = [clean_ansi(line) for line in content.get("traceback", [])]
                        raw_outputs.append({
                            "output_type": "error",
                            "ename": content.get("ename", "Exception"),
                            "evalue": content.get("evalue", ""),
                            "traceback": cleaned_tb
                        })

                try:
                    reply = self.kc.get_shell_msg(timeout=5)
                    if reply and reply.get("parent_header", {}).get("msg_id") == msg_id:
                        rep_content = reply.get("content", {})
                        if rep_content.get("execution_count"):
                            exec_count = rep_content["execution_count"]
                            self.execution_count = exec_count
                        if rep_content.get("status") == "error":
                            final_status = "error"
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"Unexpected kernel execution error: {e}")
                final_status = "error"
                raw_outputs.append({
                    "output_type": "error",
                    "ename": type(e).__name__,
                    "evalue": str(e),
                    "traceback": [f"{type(e).__name__}: {str(e)}"]
                })

            finally:
                self.status = "idle"

            duration_ms = round((time.perf_counter() - start_t) * 1000, 2)
            if exec_count is None:
                self.execution_count += 1
                exec_count = self.execution_count

            return {
                "status": final_status,
                "execution_count": exec_count,
                "outputs": raw_outputs,
                "duration_ms": duration_ms
            }


class RemoteKernelSession:
    """
    A kernel that lives in the notebook sandbox container (see sandbox/worker.py) instead of in this process.

    Used for users a masking policy applies to: the sandbox has no access to the warehouse files, runs each user's
    kernels under a separate OS user, and reads data only through the governed endpoint /api/sandbox/sql with a token
    that is bound to that user. Same interface as KernelSession, so the notebook endpoints do not care which they get.
    """

    def __init__(self, notebook_rel_path: str, owner: str):
        self.notebook_rel_path = notebook_rel_path
        self.owner = owner
        self.status = "stopped"
        self.execution_count = 0
        self.kid = hashlib.sha256(f"{owner}\0{notebook_rel_path}".encode()).hexdigest()[:32]
        self._alive = False

    def _call(self, method: str, suffix: str = "", body: Optional[dict] = None, timeout: float = 30.0):
        from web import sandbox_client
        return sandbox_client.call(method, f"/kernels/{self.kid}{suffix}", body, timeout=timeout)

    def is_alive(self) -> bool:
        try:
            self._alive = bool(self._call("GET", "/status", timeout=5).get("is_alive"))
        except Exception:
            self._alive = False
        return self._alive

    def start(self, timeout: int = 15):
        pass  # created on first execution by the worker

    def shutdown(self):
        try:
            self._call("DELETE")
        except Exception:
            pass
        self.status = "stopped"

    def restart(self, timeout: int = 15):
        from web import sandbox_client
        self._call("POST", "/restart", {"owner": self.owner, "notebook": self.notebook_rel_path,
                                        "token": sandbox_client.mint_kernel_token(self.owner)}, timeout=60)
        self.status = "idle"

    def execute_code(self, code: str, timeout: int = 120) -> Dict[str, Any]:
        from web import sandbox_client
        self.status = "busy"
        try:
            result = self._call("POST", "/execute", {
                "owner": self.owner, "notebook": self.notebook_rel_path, "code": code, "timeout": timeout,
                "token": sandbox_client.mint_kernel_token(self.owner)}, timeout=timeout + 45)
        except Exception as exc:
            return {"status": "error", "execution_count": self.execution_count, "duration_ms": 0, "outputs": [{
                "output_type": "error", "ename": "SandboxError", "evalue": str(exc), "traceback": [f"SandboxError: {exc}"]}]}
        finally:
            self.status = "idle"
        self.execution_count = result.get("execution_count") or self.execution_count
        return result


# Registry of active kernel sessions: { (owner, rel_path, sandboxed): session }. Kernels are per user: two users who open
# the same Shared notebook get separate kernels, so one cannot read or alter the other's variables. A user who becomes
# subject to a masking policy stops using their unrestricted local kernel: it is shut down and a sandboxed one replaces it.
SESSIONS: Dict[tuple, Any] = {}
SESSIONS_LOCK = threading.Lock()


def get_kernel_session(notebook_rel_path: str, owner: str = "anonymous", sandboxed: bool = False):
    """Retrieve or create the session of `owner` for the given notebook (in the sandbox container when `sandboxed`)."""
    norm_path = notebook_rel_path.strip().replace("\\", "/").lstrip("/")
    key = (owner, norm_path, bool(sandboxed))
    stale = None
    with SESSIONS_LOCK:
        if sandboxed:
            stale = SESSIONS.pop((owner, norm_path, False), None)
        if key not in SESSIONS:
            SESSIONS[key] = RemoteKernelSession(norm_path, owner) if sandboxed else KernelSession(norm_path, owner)
        session = SESSIONS[key]
    if stale is not None:
        try:
            stale.shutdown()
        except Exception:
            pass
    return session


def restart_notebook_kernel(notebook_rel_path: str, owner: str = "anonymous", sandboxed: bool = False) -> Dict[str, Any]:
    """Restart kernel for a specific notebook."""
    session = get_kernel_session(notebook_rel_path, owner, sandboxed)
    session.restart()
    return {"success": True, "status": session.status, "message": "Kernel restarted successfully"}


def get_kernel_status(notebook_rel_path: str, owner: str = "anonymous", sandboxed: bool = False) -> Dict[str, Any]:
    """Get the live status of the notebook's kernel."""
    session = get_kernel_session(notebook_rel_path, owner, sandboxed)
    is_alive = session.is_alive()
    return {
        "status": session.status if is_alive else "stopped",
        "is_alive": is_alive,
        "execution_count": session.execution_count,
        "sandboxed": bool(sandboxed)
    }


def _convert_raw_outputs_to_nb_outputs(raw_outputs: List[Dict[str, Any]]) -> list:
    """Converts our internal raw_outputs dict into valid nbformat output objects."""
    nb_outputs = []
    for out in raw_outputs:
        otype = out.get("output_type")
        if otype == "stream":
            nb_outputs.append(new_output(
                output_type="stream",
                name=out.get("name", "stdout"),
                text=out.get("text", "")
            ))
        elif otype == "execute_result":
            nb_outputs.append(new_output(
                output_type="execute_result",
                data=out.get("data", {}),
                metadata=out.get("metadata", {}),
                execution_count=out.get("execution_count")
            ))
        elif otype == "display_data":
            nb_outputs.append(new_output(
                output_type="display_data",
                data=out.get("data", {}),
                metadata=out.get("metadata", {})
            ))
        elif otype == "error":
            nb_outputs.append(new_output(
                output_type="error",
                ename=out.get("ename", "Error"),
                evalue=out.get("evalue", ""),
                traceback=out.get("traceback", [])
            ))
    return nb_outputs


def _parse_cell_for_ui(cell, index: int) -> Dict[str, Any]:
    """Serializes an nbformat cell into the dictionary expected by Databricks Studio UI."""
    raw_outputs = []
    for out in getattr(cell, "outputs", []):
        otype = out.get("output_type", "")
        if otype == "stream":
            raw_outputs.append({
                "type": "stream",
                "name": out.get("name", "stdout"),
                "text": out.get("text", "")
            })
        elif otype in ("execute_result", "display_data"):
            data = out.get("data", {})
            html_content = data.get("text/html")
            image_content = data.get("image/png")
            text_content = data.get("text/plain", "")
            raw_outputs.append({
                "type": otype,
                "text": text_content,
                "html": html_content,
                "image": image_content,
                "execution_count": out.get("execution_count")
            })
        elif otype == "error":
            raw_outputs.append({
                "type": "error",
                "ename": out.get("ename", "Error"),
                "evalue": out.get("evalue", ""),
                "traceback": [clean_ansi(l) for l in out.get("traceback", [])],
                "text": f"{out.get('ename', 'Error')}: {out.get('evalue', '')}"
            })

    return {
        "index": index,
        "type": cell.cell_type,
        "source": cell.source,
        "execution_count": getattr(cell, "execution_count", None),
        "outputs": raw_outputs
    }


def execute_single_cell(
    notebook_rel_path: str,
    cell_index: int,
    source_override: Optional[str] = None,
    owner: str = "anonymous",
    sandboxed: bool = False
) -> Dict[str, Any]:
    """
    Executes a single code cell (1-indexed) in the notebook's persistent kernel.
    Saves outputs and updated execution count back to the .ipynb file.
    """
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    zero_idx = cell_index - 1
    if zero_idx < 0 or zero_idx >= len(nb.cells):
        raise IndexError(f"Cell index {cell_index} out of range (1..{len(nb.cells)})")

    cell = nb.cells[zero_idx]
    if source_override is not None:
        cell.source = source_override

    # If it's a markdown cell, save edits without kernel execution
    if cell.cell_type != "code":
        with open(full_path, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        return {
            "success": True,
            "status": "ok",
            "cell": _parse_cell_for_ui(cell, cell_index),
            "duration_ms": 0
        }

    session = get_kernel_session(notebook_rel_path, owner, sandboxed)
    result = session.execute_code(cell.source)

    cell.execution_count = result["execution_count"]
    cell.outputs = _convert_raw_outputs_to_nb_outputs(result["outputs"])

    # Persist updated notebook to disk
    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    parsed_cell = _parse_cell_for_ui(cell, cell_index)

    return {
        "success": True,
        "status": result["status"],
        "execution_count": result["execution_count"],
        "duration_ms": result["duration_ms"],
        "cell": parsed_cell
    }


def execute_all_cells(notebook_rel_path: str, owner: str = "anonymous", sandboxed: bool = False) -> Dict[str, Any]:
    """
    Sequentially executes all code cells in the notebook, maintaining state in the kernel.
    Persists updated outputs and execution counts to the .ipynb file.
    """
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    session = get_kernel_session(notebook_rel_path, owner, sandboxed)

    total_start = time.perf_counter()
    executed_count = 0
    errors_encountered = 0

    for idx, cell in enumerate(nb.cells, start=1):
        if cell.cell_type != "code":
            continue

        result = session.execute_code(cell.source)
        cell.execution_count = result["execution_count"]
        cell.outputs = _convert_raw_outputs_to_nb_outputs(result["outputs"])
        executed_count += 1
        if result["status"] == "error":
            errors_encountered += 1

    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    total_ms = round((time.perf_counter() - total_start) * 1000, 2)
    parsed_cells = [_parse_cell_for_ui(c, i) for i, c in enumerate(nb.cells, start=1)]

    return {
        "success": True,
        "executed_cells": executed_count,
        "errors": errors_encountered,
        "total_duration_ms": total_ms,
        "cells": parsed_cells
    }


def save_cell_source(
    notebook_rel_path: str,
    cell_index: int,
    source: str
) -> Dict[str, Any]:
    """Updates the source code of a cell without executing it."""
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    zero_idx = cell_index - 1
    if zero_idx < 0 or zero_idx >= len(nb.cells):
        raise IndexError(f"Cell index {cell_index} out of range")

    nb.cells[zero_idx].source = source
    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return {
        "success": True,
        "cell": _parse_cell_for_ui(nb.cells[zero_idx], cell_index)
    }


def add_new_cell(
    notebook_rel_path: str,
    after_index: int = 0,
    cell_type: str = "code"
) -> Dict[str, Any]:
    """Inserts a new blank cell after after_index (1-indexed, 0 = prepend)."""
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    new_c = new_code_cell("") if cell_type == "code" else new_markdown_cell("")

    insert_idx = max(0, min(after_index, len(nb.cells)))
    nb.cells.insert(insert_idx, new_c)

    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return {
        "success": True,
        "cells": [_parse_cell_for_ui(c, i) for i, c in enumerate(nb.cells, start=1)],
        "new_cell_index": insert_idx + 1
    }


def delete_cell(notebook_rel_path: str, cell_index: int) -> Dict[str, Any]:
    """Deletes cell at cell_index (1-indexed)."""
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    zero_idx = cell_index - 1
    if zero_idx < 0 or zero_idx >= len(nb.cells):
        raise IndexError(f"Cell index {cell_index} out of range")

    del nb.cells[zero_idx]
    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return {
        "success": True,
        "cells": [_parse_cell_for_ui(c, i) for i, c in enumerate(nb.cells, start=1)]
    }


def clear_notebook_outputs(notebook_rel_path: str) -> Dict[str, Any]:
    """Clears all outputs and execution counts from the notebook."""
    full_path = get_safe_path(notebook_rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_rel_path}")

    nb = nbformat.read(full_path, as_version=4)
    for c in nb.cells:
        if c.cell_type == "code":
            c.execution_count = None
            c.outputs = []

    with open(full_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return {
        "success": True,
        "cells": [_parse_cell_for_ui(c, i) for i, c in enumerate(nb.cells, start=1)]
    }
