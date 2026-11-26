# IPython startup file for isolated local kernels (installed as 01_dkw_history.py by web/kernel_launch.py, after the Databricks
# shim). The shim logs every notebook query with `_log_notebook_query`, which used to write .metadata/history.db directly; an
# isolated kernel cannot read .metadata, so the query is appended to a spool file in the kernel's own home instead, and the studio
# moves it into the history after the cell, attributed to the kernel's owner (web/kernel_isolation.ingest_history).
import json as _dkw_json
import os as _dkw_os

_DKW_SPOOL = _dkw_os.path.join(_dkw_os.environ.get("HOME", "."), ".dkw_history.jsonl")


def _log_notebook_query(query_text, duration_ms, rows=0, status="SUCCESS", err=None):
    try:
        with open(_DKW_SPOOL, "a") as _f:
            _f.write(_dkw_json.dumps({"q": str(query_text)[:100000], "ms": round(float(duration_ms), 2), "rows": int(rows or 0),
                                      "status": status, "err": (str(err)[:4000] if err else None)}) + "\n")
    except Exception:
        pass
