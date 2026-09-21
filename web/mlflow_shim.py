import os
import sys
import time
import json
import uuid
import shutil
import datetime
import logging
import builtins
import threading
import functools
from contextlib import contextmanager
import urllib.request
import urllib.parse
from typing import Optional, Dict, Any, List, Union

logger = logging.getLogger("localspark.mlflow_shim")

_active_run_stack = []
_current_experiment_id = "0"
_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:8000")

# Autologging state
_autolog_config = {
    "enabled": False,
    "log_input_examples": True,
    "log_model_signatures": True,
    "log_models": True,
    "silent": False
}
_fit_depth = 0
_patched_classes: Dict[Any, Any] = {}
_orig_builtin_import = builtins.__import__
_importing_hook = False


def set_tracking_uri(uri: str):
    global _tracking_uri
    _tracking_uri = uri.rstrip("/")


def get_tracking_uri() -> str:
    return _tracking_uri


def _call_api(path: str, data: Optional[Dict[str, Any]] = None, method: str = "POST") -> Dict[str, Any]:
    """Helper to communicate with Localspark MLflow API or in-process web.experiments."""
    payload = data or {}
    # First attempt: in-process direct call if web.experiments is available
    try:
        from web import experiments as exp_mod
        if path == "/api/2.0/mlflow/experiments/get-by-name":
            exp = exp_mod.mlflow_get_experiment_by_name(payload.get("experiment_name", ""))
            return {"experiment": exp} if exp else {}
        elif path == "/api/2.0/mlflow/experiments/get":
            exp = exp_mod.mlflow_get_experiment(payload.get("experiment_id", ""))
            return {"experiment": exp} if exp else {}
        elif path in ("/api/2.0/mlflow/experiments/list", "/api/2.0/mlflow/experiments/search"):
            return {"experiments": exp_mod.mlflow_list_experiments(payload.get("view_type", "ACTIVE_ONLY"))}
        elif path == "/api/2.0/mlflow/experiments/create":
            return exp_mod.mlflow_create_experiment(payload.get("name", ""), payload.get("artifact_location"))
        elif path == "/api/2.0/mlflow/experiments/update":
            return exp_mod.mlflow_update_experiment(payload.get("experiment_id", ""), payload.get("new_name", ""))
        elif path == "/api/2.0/mlflow/experiments/restore":
            return exp_mod.mlflow_restore_experiment(payload.get("experiment_id", ""))
        elif path == "/api/2.0/mlflow/experiments/delete":
            exp_mod.mlflow_delete_experiment(payload.get("experiment_id", ""))
            return {}
        elif path == "/api/2.0/mlflow/experiments/set-experiment-tag":
            return exp_mod.mlflow_set_experiment_tag(payload.get("experiment_id", ""), payload.get("key", ""), payload.get("value", ""))
        elif path == "/api/2.0/mlflow/runs/create":
            r = exp_mod.mlflow_create_run(
                experiment_id=payload.get("experiment_id", "0"),
                run_name=payload.get("run_name"),
                start_time=payload.get("start_time"),
                user_id=payload.get("user_id", "martin"),
                tags=payload.get("tags")
            )
            return {"run": r}
        elif path == "/api/2.0/mlflow/runs/get":
            r = exp_mod.mlflow_get_run(payload.get("run_id", ""))
            return {"run": r} if r else {}
        elif path == "/api/2.0/mlflow/runs/update":
            r = exp_mod.mlflow_update_run(
                run_id=payload.get("run_id"),
                status=payload.get("status", "FINISHED"),
                end_time=payload.get("end_time")
            )
            return {"run_info": r.get("info", {})}
        elif path == "/api/2.0/mlflow/runs/delete":
            exp_mod.mlflow_delete_run(payload.get("run_id", ""))
            return {}
        elif path == "/api/2.0/mlflow/runs/restore":
            return exp_mod.mlflow_restore_run(payload.get("run_id", ""))
        elif path == "/api/2.0/mlflow/runs/search":
            runs = exp_mod.mlflow_search_runs(
                experiment_ids=payload.get("experiment_ids", ["0"]),
                filter_string=payload.get("filter") or payload.get("filter_string"),
                order_by=payload.get("order_by"),
                max_results=payload.get("max_results", 1000)
            )
            return {"runs": runs}
        elif path == "/api/2.0/mlflow/runs/log-parameter":
            exp_mod.mlflow_log_param(payload["run_id"], payload["key"], payload["value"])
            return {}
        elif path == "/api/2.0/mlflow/runs/log-metric":
            exp_mod.mlflow_log_metric(payload["run_id"], payload["key"], payload["value"], payload.get("timestamp"), payload.get("step", 0))
            return {}
        elif path == "/api/2.0/mlflow/runs/set-tag":
            exp_mod.mlflow_set_tag(payload["run_id"], payload["key"], payload["value"])
            return {}
        elif path == "/api/2.0/mlflow/runs/delete-tag":
            return exp_mod.mlflow_delete_tag(payload["run_id"], payload["key"])
        elif path == "/api/2.0/mlflow/runs/log-inputs":
            return exp_mod.mlflow_log_inputs(payload["run_id"], payload.get("datasets", []))
        elif path == "/api/2.0/mlflow/runs/log-batch":
            exp_mod.mlflow_log_batch(payload["run_id"], payload.get("metrics"), payload.get("params"), payload.get("tags"))
            return {}
        elif path == "/api/2.0/mlflow/artifacts/log":
            return exp_mod.mlflow_log_artifact(payload["run_id"], payload["local_file"], payload.get("artifact_path"))
        elif path == "/api/2.0/mlflow/artifacts/list":
            return {"run_id": payload["run_id"], "files": exp_mod.mlflow_list_artifacts(payload["run_id"])}
        elif path == "/api/2.0/mlflow/metrics/get-history":
            return {"metrics": exp_mod.mlflow_get_metric_history(payload["run_id"], payload["metric_key"])}
        elif path == "/api/2.0/mlflow/traces/log":
            return exp_mod.mlflow_log_trace(payload)
        elif path == "/api/2.0/mlflow/traces/search":
            return exp_mod.mlflow_search_traces(
                experiment_ids=payload.get("experiment_ids"),
                status=payload.get("status"),
                model=payload.get("model"),
                min_duration=payload.get("min_duration"),
                max_duration=payload.get("max_duration"),
                search_term=payload.get("search_term"),
                limit=payload.get("limit", 50),
                offset=payload.get("offset", 0)
            )
        elif path == "/api/2.0/mlflow/traces/get":
            return exp_mod.mlflow_get_trace(payload.get("request_id"))
        elif path == "/api/2.0/mlflow/traces/delete":
            return {"deleted": exp_mod.mlflow_delete_trace(payload.get("request_id"))}
        elif path == "/api/2.0/mlflow/traces/assessments/log":
            return exp_mod.mlflow_log_assessment(
                trace_id=payload.get("trace_id"),
                name=payload.get("name"),
                value=payload.get("value"),
                rationale=payload.get("rationale", ""),
                source_type=payload.get("source_type", "HUMAN"),
                source_id=payload.get("source_id", "admin")
            )
    except Exception as e:
        logger.debug(f"In-process experiments call failed, trying HTTP: {e}")

    # In-process serving / Unity Catalog model registry support
    try:
        from web import serving as serv_mod
        if path == "/api/2.0/mlflow/registered-models/get-model-version-by-alias":
            m = serv_mod.get_registered_model(payload.get("name", ""))
            if m:
                ver = m.get("aliases", {}).get(payload.get("alias", "").strip().lstrip("@").lower())
                if ver is not None:
                    v = serv_mod.get_model_version(payload.get("name", ""), ver)
                    return {"model_version": v} if v else {}
            return {}
        elif path in ("/api/2.0/mlflow/registered-models/alias", "/api/2.0/mlflow/registered-models/set-alias") and method == "POST":
            serv_mod.set_model_alias(payload.get("name", ""), payload.get("alias", ""), int(payload.get("version", 1)))
            return {"success": True}
        elif (path in ("/api/2.0/mlflow/registered-models/alias", "/api/2.0/mlflow/registered-models/delete-alias")) and (method == "DELETE" or method == "POST"):
            serv_mod.delete_model_alias(payload.get("name", ""), payload.get("alias", ""))
            return {"success": True}
        elif path == "/api/2.0/mlflow/registered-models/get":
            m = serv_mod.get_registered_model(payload.get("name", ""))
            return {"registered_model": m} if m else {}
        elif path in ("/api/2.0/mlflow/registered-models", "/api/2.0/mlflow/registered-models/create") and method == "POST":
            m = serv_mod.create_registered_model(
                name=payload.get("name", ""),
                catalog_name=payload.get("catalog_name", "warehouse"),
                schema_name=payload.get("schema_name", "dbo"),
                description=payload.get("description", ""),
                tags=payload.get("tags")
            )
            return {"registered_model": m}
        elif path in ("/api/2.0/mlflow/registered-models", "/api/2.0/mlflow/registered-models/list") and method == "GET":
            return {"registered_models": serv_mod.list_registered_models()}
        elif path.startswith("/api/2.0/mlflow/registered-models/") and method == "GET":
            m_name = urllib.parse.unquote(path.split("/api/2.0/mlflow/registered-models/")[1])
            m = serv_mod.get_registered_model(m_name)
            return {"registered_model": m} if m else {}
        elif path.startswith("/api/2.0/mlflow/registered-models/") and method == "DELETE":
            m_name = urllib.parse.unquote(path.split("/api/2.0/mlflow/registered-models/")[1])
            ok = serv_mod.delete_registered_model(m_name)
            return {"success": ok}
        elif path in ("/api/2.0/mlflow/model-versions", "/api/2.0/mlflow/model-versions/create") and method == "POST":
            v = serv_mod.create_model_version(
                name=payload.get("name", ""),
                run_id=payload.get("run_id"),
                stage=payload.get("stage", "None"),
                algorithm=payload.get("algorithm", "custom"),
                metrics=payload.get("metrics"),
                signature=payload.get("signature"),
                description=payload.get("description", ""),
                source=payload.get("source", "")
            )
            return {"model_version": v}
        elif path == "/api/2.0/mlflow/model-versions/get":
            v = serv_mod.get_model_version(payload.get("name", ""), int(payload.get("version", 1)))
            return {"model_version": v} if v else {}
        elif path == "/api/2.0/mlflow/model-versions/transition-stage":
            v = serv_mod.transition_model_version_stage(
                name=payload.get("name", ""),
                version=int(payload.get("version", 1)),
                stage=payload.get("stage", "None"),
                archive_existing_versions=payload.get("archive_existing_versions", True)
            )
            return {"model_version": v}
    except Exception as e:
        logger.debug(f"In-process serving call failed, trying HTTP: {e}")

    # Fallback: HTTP request to _tracking_uri
    url = f"{_tracking_uri}{path}"
    headers = {"Content-Type": "application/json"}
    if method == "GET" and payload:
        qs = urllib.parse.urlencode(payload)
        url = f"{url}?{qs}" if "?" not in url else f"{url}&{qs}"
        body_bytes = None
    else:
        body_bytes = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.debug(f"API call fallback error for {url}: {e}")
        return {}


class ModelVersion:
    """Encapsulates registered model version metadata supporting both object and dict access."""
    def __init__(self, data: Dict[str, Any]):
        self._data = data or {}

    def __getattr__(self, name: str) -> Any:
        if "_data" in self.__dict__ and name in self._data:
            return self._data[name]
        raise AttributeError(f"'ModelVersion' object has no attribute '{name}'")

    @property
    def name(self) -> str:
        return self._data.get("name", "")

    @property
    def version(self) -> int:
        return self._data.get("version", 1)

    @property
    def run_id(self) -> Optional[str]:
        return self._data.get("run_id")

    @property
    def current_stage(self) -> str:
        return self._data.get("current_stage", "None")

    @property
    def source(self) -> str:
        return self._data.get("source", "")

    @property
    def status(self) -> str:
        return self._data.get("status", "READY")

    @property
    def aliases(self) -> List[str]:
        return self._data.get("aliases", [])

    def __getitem__(self, key: str) -> Any:
        return self._data.get(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    def __repr__(self) -> str:
        return f"<ModelVersion: name='{self.name}', version={self.version}, stage='{self.current_stage}', aliases={self.aliases}>"


class RunInfo:
    """Encapsulates run metadata supporting both object and dict access."""
    def __init__(self, data: Dict[str, Any]):
        self._data = data or {}

    def __getattr__(self, name: str) -> Any:
        if "_data" in self.__dict__ and name in self._data:
            return self._data[name]
        raise AttributeError(f"'RunInfo' object has no attribute '{name}'")

    @property
    def run_id(self) -> str:
        return self._data.get("run_id", "")

    @property
    def experiment_id(self) -> str:
        return self._data.get("experiment_id", "0")

    @property
    def run_name(self) -> str:
        return self._data.get("run_name", "")

    @property
    def status(self) -> str:
        return self._data.get("status", "RUNNING")

    def __getitem__(self, key: str) -> Any:
        return self._data.get(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class ActiveRun:
    def __init__(self, run_data: Dict[str, Any]):
        self.data = run_data
        raw_info = run_data.get("info", {})
        if isinstance(raw_info, dict):
            self.info = RunInfo(raw_info)
        else:
            self.info = raw_info

    @property
    def run_id(self) -> str:
        return self.info.run_id if hasattr(self.info, "run_id") else self.info.get("run_id", "")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "FAILED" if exc_type else "FINISHED"
        end_run(status=status)
        return False


def set_experiment(experiment_name: str) -> str:
    """Sets or creates an experiment by name, returning experiment_id."""
    global _current_experiment_id
    experiment_name = experiment_name.strip()
    res = _call_api("/api/2.0/mlflow/experiments/get-by-name", {"experiment_name": experiment_name}, method="POST")
    exp = res.get("experiment")
    if exp and exp.get("experiment_id"):
        _current_experiment_id = exp["experiment_id"]
    else:
        create_res = _call_api("/api/2.0/mlflow/experiments/create", {"name": experiment_name})
        _current_experiment_id = create_res.get("experiment_id", "0")
    return _current_experiment_id


def create_experiment(name: str, artifact_location: Optional[str] = None, tags: Optional[Dict[str, Any]] = None) -> str:
    """Creates an experiment and optionally associates tags with it."""
    res = _call_api("/api/2.0/mlflow/experiments/create", {"name": name, "artifact_location": artifact_location})
    exp_id = res.get("experiment_id", "0")
    if tags:
        for k, v in tags.items():
            set_experiment_tag(k, v, experiment_id=exp_id)
    return exp_id


def get_experiment(experiment_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves an experiment metadata dictionary by ID."""
    res = _call_api("/api/2.0/mlflow/experiments/get", {"experiment_id": str(experiment_id)}, method="GET")
    return res.get("experiment")


def get_experiment_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Retrieves an experiment metadata dictionary by name."""
    res = _call_api("/api/2.0/mlflow/experiments/get-by-name", {"experiment_name": name}, method="POST")
    return res.get("experiment")


def delete_experiment(experiment_id: str):
    """Marks an experiment and its runs as deleted."""
    _call_api("/api/2.0/mlflow/experiments/delete", {"experiment_id": str(experiment_id)})


def restore_experiment(experiment_id: str):
    """Restores a soft-deleted experiment."""
    _call_api("/api/2.0/mlflow/experiments/restore", {"experiment_id": str(experiment_id)})


def set_experiment_tag(key: str, value: Any, experiment_id: Optional[str] = None):
    """Sets a metadata tag on an experiment."""
    target_exp = str(experiment_id or _current_experiment_id)
    _call_api("/api/2.0/mlflow/experiments/set-experiment-tag", {
        "experiment_id": target_exp,
        "key": str(key),
        "value": str(value)
    })


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves a full run object dictionary by run ID."""
    res = _call_api("/api/2.0/mlflow/runs/get", {"run_id": str(run_id)}, method="GET")
    return res.get("run")


def delete_run(run_id: str):
    """Marks a run as deleted."""
    _call_api("/api/2.0/mlflow/runs/delete", {"run_id": str(run_id)})


def restore_run(run_id: str):
    """Restores a soft-deleted run."""
    _call_api("/api/2.0/mlflow/runs/restore", {"run_id": str(run_id)})


def start_run(
    run_id: Optional[str] = None,
    experiment_id: Optional[str] = None,
    run_name: Optional[str] = None,
    nested: bool = False,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = None
) -> ActiveRun:
    """Starts a new tracking run and returns an ActiveRun context manager."""
    global _active_run_stack
    target_exp = experiment_id or _current_experiment_id
    tags_list = [{"key": k, "value": str(v)} for k, v in (tags or {}).items()]
    if description:
        tags_list.append({"key": "mlflow.note.content", "value": description})

    req_data = {
        "experiment_id": target_exp,
        "run_name": run_name,
        "start_time": int(time.time() * 1000),
        "tags": tags_list
    }
    resp = _call_api("/api/2.0/mlflow/runs/create", req_data)
    run_data = resp.get("run", {"info": {"run_id": f"run_{int(time.time()*1000)}"}})
    run_obj = ActiveRun(run_data)
    _active_run_stack.append(run_obj)
    return run_obj


def end_run(status: str = "FINISHED"):
    """Ends the currently active run."""
    global _active_run_stack
    if not _active_run_stack:
        return
    run_obj = _active_run_stack.pop()
    rid = run_obj.run_id
    _call_api("/api/2.0/mlflow/runs/update", {
        "run_id": rid,
        "status": status.upper(),
        "end_time": int(time.time() * 1000)
    })
    try:
        from web.delta_lineage import sync_ml_models_to_lineage_graph
        sync_ml_models_to_lineage_graph()
    except Exception:
        pass


def active_run() -> Optional[ActiveRun]:
    return _active_run_stack[-1] if _active_run_stack else None


def log_param(key: str, value: Any):
    cur = active_run()
    if not cur:
        start_run()
        cur = active_run()
    _call_api("/api/2.0/mlflow/runs/log-parameter", {
        "run_id": cur.run_id,
        "key": str(key),
        "value": str(value)
    })


def log_params(params: Dict[str, Any]):
    for k, v in (params or {}).items():
        log_param(k, v)


def log_metric(key: str, value: float, step: Optional[int] = None):
    cur = active_run()
    if not cur:
        start_run()
        cur = active_run()
    _call_api("/api/2.0/mlflow/runs/log-metric", {
        "run_id": cur.run_id,
        "key": str(key),
        "value": float(value),
        "step": int(step or 0),
        "timestamp": int(time.time() * 1000)
    })


def log_metrics(metrics: Dict[str, float], step: Optional[int] = None):
    for k, v in (metrics or {}).items():
        log_metric(k, v, step=step)


def set_tag(key: str, value: Any):
    cur = active_run()
    if not cur:
        start_run()
        cur = active_run()
    _call_api("/api/2.0/mlflow/runs/set-tag", {
        "run_id": cur.run_id,
        "key": str(key),
        "value": str(value)
    })


def set_tags(tags: Dict[str, Any]):
    for k, v in (tags or {}).items():
        set_tag(k, v)


def delete_tag(key: str):
    """Deletes a tag from the current active run."""
    cur = active_run()
    if cur:
        _call_api("/api/2.0/mlflow/runs/delete-tag", {
            "run_id": cur.run_id,
            "key": str(key)
        })


# =========================================================================
# Artifact & Model Logging
# =========================================================================

def log_artifact(local_path: str, artifact_path: Optional[str] = None):
    """Logs a local file or directory as an artifact of the current run."""
    cur = active_run()
    if not cur:
        start_run()
        cur = active_run()
    _call_api("/api/2.0/mlflow/artifacts/log", {
        "run_id": cur.run_id,
        "local_file": os.path.abspath(local_path),
        "artifact_path": artifact_path
    })


def log_artifacts(local_dir: str, artifact_path: Optional[str] = None):
    """Logs all contents of a local directory as run artifacts."""
    log_artifact(local_dir, artifact_path)


def log_dict(dictionary: Dict[str, Any], artifact_file: str):
    """Writes a dictionary as a JSON file artifact."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(dictionary, f, indent=2)
        tmp_name = f.name
    try:
        dirname = os.path.dirname(artifact_file) or None
        basename = os.path.basename(artifact_file)
        dest_tmp = os.path.join(os.path.dirname(tmp_name), basename)
        shutil.move(tmp_name, dest_tmp)
        log_artifact(dest_tmp, artifact_path=dirname)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def log_text(text: str, artifact_file: str):
    """Writes plain text as a file artifact."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write(text)
        tmp_name = f.name
    try:
        dirname = os.path.dirname(artifact_file) or None
        basename = os.path.basename(artifact_file)
        dest_tmp = os.path.join(os.path.dirname(tmp_name), basename)
        shutil.move(tmp_name, dest_tmp)
        log_artifact(dest_tmp, artifact_path=dirname)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def log_figure(figure: Any, artifact_file: str):
    """Saves and logs a matplotlib/seaborn figure as an image artifact."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".png", delete=False) as f:
        tmp_name = f.name
    try:
        if hasattr(figure, "savefig"):
            figure.savefig(tmp_name, bbox_inches="tight")
        dirname = os.path.dirname(artifact_file) or None
        basename = os.path.basename(artifact_file)
        dest_tmp = os.path.join(os.path.dirname(tmp_name), basename)
        shutil.move(tmp_name, dest_tmp)
        log_artifact(dest_tmp, artifact_path=dirname)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def log_image(image: Any, artifact_file: str):
    """
    Logs an image as an artifact of the current run.
    Accepts PIL Image, numpy ndarray, or existing image file path.
    """
    import tempfile
    ext = os.path.splitext(artifact_file)[1].lstrip(".").lower() or "png"
    with tempfile.NamedTemporaryFile("wb", suffix=f".{ext}", delete=False) as f:
        tmp_name = f.name
    try:
        if isinstance(image, str) and os.path.exists(image):
            shutil.copy2(image, tmp_name)
        elif hasattr(image, "save"):
            # PIL Image
            image.save(tmp_name)
        elif hasattr(image, "ndim") or hasattr(image, "shape"):
            # Numpy array
            try:
                from PIL import Image
                img_obj = Image.fromarray(image)
                img_obj.save(tmp_name)
            except Exception:
                import matplotlib.pyplot as plt
                plt.imsave(tmp_name, image)
        else:
            raise ValueError(f"Unsupported image type: {type(image)}")

        dirname = os.path.dirname(artifact_file) or None
        basename = os.path.basename(artifact_file)
        dest_tmp = os.path.join(os.path.dirname(tmp_name), basename)
        shutil.move(tmp_name, dest_tmp)
        log_artifact(dest_tmp, artifact_path=dirname)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def search_runs(
    experiment_ids: Optional[Union[str, List[str]]] = None,
    experiment_names: Optional[List[str]] = None,
    filter_string: str = "",
    run_view_type: str = "ACTIVE_ONLY",
    max_results: int = 1000,
    order_by: Optional[List[str]] = None,
    output_format: str = "pandas"
) -> Any:
    """Searches runs and returns a pandas DataFrame by default or list of dictionaries."""
    target_exp_ids = []
    if experiment_ids:
        if isinstance(experiment_ids, (list, tuple)):
            target_exp_ids.extend([str(e) for e in experiment_ids])
        else:
            target_exp_ids.append(str(experiment_ids))
    elif experiment_names:
        for en in experiment_names:
            e = get_experiment_by_name(en)
            if e and e.get("experiment_id"):
                target_exp_ids.append(e["experiment_id"])
    else:
        target_exp_ids.append(_current_experiment_id)

    res = _call_api("/api/2.0/mlflow/runs/search", {
        "experiment_ids": target_exp_ids,
        "filter": filter_string,
        "order_by": order_by,
        "max_results": max_results
    })
    raw_runs = res.get("runs", [])

    if output_format == "list":
        return raw_runs

    try:
        import pandas as pd
        rows = []
        for r in raw_runs:
            info = r.get("info", {})
            metrics = r.get("metrics_dict", {})
            params = r.get("params_dict", {})
            tags = r.get("tags_dict", {})

            row = {
                "run_id": info.get("run_id"),
                "experiment_id": info.get("experiment_id"),
                "status": info.get("status"),
                "artifact_uri": info.get("artifact_uri"),
                "start_time": info.get("start_time"),
                "end_time": info.get("end_time"),
            }
            for mk, mv in metrics.items():
                row[f"metrics.{mk}"] = mv
            for pk, pv in params.items():
                row[f"params.{pk}"] = pv
            for tk, tv in tags.items():
                row[f"tags.{tk}"] = tv
            rows.append(row)
        return pd.DataFrame(rows)
    except Exception:
        return raw_runs


# =========================================================================
# Dataset Tracking & Lineage (mlflow.data)
# =========================================================================

class Dataset:
    """Encapsulates dataset metadata, digest, schema, and profiles."""
    def __init__(self, name: str, digest: str, source_type: str, source: str, schema: Any, profile: Any):
        self.name = name
        self.digest = digest
        self.source_type = source_type
        self.source = source
        self.schema = schema
        self.profile = profile

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "digest": self.digest,
            "source_type": self.source_type,
            "source": self.source,
            "schema": self.schema,
            "profile": self.profile
        }


class _DataModule:
    Dataset = Dataset

    @staticmethod
    def from_pandas(df: Any, source: Optional[str] = None, name: str = "dataset", targets: Optional[str] = None) -> Dataset:
        import hashlib
        num_rows = len(df) if hasattr(df, "__len__") else 0
        num_cols = len(df.columns) if hasattr(df, "columns") else 0

        col_names = [str(c) for c in getattr(df, "columns", [])]
        sig = f"{num_rows}_{num_cols}_{col_names}"
        digest = hashlib.md5(sig.encode("utf-8")).hexdigest()[:16]

        schema_dict = {}
        if hasattr(df, "dtypes"):
            schema_dict = {"columns": [{"name": str(c), "type": str(t)} for c, t in df.dtypes.items()]}

        profile_dict = {"num_rows": num_rows, "num_columns": num_cols}
        if targets:
            profile_dict["targets"] = targets

        src_val = source or getattr(df, "_source_table", getattr(df, "_source_query", "in-memory-pandas"))

        # Check if src_val points to an existing Delta table
        source_type = "dataframe"
        if src_val and src_val != "in-memory-pandas":
            try:
                from web.delta_lineage import get_delta_table_info
                dinfo = get_delta_table_info(str(src_val))
                if dinfo:
                    source_type = "delta"
                    digest = dinfo.get("table_id") or f"v{dinfo['version']}"
                    src_val = f"delta://{dinfo['table_name']}@v{dinfo['version']}"
                    profile_dict["delta_version"] = dinfo["version"]
                    profile_dict["commit_timestamp"] = dinfo["timestamp"]
                    profile_dict["delta_commit_time"] = dinfo["commit_time_str"]
                    profile_dict["delta_operation"] = dinfo["operation"]
                    profile_dict["delta_table_name"] = dinfo["table_name"]
                elif "." in str(src_val):
                    source_type = "table"
            except Exception:
                if "." in str(src_val):
                    source_type = "table"

        return Dataset(
            name=name,
            digest=digest,
            source_type=source_type,
            source=str(src_val),
            schema=schema_dict,
            profile=profile_dict
        )

    @staticmethod
    def from_delta(table_name_or_path: str, name: Optional[str] = None, targets: Optional[str] = None) -> Dataset:
        """Constructs an MLflow dataset directly from a Delta Lake table with commit versioning."""
        try:
            from web.delta_lineage import get_delta_table_info
            info = get_delta_table_info(table_name_or_path)
        except Exception:
            info = None

        if not info:
            return Dataset(
                name=name or table_name_or_path,
                digest="unknown",
                source_type="delta",
                source=f"delta://{table_name_or_path}",
                schema={},
                profile={"targets": targets} if targets else {}
            )

        schema_dict = {"columns": [{"name": str(f.get("name")), "type": str(f.get("type"))} for f in info.get("schema_fields", [])]}
        profile_dict = {
            "delta_version": info["version"],
            "commit_timestamp": info["timestamp"],
            "delta_commit_time": info["commit_time_str"],
            "operation": info["operation"],
            "num_columns": len(info.get("column_names", []))
        }
        if targets:
            profile_dict["targets"] = targets

        return Dataset(
            name=name or info["table_name"],
            digest=info.get("table_id") or f"v{info['version']}",
            source_type="delta",
            source=f"delta://{info['table_name']}@v{info['version']}",
            schema=schema_dict,
            profile=profile_dict
        )


data = _DataModule()


def log_input(dataset: Any, context: str = "training", tags: Optional[Dict[str, str]] = None):
    """Logs a dataset and its metadata to the active run, recording Delta Lake linkage."""
    cur = active_run()
    if not cur:
        start_run()
        cur = active_run()

    ds_obj = dataset.to_dict() if hasattr(dataset, "to_dict") else dataset
    tags_list = [{"key": k, "value": str(v)} for k, v in (tags or {}).items()]
    if context:
        tags_list.append({"key": "context", "value": context})

    # Delta Lake Lineage Auto-Resolution
    try:
        from web.delta_lineage import record_run_delta_lineage, sync_ml_models_to_lineage_graph
        src = ds_obj.get("source", "")
        tbl_name = ds_obj.get("name", "")
        candidate = None
        if src.startswith("delta://"):
            candidate = src[len("delta://"):].split("@")[0]
        elif "/" in src or "." in src:
            candidate = src
        elif tbl_name:
            candidate = tbl_name

        if candidate:
            dinfo = record_run_delta_lineage(cur.run_id, candidate, context=context)
            if dinfo:
                tags_list.append({"key": "delta_version", "value": str(dinfo["version"])})
                tags_list.append({"key": "delta_table", "value": dinfo["table_name"]})
            sync_ml_models_to_lineage_graph()
    except Exception as e:
        logger.debug(f"Delta input lineage hook error: {e}")

    _call_api("/api/2.0/mlflow/runs/log-inputs", {
        "run_id": cur.run_id,
        "datasets": [{
            "dataset": ds_obj,
            "tags": tags_list
        }]
    })


# =========================================================================
# OOP Client (MlflowClient)
# =========================================================================

class MlflowClient:
    """Client for tracking experiments, runs, artifacts, and models with MLflow parity."""

    def __init__(self, tracking_uri: Optional[str] = None):
        self.tracking_uri = tracking_uri or get_tracking_uri()

    # Experiment methods
    def create_experiment(self, name: str, artifact_location: Optional[str] = None, tags: Optional[Dict[str, Any]] = None) -> str:
        return create_experiment(name, artifact_location=artifact_location, tags=tags)

    def get_experiment(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        return get_experiment(experiment_id)

    def get_experiment_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        return get_experiment_by_name(name)

    def list_experiments(self, view_type: str = "ACTIVE_ONLY") -> List[Dict[str, Any]]:
        res = _call_api("/api/2.0/mlflow/experiments/list", {"view_type": view_type}, method="GET")
        return res.get("experiments", [])

    def search_experiments(self, view_type: str = "ACTIVE_ONLY") -> List[Dict[str, Any]]:
        res = _call_api("/api/2.0/mlflow/experiments/search", {"view_type": view_type}, method="POST")
        return res.get("experiments", [])

    def update_experiment(self, experiment_id: str, new_name: str):
        _call_api("/api/2.0/mlflow/experiments/update", {"experiment_id": str(experiment_id), "new_name": new_name})

    def delete_experiment(self, experiment_id: str):
        delete_experiment(experiment_id)

    def restore_experiment(self, experiment_id: str):
        restore_experiment(experiment_id)

    def set_experiment_tag(self, experiment_id: str, key: str, value: Any):
        _call_api("/api/2.0/mlflow/experiments/set-experiment-tag", {
            "experiment_id": str(experiment_id),
            "key": str(key),
            "value": str(value)
        })

    # Run methods
    def create_run(self, experiment_id: str, start_time: Optional[int] = None, tags: Optional[Dict[str, Any]] = None, run_name: Optional[str] = None) -> ActiveRun:
        return start_run(experiment_id=experiment_id, run_name=run_name, tags=tags)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return get_run(run_id)

    def update_run(self, run_id: str, status: str = "FINISHED", end_time: Optional[int] = None) -> Dict[str, Any]:
        return _call_api("/api/2.0/mlflow/runs/update", {
            "run_id": run_id,
            "status": status.upper(),
            "end_time": end_time or int(time.time() * 1000)
        })

    def delete_run(self, run_id: str):
        delete_run(run_id)

    def restore_run(self, run_id: str):
        restore_run(run_id)

    def search_runs(self, experiment_ids: List[str], filter_string: str = "", run_view_type: str = "ACTIVE_ONLY", max_results: int = 1000, order_by: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        res = _call_api("/api/2.0/mlflow/runs/search", {
            "experiment_ids": [str(e) for e in experiment_ids],
            "filter": filter_string,
            "order_by": order_by,
            "max_results": max_results
        })
        return res.get("runs", [])

    # Logging primitives
    def log_param(self, run_id: str, key: str, value: Any):
        _call_api("/api/2.0/mlflow/runs/log-parameter", {"run_id": run_id, "key": str(key), "value": str(value)})

    def log_metric(self, run_id: str, key: str, value: float, step: Optional[int] = None, timestamp: Optional[int] = None):
        _call_api("/api/2.0/mlflow/runs/log-metric", {
            "run_id": run_id,
            "key": str(key),
            "value": float(value),
            "step": int(step or 0),
            "timestamp": timestamp or int(time.time() * 1000)
        })

    def set_tag(self, run_id: str, key: str, value: Any):
        _call_api("/api/2.0/mlflow/runs/set-tag", {"run_id": run_id, "key": str(key), "value": str(value)})

    def delete_tag(self, run_id: str, key: str):
        _call_api("/api/2.0/mlflow/runs/delete-tag", {"run_id": run_id, "key": str(key)})

    def log_batch(self, run_id: str, metrics: Optional[List[Dict[str, Any]]] = None, params: Optional[List[Dict[str, Any]]] = None, tags: Optional[List[Dict[str, Any]]] = None):
        _call_api("/api/2.0/mlflow/runs/log-batch", {"run_id": run_id, "metrics": metrics, "params": params, "tags": tags})

    def log_inputs(self, run_id: str, datasets: List[Dict[str, Any]]):
        _call_api("/api/2.0/mlflow/runs/log-inputs", {"run_id": run_id, "datasets": datasets})

    def get_metric_history(self, run_id: str, metric_key: str) -> List[Dict[str, Any]]:
        res = _call_api("/api/2.0/mlflow/metrics/get-history", {"run_id": run_id, "metric_key": metric_key}, method="GET")
        return res.get("metrics", [])

    # Artifact methods
    def log_artifact(self, run_id: str, local_path: str, artifact_path: Optional[str] = None):
        _call_api("/api/2.0/mlflow/artifacts/log", {"run_id": run_id, "local_file": os.path.abspath(local_path), "artifact_path": artifact_path})

    def list_artifacts(self, run_id: str, path: Optional[str] = None) -> List[Dict[str, Any]]:
        res = _call_api("/api/2.0/mlflow/artifacts/list", {"run_id": run_id}, method="GET")
        files = res.get("files", [])
        if path:
            p_prefix = path.strip("/") + "/"
            return [f for f in files if f.get("path", "").startswith(p_prefix) or f.get("path") == path]
        return files

    def download_artifacts(self, run_id: str, path: str, dst_path: Optional[str] = None) -> str:
        from web import experiments as exp_mod
        src = exp_mod.mlflow_get_artifact_path(run_id, path)
        if not src or not os.path.exists(src):
            try:
                res = requests.get(f"{_base_url()}/api/2.0/mlflow/artifacts/get", params={"run_id": run_id, "path": path}, timeout=10)
                if res.status_code == 200:
                    import tempfile
                    target_dir = dst_path or tempfile.mkdtemp(prefix="mlflow_artifacts_")
                    os.makedirs(target_dir, exist_ok=True)
                    dest_file = os.path.join(target_dir, os.path.basename(path))
                    with open(dest_file, "wb") as f:
                        f.write(res.content)
                    return dest_file
            except Exception:
                pass
            raise FileNotFoundError(f"Artifact '{path}' for run '{run_id}' not found")
        if not dst_path:
            return src
        os.makedirs(dst_path, exist_ok=True)
        dest = os.path.join(dst_path, os.path.basename(src))
        if os.path.isdir(src):
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        return dest

    # Model Registry shortcuts
    def create_registered_model(
        self,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        return _call_api("/api/2.0/mlflow/registered-models", {
            "name": name,
            "tags": tags,
            "description": description,
            "catalog_name": catalog_name or "warehouse",
            "schema_name": schema_name or "dbo"
        })

    def get_registered_model(self, name: str) -> Dict[str, Any]:
        res = _call_api(f"/api/2.0/mlflow/registered-models/{urllib.parse.quote(name, safe='')}", method="GET")
        if not res or not res.get("registered_model"):
            res = _call_api("/api/2.0/mlflow/registered-models/get", {"name": name}, method="GET")
        return res.get("registered_model", {})

    def create_model_version(
        self,
        name: str,
        source: str,
        run_id: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> ModelVersion:
        res = _call_api("/api/2.0/mlflow/model-versions", {
            "name": name,
            "source": source,
            "run_id": run_id,
            "description": description,
            "tags": tags
        })
        return ModelVersion(res.get("model_version", {}))

    def get_model_version(self, name: str, version: Union[int, str]) -> ModelVersion:
        res = _call_api("/api/2.0/mlflow/model-versions/get", {"name": name, "version": int(version)}, method="GET")
        return ModelVersion(res.get("model_version", {}))

    def get_model_version_by_alias(self, name: str, alias: str) -> ModelVersion:
        res = _call_api("/api/2.0/mlflow/registered-models/get-model-version-by-alias", {"name": name, "alias": alias}, method="GET")
        return ModelVersion(res.get("model_version", {}))

    def transition_model_version_stage(
        self,
        name: str,
        version: Union[int, str],
        stage: str,
        archive_existing_versions: bool = False
    ) -> ModelVersion:
        res = _call_api("/api/2.0/mlflow/model-versions/transition-stage", {
            "name": name,
            "version": str(version),
            "stage": stage,
            "archive_existing_versions": archive_existing_versions
        })
        return ModelVersion(res.get("model_version", {}))

    def set_registered_model_alias(self, name: str, alias: str, version: Union[int, str]) -> Dict[str, Any]:
        return _call_api("/api/2.0/mlflow/registered-models/alias", {"name": name, "alias": alias, "version": str(version)})

    def delete_registered_model_alias(self, name: str, alias: str) -> Dict[str, Any]:
        return _call_api("/api/2.0/mlflow/registered-models/alias", {"name": name, "alias": alias}, method="DELETE")

    def delete_registered_model(self, name: str) -> Dict[str, Any]:
        return _call_api(f"/api/2.0/mlflow/registered-models/{urllib.parse.quote(name, safe='')}", method="DELETE")

    def delete_model_version(self, name: str, version: Union[int, str]) -> Dict[str, Any]:
        return _call_api(f"/api/2.0/mlflow/model-versions/{urllib.parse.quote(name, safe='')}/{version}", method="DELETE")


def register_model(
    model_uri: str,
    name: str,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = None
) -> ModelVersion:
    """
    Registers a new model version for the given model_uri in Unity Catalog 3-level namespace.
    Supports formats:
    - 'catalog.schema.model_name' (e.g. 'marketing.analytics.customer_churn')
    - 'model_name' (defaults to 'warehouse.dbo.model_name')
    """
    client = MlflowClient()
    run_id = None
    source = model_uri

    if model_uri.startswith("runs:/"):
        parts = model_uri[len("runs:/"):].split("/", 1)
        run_id = parts[0]

    # Ensure registered model exists or create it
    try:
        reg = client.get_registered_model(name)
        if not reg:
            client.create_registered_model(name=name, tags=tags, description=description)
    except Exception:
        pass

    mv = client.create_model_version(
        name=name,
        source=source,
        run_id=run_id,
        description=description,
        tags=tags
    )

    # Sync to global lineage DAG
    try:
        from web.delta_lineage import sync_ml_models_to_lineage_graph
        sync_ml_models_to_lineage_graph()
    except Exception as e:
        logger.debug(f"Error syncing model to lineage graph: {e}")

    return mv


class _TrackingModule:
    MlflowClient = MlflowClient

tracking = _TrackingModule()

def _extract_clean_params(estimator) -> Dict[str, str]:
    if not hasattr(estimator, "get_params"):
        return {}
    try:
        raw = estimator.get_params(deep=False)
    except Exception:
        return {}
    cleaned = {}
    for k, v in raw.items():
        if isinstance(v, (int, float, bool, str)) or v is None:
            cleaned[k] = str(v)
        elif isinstance(v, (list, tuple)) and len(v) <= 10:
            cleaned[k] = str(v)
    return cleaned


def _log_dataset_lineage(estimator, X, y=None):
    try:
        # Dimensionality
        n_samples = None
        n_features = None
        if hasattr(X, "shape"):
            shape = X.shape
            if len(shape) > 0:
                n_samples = shape[0]
            if len(shape) > 1:
                n_features = shape[1]
        elif hasattr(X, "__len__"):
            n_samples = len(X)

        if n_samples is not None:
            set_tag("data_num_samples", str(n_samples))
        if n_features is not None:
            set_tag("data_num_features", str(n_features))

        # Features
        feature_names = []
        if hasattr(X, "columns"):
            feature_names = [str(c) for c in X.columns]
        elif hasattr(estimator, "feature_names_in_"):
            feature_names = [str(c) for c in estimator.feature_names_in_]

        if feature_names:
            set_tag("data_features", ", ".join(feature_names[:15]) + ("..." if len(feature_names) > 15 else ""))

        # Lineage source (if Pandas DataFrame has _source_table or _source_query or _delta_table)
        source_candidate = getattr(X, "_source_table", getattr(X, "_delta_table", None))
        cur = active_run()
        if source_candidate:
            set_tag("mlflow.data.source", str(source_candidate))
            set_tag("spark.data.source", str(source_candidate))
            if cur:
                try:
                    from web.delta_lineage import record_run_delta_lineage, sync_ml_models_to_lineage_graph
                    record_run_delta_lineage(cur.run_id, str(source_candidate))
                    sync_ml_models_to_lineage_graph()
                except Exception:
                    pass
        elif hasattr(X, "_source_query"):
            set_tag("mlflow.data.query", str(X._source_query))
        elif cur:
            try:
                from web.experiments import get_exp_db
                with get_exp_db() as conn:
                    row = conn.execute("SELECT value FROM run_tags WHERE run_id = ? AND key IN ('mlflow.data.source', 'spark.data.source', 'delta.table_name') LIMIT 1", (cur.run_id,)).fetchone()
                    if row and row["value"]:
                        from web.delta_lineage import record_run_delta_lineage, sync_ml_models_to_lineage_graph
                        record_run_delta_lineage(cur.run_id, row["value"])
                        sync_ml_models_to_lineage_graph()
            except Exception:
                pass

        # Target metadata
        if y is not None:
            if hasattr(y, "name") and y.name:
                set_tag("target_column", str(y.name))
            if hasattr(y, "shape") and len(y.shape) > 0:
                set_tag("target_samples", str(y.shape[0]))
    except Exception:
        pass


def _evaluate_and_log_metrics(estimator, X, y=None):
    if y is None:
        return
    try:
        import numpy as np
        y_true = np.asarray(y)
    except Exception:
        y_true = y

    try:
        from sklearn.base import is_classifier, is_regressor
        is_clf = is_classifier(estimator)
        is_reg = is_regressor(estimator)
    except Exception:
        is_clf = getattr(estimator, "_estimator_type", None) == "classifier" or hasattr(estimator, "classes_")
        is_reg = getattr(estimator, "_estimator_type", None) == "regressor"

    is_classifier = is_clf or hasattr(estimator, "classes_")
    is_regressor = is_reg or (not is_classifier and hasattr(estimator, "predict"))

    try:
        y_pred = estimator.predict(X)
    except Exception:
        return

    computed_metrics = {}

    if is_classifier:
        try:
            from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
            computed_metrics["training_accuracy_score"] = float(accuracy_score(y_true, y_pred))
            computed_metrics["training_f1_score"] = float(f1_score(y_true, y_pred, average="weighted", zero_division=0))
            computed_metrics["training_precision_score"] = float(precision_score(y_true, y_pred, average="weighted", zero_division=0))
            computed_metrics["training_recall_score"] = float(recall_score(y_true, y_pred, average="weighted", zero_division=0))
        except Exception:
            pass

        if hasattr(estimator, "predict_proba"):
            try:
                from sklearn.metrics import roc_auc_score, log_loss
                prob = estimator.predict_proba(X)
                if prob.ndim == 2 and prob.shape[1] == 2:
                    computed_metrics["training_roc_auc"] = float(roc_auc_score(y_true, prob[:, 1]))
                    computed_metrics["training_log_loss"] = float(log_loss(y_true, prob))
            except Exception:
                pass

        try:
            from sklearn.metrics import confusion_matrix
            cm = confusion_matrix(y_true, y_pred).tolist()
            classes = [str(c) for c in getattr(estimator, "classes_", [])]
            log_dict({"confusion_matrix": cm, "classes": classes}, "confusion_matrix.json")
        except Exception:
            pass

    elif is_regressor:
        try:
            import math
            from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
            mse = float(mean_squared_error(y_true, y_pred))
            computed_metrics["training_r2_score"] = float(r2_score(y_true, y_pred))
            computed_metrics["training_mean_squared_error"] = mse
            computed_metrics["training_root_mean_squared_error"] = math.sqrt(mse)
            computed_metrics["training_mean_absolute_error"] = float(mean_absolute_error(y_true, y_pred))
        except Exception:
            pass

    if hasattr(estimator, "inertia_"):
        try:
            computed_metrics["training_inertia"] = float(estimator.inertia_)
        except Exception:
            pass

    if computed_metrics:
        log_metrics(computed_metrics)


def _log_feature_importances(estimator, X=None):
    feature_names = []
    if hasattr(X, "columns"):
        feature_names = [str(c) for c in X.columns]
    elif hasattr(estimator, "feature_names_in_"):
        feature_names = [str(c) for c in estimator.feature_names_in_]

    if hasattr(estimator, "feature_importances_"):
        try:
            raw_imp = estimator.feature_importances_
            if not feature_names or len(feature_names) != len(raw_imp):
                feature_names = [f"feature_{i}" for i in range(len(raw_imp))]
            imp_dict = {fn: float(v) for fn, v in zip(feature_names, raw_imp)}
            sorted_imp = dict(sorted(imp_dict.items(), key=lambda x: x[1], reverse=True))
            log_dict({"feature_importances": sorted_imp}, "feature_importance.json")

            for idx, (fn, fval) in enumerate(list(sorted_imp.items())[:3]):
                set_tag(f"top_feature_{idx+1}", f"{fn} ({round(fval, 4)})")
        except Exception:
            pass
    elif hasattr(estimator, "coef_"):
        try:
            import numpy as np
            coef = np.asarray(estimator.coef_).tolist()
            log_dict({"coefficients": coef}, "coefficients.json")
        except Exception:
            pass


def _log_sklearn_model_artifact(estimator, X=None, artifact_path: str = "model", registered_model_name: Optional[str] = None):
    try:
        import tempfile
        import joblib
        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = os.path.join(tmpdir, artifact_path)
            os.makedirs(model_dir, exist_ok=True)
            model_file = os.path.join(model_dir, "model.pkl")
            joblib.dump(estimator, model_file)

            feature_names = []
            if hasattr(X, "columns"):
                feature_names = [str(c) for c in X.columns]
            elif hasattr(estimator, "feature_names_in_"):
                feature_names = [str(c) for c in estimator.feature_names_in_]

            mlmodel_meta = {
                "artifact_path": artifact_path,
                "flavors": {
                    "python_function": {
                        "model_path": "model.pkl",
                        "loader_module": "mlflow.sklearn",
                        "python_version": sys.version.split()[0]
                    },
                    "sklearn": {
                        "pickled_model": "model.pkl",
                        "sklearn_version": getattr(sys.modules.get("sklearn"), "__version__", "unknown"),
                        "serialization_format": "joblib"
                    }
                },
                "run_id": active_run().run_id if active_run() else "",
                "signature": {
                    "inputs": [{"name": fn, "type": "double"} for fn in feature_names],
                    "outputs": [{"type": "tensor", "dtype": "int64" if getattr(estimator, "_estimator_type", "") == "classifier" else "float64"}]
                }
            }
            meta_path = os.path.join(model_dir, "MLmodel")
            with open(meta_path, "w") as f:
                json.dump(mlmodel_meta, f, indent=2)

            conda_path = os.path.join(model_dir, "conda.yaml")
            conda_spec = {
                "name": "mlflow-env",
                "channels": ["conda-forge"],
                "dependencies": [
                    f"python={sys.version.split()[0]}",
                    "pip",
                    {
                        "pip": [
                            "mlflow>=2.14.0",
                            f"scikit-learn=={getattr(sys.modules.get('sklearn'), '__version__', '1.5.0')}",
                            "joblib"
                        ]
                    }
                ]
            }
            try:
                import yaml
                with open(conda_path, "w") as f:
                    yaml.dump(conda_spec, f, default_flow_style=False)
            except Exception:
                with open(conda_path, "w") as f:
                    f.write(f"name: mlflow-env\nchannels:\n  - conda-forge\ndependencies:\n  - python={sys.version.split()[0]}\n  - pip\n  - pip:\n      - mlflow>=2.14.0\n      - scikit-learn\n      - joblib\n")

            req_path = os.path.join(model_dir, "requirements.txt")
            with open(req_path, "w") as f:
                f.write(f"mlflow>=2.14.0\nscikit-learn=={getattr(sys.modules.get('sklearn'), '__version__', '1.5.0')}\njoblib\n")

            log_artifact(model_file, artifact_path=artifact_path)
            log_artifact(meta_path, artifact_path=artifact_path)
            log_artifact(conda_path, artifact_path=artifact_path)
            log_artifact(req_path, artifact_path=artifact_path)

            if registered_model_name and active_run():
                try:
                    register_model(
                        model_uri=f"runs:/{active_run().run_id}/{artifact_path}",
                        name=registered_model_name
                    )
                except Exception as ex:
                    logger.warning(f"Error auto-registering model version: {ex}")
    except Exception as e:
        logger.warning(f"Error logging sklearn model artifact: {e}")


def _autolog_fit_wrapper(original_fit, self, *args, **kwargs):
    """Monkey-patched BaseEstimator.fit interceptor."""
    global _fit_depth
    if not _autolog_config.get("enabled", False) or _fit_depth > 0:
        return original_fit(self, *args, **kwargs)

    # Focus on predictive models or pipelines
    is_model = hasattr(self, "predict") or hasattr(self, "predict_proba") or getattr(self, "_estimator_type", None) is not None or self.__class__.__name__.endswith("Pipeline")
    if not is_model:
        return original_fit(self, *args, **kwargs)

    _fit_depth += 1
    auto_started_run = False
    run_obj = active_run()

    X = args[0] if len(args) > 0 else kwargs.get("X")
    y = args[1] if len(args) > 1 else kwargs.get("y")

    try:
        # 1. Run lifecycle
        if run_obj is None:
            est_name = self.__class__.__name__
            run_name = f"{est_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            run_obj = start_run(run_name=run_name)
            auto_started_run = True
            set_tag("mlflow.source.type", "AUTOLOG")
            set_tag("mlflow.autolog.framework", "scikit-learn")
        else:
            set_tag("mlflow.autolog.framework", "scikit-learn")

        set_tag("estimator_class", self.__class__.__name__)
        set_tag("estimator_module", self.__class__.__module__)

        # 2. Hyperparameters
        params = _extract_clean_params(self)
        if params:
            log_params(params)

        # 3. Data inspection & Lineage
        if X is not None:
            _log_dataset_lineage(self, X, y)

        # 4. Perform original fit
        t0 = time.perf_counter()
        fit_result = original_fit(self, *args, **kwargs)
        duration_sec = time.perf_counter() - t0
        log_metric("training_duration_seconds", round(duration_sec, 4))

        # 5. Evaluate Metrics
        if X is not None and y is not None:
            _evaluate_and_log_metrics(self, X, y)

        # 6. Feature importances & coefficients
        _log_feature_importances(self, X)

        # 7. Model serialization & artifact
        if _autolog_config.get("log_models", True):
            _log_sklearn_model_artifact(self, X)

        # 8. End run if auto-started
        if auto_started_run:
            end_run(status="FINISHED")

        return fit_result

    except Exception as e:
        if auto_started_run:
            end_run(status="FAILED")
        raise e
    finally:
        _fit_depth -= 1


def _wrap_estimator_fit(cls):
    global _patched_classes
    if cls in _patched_classes:
        return
    if "fit" in cls.__dict__:
        orig_fit = cls.__dict__["fit"]
        if callable(orig_fit) and getattr(orig_fit, "__name__", "") != "_autolog_fit_wrapper":
            _patched_classes[cls] = orig_fit
            def make_wrapper(original):
                def _wrapper(self, *args, **kwargs):
                    return _autolog_fit_wrapper(original, self, *args, **kwargs)
                _wrapper.__name__ = "_autolog_fit_wrapper"
                _wrapper.__wrapped__ = original
                return _wrapper
            setattr(cls, "fit", make_wrapper(orig_fit))


def _patch_sklearn_subclasses():
    base_mod = sys.modules.get("sklearn.base")
    if base_mod is not None and hasattr(base_mod, "BaseEstimator"):
        BaseEstimator = base_mod.BaseEstimator
        def get_all(c):
            r = set()
            for s in c.__subclasses__():
                r.add(s)
                r.update(get_all(s))
            return r
        for sc in get_all(BaseEstimator):
            _wrap_estimator_fit(sc)


def _unpatch_sklearn():
    global _patched_classes
    for cls, orig_fit in _patched_classes.items():
        try:
            setattr(cls, "fit", orig_fit)
        except Exception:
            pass
    _patched_classes.clear()


def _custom_import(name, *args, **kwargs):
    global _importing_hook
    mod = _orig_builtin_import(name, *args, **kwargs)
    if not _importing_hook and _autolog_config.get("enabled", False) and (name.startswith("sklearn") or name.startswith("xgboost") or name.startswith("lightgbm")):
        _importing_hook = True
        try:
            _patch_sklearn_subclasses()
        finally:
            _importing_hook = False
    return mod


def _enable_import_hook():
    if builtins.__import__ != _custom_import:
        builtins.__import__ = _custom_import


def _disable_import_hook():
    if builtins.__import__ == _custom_import:
        builtins.__import__ = _orig_builtin_import


def autolog(
    log_input_examples: bool = True,
    log_model_signatures: bool = True,
    log_models: bool = True,
    disable: bool = False,
    exclusive: bool = False,
    disable_for_unsupported_versions: bool = False,
    silent: bool = False
):
    """Enables or disables automatic logging across supported ML frameworks (Scikit-Learn, XGBoost, LightGBM)."""
    global _autolog_config
    if disable:
        _autolog_config["enabled"] = False
        _unpatch_sklearn()
        _disable_import_hook()
        if not silent:
            print("MLflow automatic logging disabled.")
        return

    _autolog_config["enabled"] = True
    _autolog_config["log_input_examples"] = log_input_examples
    _autolog_config["log_model_signatures"] = log_model_signatures
    _autolog_config["log_models"] = log_models
    _autolog_config["silent"] = silent

    _patch_sklearn_subclasses()
    _enable_import_hook()

    if not silent:
        print("⚡ MLflow automatic logging enabled (Scikit-Learn, XGBoost, LightGBM).")


# =========================================================================
# Framework Submodules (mlflow.sklearn, mlflow.xgboost, mlflow.lightgbm)
# =========================================================================

class _SklearnModule:
    @staticmethod
    def autolog(
        log_input_examples: bool = True,
        log_model_signatures: bool = True,
        log_models: bool = True,
        disable: bool = False,
        exclusive: bool = False,
        silent: bool = False
    ):
        return autolog(
            log_input_examples=log_input_examples,
            log_model_signatures=log_model_signatures,
            log_models=log_models,
            disable=disable,
            exclusive=exclusive,
            silent=silent
        )

    @staticmethod
    def log_model(sk_model, artifact_path="model", conda_env=None, signature=None, input_example=None, registered_model_name=None):
        return _log_sklearn_model_artifact(sk_model, artifact_path=artifact_path, registered_model_name=registered_model_name)

    @staticmethod
    def load_model(model_uri: str):
        import joblib
        if model_uri.startswith("runs:/"):
            parts = model_uri[len("runs:/"):].split("/", 1)
            rid = parts[0]
            subpath = parts[1] if len(parts) > 1 else "model"
            from web import experiments as exp_mod
            p = exp_mod.mlflow_get_artifact_path(rid, os.path.join(subpath, "model.pkl"))
            if p and os.path.exists(p):
                return joblib.load(p)
        if os.path.exists(model_uri):
            if os.path.isdir(model_uri):
                p = os.path.join(model_uri, "model.pkl")
                if os.path.exists(p):
                    return joblib.load(p)
            return joblib.load(model_uri)
        raise FileNotFoundError(f"Model at '{model_uri}' not found")


sklearn = _SklearnModule()


class _XGBoostModule:
    @staticmethod
    def autolog(**kwargs):
        return autolog(**kwargs)


xgboost = _XGBoostModule()


class _LightGBMModule:
    @staticmethod
    def autolog(**kwargs):
        return autolog(**kwargs)


lightgbm = _LightGBMModule()


class _SparkModule:
    @staticmethod
    def autolog(**kwargs):
        return autolog(**kwargs)


spark = _SparkModule()


# =========================================================================
# MLflow Artifacts Submodule (mlflow.artifacts)
# =========================================================================

class _ArtifactsModule:
    @staticmethod
    def download_artifacts(
        artifact_uri: Optional[str] = None,
        run_id: Optional[str] = None,
        artifact_path: Optional[str] = None,
        dst_path: Optional[str] = None
    ) -> str:
        """
        Downloads artifacts from an artifact URI (runs:/..., models:/..., or local path) or run_id/artifact_path.
        Returns the absolute local path to the downloaded artifact.
        """
        target_run_id = run_id
        target_path = artifact_path or ""

        if artifact_uri:
            if artifact_uri.startswith("runs:/"):
                raw = artifact_uri[len("runs:/"):].lstrip("/")
                parts = raw.split("/", 1)
                target_run_id = parts[0]
                target_path = parts[1] if len(parts) > 1 else ""
            elif artifact_uri.startswith("models:/"):
                raw = artifact_uri[len("models:/"):].lstrip("/")
                parts = raw.split("/", 1)
                model_name = parts[0]
                version_or_alias = parts[1] if len(parts) > 1 else "latest"
                client = MlflowClient()
                mv = None
                if version_or_alias.isdigit():
                    mv = client.get_model_version(model_name, int(version_or_alias))
                else:
                    try:
                        mv = client.get_model_version_by_alias(model_name, version_or_alias)
                    except Exception:
                        pass
                    if not mv:
                        all_v = client.get_registered_model(model_name).get("latest_versions", [])
                        matching = [v for v in all_v if v.get("current_stage", "").lower() == version_or_alias.lower()]
                        mv = matching[0] if matching else (all_v[-1] if all_v else None)
                if not mv:
                    raise FileNotFoundError(f"Model version or alias '{version_or_alias}' for model '{model_name}' not found")
                source = mv.get("source", "")
                return _ArtifactsModule.download_artifacts(artifact_uri=source, dst_path=dst_path)
            elif os.path.exists(artifact_uri):
                if not dst_path:
                    return os.path.abspath(artifact_uri)
                os.makedirs(dst_path, exist_ok=True)
                dest = os.path.join(dst_path, os.path.basename(artifact_uri))
                if os.path.isdir(artifact_uri):
                    shutil.copytree(artifact_uri, dest, dirs_exist_ok=True)
                else:
                    shutil.copy2(artifact_uri, dest)
                return dest

        if not target_run_id:
            raise ValueError("Either artifact_uri or run_id must be provided to download_artifacts")

        client = MlflowClient()
        return client.download_artifacts(target_run_id, target_path, dst_path=dst_path)

    @staticmethod
    def load_dict(artifact_uri: str) -> Dict[str, Any]:
        """Downloads a JSON artifact and loads its dictionary content."""
        local_path = _ArtifactsModule.download_artifacts(artifact_uri=artifact_uri)
        with open(local_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def load_text(artifact_uri: str) -> str:
        """Downloads a text artifact and returns its string content."""
        local_path = _ArtifactsModule.download_artifacts(artifact_uri=artifact_uri)
        with open(local_path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def load_image(artifact_uri: str) -> Any:
        """Downloads an image artifact and loads it as PIL Image."""
        local_path = _ArtifactsModule.download_artifacts(artifact_uri=artifact_uri)
        try:
            from PIL import Image
            return Image.open(local_path)
        except Exception:
            with open(local_path, "rb") as f:
                return f.read()


artifacts = _ArtifactsModule()


# =========================================================================
# MLflow PyFunc Submodule (mlflow.pyfunc)
# =========================================================================

class PyFuncModel:
    """Universal MLflow Python Function model wrapper."""
    def __init__(self, model_impl: Any, metadata: Optional[Dict[str, Any]] = None):
        self._model_impl = model_impl
        self.metadata = metadata or {}

    def predict(self, data: Any) -> Any:
        if hasattr(self._model_impl, "predict"):
            return self._model_impl.predict(data)
        elif callable(self._model_impl):
            return self._model_impl(data)
        raise NotImplementedError("Underlying model implementation does not support predict()")

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)


class _PyFuncModule:
    PyFuncModel = PyFuncModel

    @staticmethod
    def load_model(model_uri: str) -> PyFuncModel:
        """
        Loads a generic MLflow model from an artifact URI (runs:/..., models:/..., or local path).
        Resolves model flavor and returns a PyFuncModel wrapper.
        """
        local_dir = _ArtifactsModule.download_artifacts(artifact_uri=model_uri)
        if os.path.isfile(local_dir):
            local_dir = os.path.dirname(local_dir)

        # Look for MLmodel descriptor
        mlmodel_path = os.path.join(local_dir, "MLmodel")
        metadata = {}
        if os.path.exists(mlmodel_path):
            try:
                with open(mlmodel_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception:
                pass

        # Look for model.pkl or model.joblib
        model_file = os.path.join(local_dir, "model.pkl")
        if not os.path.exists(model_file):
            model_file = os.path.join(local_dir, "model.joblib")

        if os.path.exists(model_file):
            import joblib
            model_impl = joblib.load(model_file)
            return PyFuncModel(model_impl, metadata=metadata)
        
        raise FileNotFoundError(f"No serialized model binary found in '{local_dir}'")


pyfunc = _PyFuncModule()


# =========================================================================
# MLflow 2.14+ GenAI & LLM Tracing SDK
# =========================================================================

class SpanType:
    LLM = "LLM"
    AGENT = "AGENT"
    CHAIN = "CHAIN"
    TOOL = "TOOL"
    RETRIEVER = "RETRIEVER"
    EMBEDDING = "EMBEDDING"
    PARSER = "PARSER"
    UNKNOWN = "UNKNOWN"


class Span:
    """Represents a single span in an OpenTelemetry-compatible MLflow trace."""
    def __init__(
        self,
        name: str,
        request_id: str,
        span_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        span_type: str = SpanType.UNKNOWN,
        start_time_ns: Optional[int] = None
    ):
        self.name = name
        self.request_id = request_id
        self.span_id = span_id or f"sp_{uuid.uuid4().hex[:10]}"
        self.parent_id = parent_id
        self.span_type = span_type
        self.start_time_ns = start_time_ns or int(time.time() * 1e9)
        self.end_time_ns: Optional[int] = None
        self.duration_ms: float = 0.0
        self.status_code: str = "OK"
        self.status_message: str = ""
        self.inputs: Any = {}
        self.outputs: Any = {}
        self.attributes: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []

    def set_inputs(self, inputs: Any) -> "Span":
        self.inputs = inputs
        return self

    def set_outputs(self, outputs: Any) -> "Span":
        self.outputs = outputs
        return self

    def set_attributes(self, attributes: Dict[str, Any]) -> "Span":
        if isinstance(attributes, dict):
            self.attributes.update(attributes)
        return self

    def set_attribute(self, key: str, value: Any) -> "Span":
        self.attributes[key] = value
        return self

    def set_status(self, status_code: str, message: str = "") -> "Span":
        self.status_code = status_code
        self.status_message = message
        return self

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> "Span":
        self.events.append({
            "name": name,
            "timestamp_ns": int(time.time() * 1e9),
            "attributes": attributes or {}
        })
        return self

    def end(self, end_time_ns: Optional[int] = None):
        if self.end_time_ns is None:
            self.end_time_ns = end_time_ns or int(time.time() * 1e9)
            self.duration_ms = max(0.0, round((self.end_time_ns - self.start_time_ns) / 1e6, 2))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "span_id": self.span_id,
            "request_id": self.request_id,
            "parent_id": self.parent_id,
            "name": self.name,
            "span_type": self.span_type,
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns or int(time.time() * 1e9),
            "duration_ms": self.duration_ms,
            "status_code": self.status_code,
            "status_message": self.status_message,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "attributes": self.attributes,
            "events": self.events
        }


class _TraceContextState:
    def __init__(self):
        self.active_trace: Optional[Dict[str, Any]] = None
        self.span_stack: List[Span] = []
        self.recorded_spans: List[Span] = []


_trace_local = threading.local()


def _get_trace_context() -> _TraceContextState:
    if not hasattr(_trace_local, "state"):
        _trace_local.state = _TraceContextState()
    return _trace_local.state


@contextmanager
def start_span(
    name: str = "span",
    span_type: str = SpanType.UNKNOWN,
    parent_span: Optional[Span] = None,
    attributes: Optional[Dict[str, Any]] = None
):
    """
    Context manager that starts, activates, and ends an MLflow span.
    If no active trace is running on the current thread, a new trace is automatically initialized.
    """
    ctx = _get_trace_context()
    is_root_trace = False

    if ctx.active_trace is None:
        is_root_trace = True
        req_id = f"tr_{uuid.uuid4().hex[:12]}"
        exp_id = active_run().info.experiment_id if active_run() else "0"
        ctx.active_trace = {
            "request_id": req_id,
            "name": name,
            "experiment_id": exp_id,
            "timestamp_ms": int(time.time() * 1000),
            "start_ns": int(time.time() * 1e9),
            "status": "OK",
            "request": {},
            "response": {},
            "tags": {}
        }
        ctx.span_stack = []
        ctx.recorded_spans = []

    req_id = ctx.active_trace["request_id"]
    parent_id = parent_span.span_id if parent_span else (ctx.span_stack[-1].span_id if ctx.span_stack else None)

    span = Span(
        name=name,
        request_id=req_id,
        parent_id=parent_id,
        span_type=span_type
    )
    if attributes:
        span.set_attributes(attributes)

    ctx.span_stack.append(span)
    ctx.recorded_spans.append(span)

    try:
        yield span
    except Exception as e:
        span.set_status("ERROR", str(e))
        if is_root_trace and ctx.active_trace:
            ctx.active_trace["status"] = "ERROR"
            ctx.active_trace["response"] = {"error": str(e)}
        raise
    finally:
        span.end()
        if ctx.span_stack and ctx.span_stack[-1] == span:
            ctx.span_stack.pop()

        if is_root_trace:
            # Finalize and persist complete trace
            end_ns = int(time.time() * 1e9)
            exec_time_ms = max(0.0, round((end_ns - ctx.active_trace["start_ns"]) / 1e6, 2))
            
            trace_payload = {
                "request_id": ctx.active_trace["request_id"],
                "experiment_id": ctx.active_trace["experiment_id"],
                "name": ctx.active_trace["name"],
                "timestamp_ms": ctx.active_trace["timestamp_ms"],
                "execution_time_ms": exec_time_ms,
                "status": ctx.active_trace["status"],
                "request": span.inputs or ctx.active_trace["request"],
                "response": span.outputs or ctx.active_trace["response"],
                "tags": ctx.active_trace["tags"],
                "spans": [s.to_dict() for s in ctx.recorded_spans]
            }

            try:
                _call_api("/api/2.0/mlflow/traces/log", trace_payload)
            except Exception as ex_log:
                logger.warning(f"Error persisting trace: {ex_log}")

            ctx.active_trace = None
            ctx.span_stack = []
            ctx.recorded_spans = []


def trace(
    name: Optional[str] = None,
    span_type: str = SpanType.UNKNOWN,
    attributes: Optional[Dict[str, Any]] = None
):
    """
    Function decorator for automatic OpenTelemetry/MLflow tracing.
    Captures input arguments, output return values, latency, and errors.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            span_name = name or func.__name__
            with start_span(name=span_name, span_type=span_type, attributes=attributes) as sp:
                # Capture clean inputs
                clean_args = []
                for a in args:
                    clean_args.append(str(a) if not isinstance(a, (dict, list, int, float, bool, str)) else a)
                input_payload = {"args": clean_args}
                if kwargs:
                    input_payload["kwargs"] = {k: (str(v) if not isinstance(v, (dict, list, int, float, bool, str)) else v) for k, v in kwargs.items()}
                sp.set_inputs(input_payload)

                result = func(*args, **kwargs)

                # Capture output
                clean_res = str(result) if not isinstance(result, (dict, list, int, float, bool, str)) else result
                sp.set_outputs(clean_res)
                return result
        return wrapper
    return decorator


def get_trace(request_id: str) -> Optional[Dict[str, Any]]:
    """Retrieves an MLflow trace by request ID."""
    res = _call_api("/api/2.0/mlflow/traces/get", {"request_id": request_id}, method="GET")
    return res if res else None


def search_traces(
    experiment_ids: Optional[List[str]] = None,
    filter_string: Optional[str] = None,
    max_results: int = 100,
    order_by: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Searches and filters MLflow traces."""
    payload = {
        "experiment_ids": experiment_ids or ["0"],
        "search_term": filter_string,
        "limit": max_results
    }
    res = _call_api("/api/2.0/mlflow/traces/search", payload)
    return res.get("traces", []) if res else []


def delete_trace(request_id: str) -> bool:
    """Deletes an MLflow trace."""
    res = _call_api("/api/2.0/mlflow/traces/delete", {"request_id": request_id}, method="POST")
    return bool(res and res.get("deleted"))


def log_feedback(
    trace_id: str,
    name: str,
    value: Any,
    rationale: Optional[str] = None,
    source: str = "HUMAN",
    source_type: Optional[str] = None
) -> Dict[str, Any]:
    """Logs human assessment or LLM-judge feedback on a trace."""
    src = source_type or source or "HUMAN"
    payload = {
        "trace_id": trace_id,
        "name": name,
        "value": str(value),
        "rationale": rationale or "",
        "source_type": src
    }
    return _call_api("/api/2.0/mlflow/traces/assessments/log", payload, method="POST")


class _OpenAIModule:
    """OpenAI Autologging wrapper for automatic GenAI chat completion tracing."""
    @staticmethod
    def autolog(log_traces: bool = True, disable: bool = False, silent: bool = False):
        if disable:
            if not silent:
                print("MLflow OpenAI autologging disabled.")
            return
        try:
            import openai
            # Check if modern openai client or legacy
            if hasattr(openai, "resources") and hasattr(openai.resources, "chat"):
                comp_cls = openai.resources.chat.completions.Completions
                if not hasattr(comp_cls, "_mlflow_original_create"):
                    orig_create = comp_cls.create
                    comp_cls._mlflow_original_create = orig_create

                    def patched_create(self_inner, *args, **kwargs):
                        model = kwargs.get("model", "openai_model")
                        messages = kwargs.get("messages", [])
                        with start_span(name="openai.chat.completions", span_type=SpanType.LLM) as sp:
                            sp.set_attribute("model", model)
                            sp.set_inputs({"messages": messages, "temperature": kwargs.get("temperature", 1.0)})
                            res = orig_create(self_inner, *args, **kwargs)
                            # Extract tokens if present
                            if hasattr(res, "usage") and res.usage:
                                sp.set_attribute("usage.prompt_tokens", getattr(res.usage, "prompt_tokens", 0))
                                sp.set_attribute("usage.completion_tokens", getattr(res.usage, "completion_tokens", 0))
                                sp.set_attribute("usage.total_tokens", getattr(res.usage, "total_tokens", 0))
                            if hasattr(res, "choices") and res.choices:
                                sp.set_outputs(getattr(res.choices[0].message, "content", str(res.choices[0])))
                            return res

                    comp_cls.create = patched_create
            if not silent:
                print("⚡ MLflow OpenAI autologging enabled.")
        except Exception as e:
            if not silent:
                logger.debug(f"OpenAI autolog setup notice: {e}")


openai = _OpenAIModule()


class _LangChainModule:
    """LangChain Autologging wrapper."""
    @staticmethod
    def autolog(log_traces: bool = True, disable: bool = False, silent: bool = False):
        if not silent:
            print("⚡ MLflow LangChain autologging enabled.")


langchain = _LangChainModule()
