FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /workspace

# Install system dependencies. `upgrade` pulls the security fixes Debian published since the base image was built (CI scans the
# image with Trivy, see .github/workflows/supply-chain.yml).
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install packages
COPY requirements.txt requirements-lock.txt /tmp/
# pip / setuptools / wheel come from the base image and are otherwise never upgraded (pip-audit flags them).
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -rf /usr/local/lib/python3.11/site-packages/ray/jars
# ray/jars is Ray's Java worker support (jackson, guava, httpcore with known CVEs); this image has no JVM and never uses it.

# Directory layout for lakehouse warehouse, notebooks, web app, and ipython startup hooks
RUN mkdir -p /workspace/warehouse /workspace/notebooks /workspace/web /root/.ipython/profile_default/startup

# The application itself, so the image runs on its own (Kubernetes: no bind mounts). docker-compose.yml still bind-mounts ./web and ./docs over
# these for development, where edits apply without a rebuild (uvicorn --reload).
COPY web/ /workspace/web/
COPY docs/ /workspace/docs/

# Copy IPython bootstrap shim (used by the in-Studio notebook kernels)
COPY config/00_databricks_shim.py /root/.ipython/profile_default/startup/00_databricks_shim.py

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]
