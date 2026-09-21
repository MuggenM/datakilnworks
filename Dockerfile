FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Directory layout for lakehouse warehouse, notebooks, web app, and ipython startup hooks
RUN mkdir -p /workspace/warehouse /workspace/notebooks /workspace/web /root/.ipython/profile_default/startup

# Copy IPython bootstrap shim (used by the in-Studio notebook kernels)
COPY config/00_databricks_shim.py /root/.ipython/profile_default/startup/00_databricks_shim.py

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
