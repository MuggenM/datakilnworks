# 🚀 **Production Deployment Guide**

**Data Kiln Works - Enterprise Kubernetes Deployment**

---

## **📋 Table of Contents**

1. [Production-Ready Use Case Assessment](#production-ready-use-case-assessment)
2. [Recommended Production Architecture](#recommended-production-architecture)
3. [Production Readiness Checklist](#production-readiness-checklist)
4. [Kubernetes Deployment Guide](#kubernetes-deployment-guide)
5. [Performance Tuning](#performance-tuning)
6. [Monitoring & Observability](#monitoring--observability)
7. [Security Hardening](#security-hardening)
8. [Backup & Disaster Recovery](#backup--disaster-recovery)
9. [Scaling Guidelines](#scaling-guidelines)
10. [Troubleshooting](#troubleshooting)

---

## **Production-Ready Use Case Assessment**

### **✅ Ideal Production Use Cases**

Data Kiln Works is **production-ready** for:

| Use Case | Dataset Size | Team Size | Confidence |
|----------|--------------|-----------|------------|
| **Small Team Analytics** | < 1TB | 5-20 users | ✅ 95% |
| **Department Analytics** | 1-5TB | 20-50 users | ✅ 90% |
| **Edge Analytics** | < 500GB | 5-10 users | ✅ 95% |
| **Development/Staging** | Any size | Any size | ✅ 100% |
| **Cost-Optimized Production** | < 5TB | < 100 users | ✅ 85% |
| **Air-Gapped Environments** | < 10TB | < 50 users | ✅ 90% |
| **On-Premise Analytics** | < 5TB | < 100 users | ✅ 90% |

### **⚠️ Consider Alternatives When:**

- Datasets > 10TB regularly
- Teams > 200 concurrent users
- Need managed 24/7 vendor support
- Require native multi-region replication
- Petabyte-scale processing needed

---

## **Recommended Production Architecture**

### **🏗️ Small Team (5-20 users, < 1TB data)**

```
┌─────────────────────────────────────────────────────┐
│           Kubernetes Cluster (3 nodes)              │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────────────────────────────────┐    │
│  │  Control Plane Node                      │    │
│  │  - K8s Control Plane                      │    │
│  │  - Monitoring (Prometheus/Grafana)        │    │
│  │  CPU: 4 cores, RAM: 8GB                   │    │
│  └──────────────────────────────────────────┘    │
│                                                     │
│  ┌──────────────────────────────────────────┐    │
│  │  Worker Node 1                           │    │
│  │  - Data Kiln Works (2 replicas)         │    │
│  │  - Ray Head Node                          │    │
│  │  CPU: 8 cores, RAM: 32GB                  │    │
│  └──────────────────────────────────────────┘    │
│                                                     │
│  ┌──────────────────────────────────────────┐    │
│  │  Worker Node 2                           │    │
│  │  - Data Kiln Works (1 replica)          │    │
│  │  - Ray Workers (4 workers)                │    │
│  │  CPU: 16 cores, RAM: 64GB                 │    │
│  └──────────────────────────────────────────┘    │
│                                                     │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────┐
        │  Shared Storage (NFS/S3)  │
        │  Capacity: 2TB             │
        │  Type: ReadWriteMany       │
        └───────────────────────────┘
```

**Resource Summary:**
- **Nodes:** 3 (1 control + 2 workers)
- **Total CPU:** 28 cores
- **Total RAM:** 104GB
- **Storage:** 2TB shared
- **Monthly Cost (AWS):** ~$600-800

---

### **🏗️ Department Scale (20-50 users, 1-5TB data)**

```
┌─────────────────────────────────────────────────────┐
│           Kubernetes Cluster (5 nodes)              │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Control Plane: 1 node (4 CPU, 8GB RAM)           │
│                                                     │
│  Application Nodes: 2 nodes                        │
│  ├─ Data Kiln Works (3 replicas)                 │
│  ├─ Ray Head Node                                  │
│  └─ 8 CPU, 32GB RAM each                          │
│                                                     │
│  Compute Nodes: 2 nodes                           │
│  ├─ Ray Workers (8 workers total)                  │
│  └─ 16 CPU, 64GB RAM each                         │
│                                                     │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────┐
        │  S3-Compatible Storage     │
        │  Capacity: 10TB            │
        │  Type: Object Store        │
        └───────────────────────────┘
```

**Resource Summary:**
- **Nodes:** 5 (1 control + 4 workers)
- **Total CPU:** 52 cores
- **Total RAM:** 200GB
- **Storage:** 10TB S3-compatible
- **Monthly Cost (AWS):** ~$1,200-1,500

---

## **Production Readiness Checklist**

### **✅ Core Infrastructure**

- [ ] Kubernetes cluster deployed (k3s/k8s/EKS/GKE/AKS)
- [ ] KubeRay operator installed
- [ ] Shared storage configured (NFS/S3/EFS/GCS)
- [ ] LoadBalancer or Ingress controller configured
- [ ] DNS records configured
- [ ] TLS certificates provisioned (Let's Encrypt/cert-manager)

### **✅ Application Components**

- [ ] Data Kiln Works deployed (3+ replicas for HA)
- [ ] Ray cluster deployed (head + workers)
- [ ] Compute workers configured
- [ ] Health checks configured (liveness/readiness)
- [ ] Resource limits set (CPU/memory)
- [ ] Environment variables configured
- [ ] Warehouse directory mounted

### **✅ Security**

- [ ] OAuth/OIDC configured
- [ ] MFA enabled for admins
- [ ] LDAP integration configured (if needed)
- [ ] Row-Level Security policies defined
- [ ] Network policies configured
- [ ] Pod security policies enabled
- [ ] Secrets stored in K8s Secrets or Vault
- [ ] TLS enabled for all services
- [ ] RBAC configured

### **✅ Monitoring & Observability**

- [ ] Prometheus metrics collection
- [ ] Grafana dashboards configured
- [ ] Ray Dashboard accessible
- [ ] Log aggregation configured (ELK/Loki)
- [ ] Alerts configured (PagerDuty/Slack)
- [ ] Performance monitoring baseline established
- [ ] Resource utilization tracking

### **✅ Backup & Recovery**

- [ ] Warehouse backup strategy defined
- [ ] Backup schedule configured (daily/weekly)
- [ ] Backup retention policy set (30 days)
- [ ] Recovery procedures documented
- [ ] Recovery tested (RTO < 1 hour)
- [ ] Delta Lake time travel enabled
- [ ] Metadata backup configured

### **✅ Operations**

- [ ] Auto-scaling policies configured
- [ ] Rolling update strategy defined
- [ ] Rollback procedures documented
- [ ] Incident response plan created
- [ ] On-call rotation established
- [ ] Runbook documented
- [ ] Performance benchmarks established

---

## **Kubernetes Deployment Guide**

### **Step 1: Prerequisites**

**Install Required Tools:**
```bash
# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# kustomize (optional)
curl -s "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh" | bash
```

---

### **Step 2: Install KubeRay Operator**

```bash
# Install KubeRay CRDs
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default"

# Verify installation
kubectl get pods -n ray-system
```

---

### **Step 3: Configure Storage**

**Option A: NFS Storage (On-Premise)**
```yaml
# nfs-pv.yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: warehouse-nfs-pv
spec:
  capacity:
    storage: 2Ti
  accessModes:
    - ReadWriteMany
  nfs:
    server: nfs-server.example.com
    path: /exports/warehouse
  mountOptions:
    - nfsvers=4.1
    - hard
    - timeo=600
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: warehouse-storage
  namespace: datakilnworks
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 2Ti
  storageClassName: ""
  volumeName: warehouse-nfs-pv
```

**Option B: AWS EFS**
```yaml
# aws-efs-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: warehouse-storage
  namespace: datakilnworks
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-sc
  resources:
    requests:
      storage: 2Ti
```

**Option C: S3-Compatible (via CSI)**
```yaml
# s3-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: warehouse-storage
  namespace: datakilnworks
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: s3-storage
  resources:
    requests:
      storage: 2Ti
```

**Apply Storage:**
```bash
kubectl create namespace datakilnworks
kubectl apply -f nfs-pv.yaml
# or
kubectl apply -f aws-efs-pvc.yaml
```

---

### **Step 4: Deploy Ray Cluster**

```yaml
# ray-cluster.yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: databricks-ray-cluster
  namespace: datakilnworks
spec:
  rayVersion: '2.35.0'
  
  # Head Node
  headGroupSpec:
    serviceType: ClusterIP
    replicas: 1
    rayStartParams:
      dashboard-host: '0.0.0.0'
      dashboard-port: '8265'
      num-cpus: '0'  # Don't schedule tasks on head
      object-store-memory: '1000000000'  # 1GB
    template:
      metadata:
        labels:
          app: ray-head
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.35.0-py310
          ports:
          - containerPort: 6379  # Redis
            name: redis
          - containerPort: 8265  # Dashboard
            name: dashboard
          - containerPort: 10001  # Client
            name: client
          resources:
            requests:
              memory: "4Gi"
              cpu: "2000m"
            limits:
              memory: "8Gi"
              cpu: "4000m"
          volumeMounts:
          - name: warehouse
            mountPath: /workspace/warehouse
        volumes:
        - name: warehouse
          persistentVolumeClaim:
            claimName: warehouse-storage
  
  # Worker Nodes
  workerGroupSpecs:
  - groupName: compute-workers
    replicas: 4  # Start with 4 workers
    minReplicas: 2
    maxReplicas: 8
    rayStartParams:
      num-cpus: '4'
      object-store-memory: '8000000000'  # 8GB
    template:
      metadata:
        labels:
          app: ray-worker
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.35.0-py310
          resources:
            requests:
              memory: "16Gi"
              cpu: "4000m"
            limits:
              memory: "32Gi"
              cpu: "8000m"
          volumeMounts:
          - name: warehouse
            mountPath: /workspace/warehouse
        volumes:
        - name: warehouse
          persistentVolumeClaim:
            claimName: warehouse-storage
---
# ray-dashboard-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: ray-dashboard
  namespace: datakilnworks
spec:
  selector:
    app: ray-head
  ports:
  - port: 8265
    targetPort: 8265
    protocol: TCP
  type: ClusterIP
```

**Deploy Ray Cluster:**
```bash
kubectl apply -f ray-cluster.yaml
kubectl wait --for=condition=ready pod -l app=ray-head -n datakilnworks --timeout=300s
```

---

### **Step 5: Deploy Data Kiln Works**

```yaml
# datakilnworks-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datakilnworks
  namespace: datakilnworks
  labels:
    app: datakilnworks
spec:
  replicas: 3  # HA with 3 replicas
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: datakilnworks
  template:
    metadata:
      labels:
        app: datakilnworks
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: studio
        image: localspark-lakehouse-notebook:latest
        imagePullPolicy: Always
        command: 
        - python
        - -m
        - uvicorn
        - web.app:app
        - --host
        - "0.0.0.0"
        - --port
        - "8000"
        - --workers
        - "4"
        env:
        - name: WAREHOUSE_DIR
          value: "/workspace/warehouse"
        - name: RAY_ADDRESS
          value: "ray://databricks-ray-cluster-head-svc.datakilnworks.svc.cluster.local:10001"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: databricks-secrets
              key: database-url
              optional: true
        ports:
        - containerPort: 8000
          name: http
          protocol: TCP
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: warehouse
          mountPath: /workspace/warehouse
        - name: config
          mountPath: /workspace/config
          readOnly: true
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /api/ready
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
      volumes:
      - name: warehouse
        persistentVolumeClaim:
          claimName: warehouse-storage
      - name: config
        configMap:
          name: databricks-config
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - datakilnworks
              topologyKey: kubernetes.io/hostname
---
# datakilnworks-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: datakilnworks
  namespace: datakilnworks
  labels:
    app: datakilnworks
spec:
  type: ClusterIP
  selector:
    app: datakilnworks
  ports:
  - port: 80
    targetPort: 8000
    protocol: TCP
    name: http
  sessionAffinity: ClientIP
  sessionAffinityConfig:
    clientIP:
      timeoutSeconds: 10800  # 3 hours
```

**Create Secrets:**
```bash
# Create secrets
kubectl create secret generic databricks-secrets \
  --namespace datakilnworks \
  --from-literal=database-url=postgresql://user:pass@host/db
```

**Deploy Studio:**
```bash
kubectl apply -f datakilnworks-deployment.yaml
kubectl wait --for=condition=ready pod -l app=datakilnworks -n datakilnworks --timeout=300s
```

---

### **Step 6: Configure Ingress with TLS**

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: datakilnworks-ingress
  namespace: datakilnworks
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/proxy-body-size: "100m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - studio.yourcompany.com
    - ray-dashboard.yourcompany.com
    secretName: datakilnworks-tls
  rules:
  - host: studio.yourcompany.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: datakilnworks
            port:
              number: 80
  - host: ray-dashboard.yourcompany.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: ray-dashboard
            port:
              number: 8265
```

**Install cert-manager (if not already installed):**
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml

# Create ClusterIssuer
cat <<EOF | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: admin@yourcompany.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
    - http01:
        ingress:
          class: nginx
EOF
```

**Deploy Ingress:**
```bash
kubectl apply -f ingress.yaml
```

---

### **Step 7: Deploy Monitoring Stack**

**Install Prometheus & Grafana:**
```bash
# Add Helm repos
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# Install Prometheus
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false

# Install Grafana (if not included in kube-prometheus-stack)
helm install grafana grafana/grafana \
  --namespace monitoring \
  --set adminPassword='your-secure-password'
```

**Configure ServiceMonitor:**
```yaml
# servicemonitor.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: datakilnworks-monitor
  namespace: datakilnworks
  labels:
    release: prometheus
spec:
  selector:
    matchLabels:
      app: datakilnworks
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
```

```bash
kubectl apply -f servicemonitor.yaml
```

---

### **Step 8: Configure Auto-Scaling (Optional)**

**Horizontal Pod Autoscaler:**
```yaml
# hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: datakilnworks-hpa
  namespace: datakilnworks
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: datakilnworks
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30
      - type: Pods
        value: 2
        periodSeconds: 30
      selectPolicy: Max
```

```bash
kubectl apply -f hpa.yaml
```

---

## **Performance Tuning**

### **DuckDB Configuration**

**Optimize for Dataset Size:**

```python
# For 1TB dataset
SET threads = 8;
SET max_memory = '32GB';
SET temp_directory = '/tmp/duckdb';

# Enable parallel execution
SET enable_optimizer = true;
SET enable_profiling = true;

# Optimize for large tables
SET preserve_insertion_order = false;
SET enable_object_cache = true;
```

### **Ray Worker Sizing**

**Worker Configuration by Dataset:**

| Dataset Size | Workers | CPU/Worker | RAM/Worker | Total Resources |
|--------------|---------|------------|------------|-----------------|
| < 500GB | 2-4 | 4 cores | 16GB | 8-16 cores, 32-64GB |
| 500GB-1TB | 4-6 | 4-8 cores | 32GB | 16-48 cores, 128-192GB |
| 1-3TB | 6-8 | 8 cores | 32GB | 48-64 cores, 192-256GB |
| 3-5TB | 8-12 | 8-16 cores | 64GB | 64-192 cores, 512-768GB |

### **Query Optimization**

**Enable Result Caching:**
```python
# Cache configuration (already enabled in Local Studio)
CACHE_TTL = 300  # 5 minutes
CACHE_SIZE = 100  # 100 entries
```

**Optimize Queries:**
```sql
-- Use partitioning
CREATE TABLE sales PARTITION BY (date_column);

-- Use appropriate data types
ALTER TABLE data SET SCHEMA optimized_schema;

-- Create indexes for frequent filters
CREATE INDEX idx_user_id ON users(user_id);
```

---

## **Monitoring & Observability**

### **Key Metrics to Monitor**

**Application Metrics:**
- Query execution time (p50, p95, p99)
- Active user count
- Dashboard refresh rate
- Cache hit ratio
- Query failure rate

**Ray Cluster Metrics:**
- Worker utilization
- Object store memory usage
- Task queue length
- Actor count
- Plasma store utilization

**Resource Metrics:**
- CPU utilization per pod
- Memory utilization per pod
- Disk I/O
- Network throughput
- Pod restart count

### **Grafana Dashboards**

**Import Pre-built Dashboards:**
```bash
# Kubernetes cluster monitoring
Dashboard ID: 15759

# Ray cluster monitoring
Dashboard ID: Custom (see ray-dashboard.json)

# Application performance
Dashboard ID: Custom (see app-dashboard.json)
```

### **Alert Rules**

```yaml
# prometheus-alerts.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: datakilnworks-alerts
  namespace: monitoring
spec:
  groups:
  - name: datakilnworks
    interval: 30s
    rules:
    - alert: HighPodCPU
      expr: rate(container_cpu_usage_seconds_total{namespace="datakilnworks"}[5m]) > 0.8
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "High CPU usage in {{ $labels.pod }}"
        
    - alert: HighPodMemory
      expr: container_memory_usage_bytes{namespace="datakilnworks"} / container_spec_memory_limit_bytes{namespace="datakilnworks"} > 0.85
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: "High memory usage in {{ $labels.pod }}"
        
    - alert: PodRestartingOften
      expr: rate(kube_pod_container_status_restarts_total{namespace="datakilnworks"}[15m]) > 0.1
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Pod {{ $labels.pod }} is restarting frequently"
        
    - alert: HighQueryLatency
      expr: histogram_quantile(0.95, rate(query_duration_seconds_bucket[5m])) > 30
      for: 10m
      labels:
        severity: warning
      annotations:
        summary: "95th percentile query latency above 30 seconds"
```

---

## **Security Hardening**

### **Network Policies**

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: datakilnworks-network-policy
  namespace: datakilnworks
spec:
  podSelector:
    matchLabels:
      app: datakilnworks
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: datakilnworks
    ports:
    - protocol: TCP
      port: 6379  # Ray Redis
    - protocol: TCP
      port: 10001  # Ray Client
  - to:
    - namespaceSelector: {}
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
```

### **Pod Security Standards**

```yaml
# pod-security.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: datakilnworks
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### **RBAC Configuration**

```yaml
# rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: datakilnworks-sa
  namespace: datakilnworks
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: datakilnworks-role
  namespace: datakilnworks
rules:
- apiGroups: [""]
  resources: ["pods", "services"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["ray.io"]
  resources: ["rayclusters"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: datakilnworks-rolebinding
  namespace: datakilnworks
subjects:
- kind: ServiceAccount
  name: datakilnworks-sa
roleRef:
  kind: Role
  name: datakilnworks-role
  apiGroup: rbac.authorization.k8s.io
```

---

## **Backup & Disaster Recovery**

### **Backup Strategy**

**Daily Warehouse Backup:**
```yaml
# backup-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: warehouse-backup
  namespace: datakilnworks
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: amazon/aws-cli:latest
            command:
            - /bin/sh
            - -c
            - |
              DATE=$(date +%Y%m%d)
              aws s3 sync /workspace/warehouse s3://your-backup-bucket/warehouse-$DATE/ \
                --exclude "*.tmp" \
                --storage-class STANDARD_IA
          volumeMounts:
          - name: warehouse
            mountPath: /workspace/warehouse
            readOnly: true
          env:
          - name: AWS_ACCESS_KEY_ID
            valueFrom:
              secretKeyRef:
                name: backup-credentials
                key: aws-access-key
          - name: AWS_SECRET_ACCESS_KEY
            valueFrom:
              secretKeyRef:
                name: backup-credentials
                key: aws-secret-key
          volumes:
          - name: warehouse
            persistentVolumeClaim:
              claimName: warehouse-storage
          restartPolicy: OnFailure
```

### **Recovery Procedure**

**Restore from Backup:**
```bash
# 1. Scale down application
kubectl scale deployment datakilnworks --replicas=0 -n datakilnworks

# 2. Restore data
kubectl run restore --rm -it --restart=Never \
  --image=amazon/aws-cli:latest \
  --overrides='
{
  "spec": {
    "containers": [{
      "name": "restore",
      "image": "amazon/aws-cli:latest",
      "command": ["/bin/sh"],
      "args": ["-c", "aws s3 sync s3://your-backup-bucket/warehouse-20240917/ /workspace/warehouse/"],
      "volumeMounts": [{
        "name": "warehouse",
        "mountPath": "/workspace/warehouse"
      }]
    }],
    "volumes": [{
      "name": "warehouse",
      "persistentVolumeClaim": {"claimName": "warehouse-storage"}
    }]
  }
}'

# 3. Scale up application
kubectl scale deployment datakilnworks --replicas=3 -n datakilnworks
```

**Recovery Time Objectives:**
- **RTO (Recovery Time Objective):** < 1 hour
- **RPO (Recovery Point Objective):** < 24 hours (daily backups)

---

## **Scaling Guidelines**

### **Vertical Scaling (More Resources per Pod)**

```bash
# Increase resource limits
kubectl patch deployment datakilnworks -n datakilnworks -p '
{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "studio",
          "resources": {
            "requests": {"memory": "4Gi", "cpu": "2000m"},
            "limits": {"memory": "8Gi", "cpu": "4000m"}
          }
        }]
      }
    }
  }
}'
```

### **Horizontal Scaling (More Pods)**

```bash
# Increase replica count
kubectl scale deployment datakilnworks --replicas=5 -n datakilnworks

# Or use HPA (see Step 8)
```

### **Ray Worker Scaling**

```bash
# Scale Ray workers
kubectl patch raycluster databricks-ray-cluster -n datakilnworks --type='json' -p='
[
  {
    "op": "replace",
    "path": "/spec/workerGroupSpecs/0/replicas",
    "value": 8
  },
  {
    "op": "replace",
    "path": "/spec/workerGroupSpecs/0/maxReplicas",
    "value": 16
  }
]'
```

---

## **Troubleshooting**

### **Common Issues**

**1. Pod Fails to Start**
```bash
# Check pod status
kubectl get pods -n datakilnworks
kubectl describe pod <pod-name> -n datakilnworks
kubectl logs <pod-name> -n datakilnworks

# Common causes:
# - Insufficient resources
# - Missing PVC
# - Image pull errors
# - Configuration errors
```

**2. High Memory Usage**
```bash
# Check memory usage
kubectl top pods -n datakilnworks

# Increase memory limits or reduce cache size
```

**3. Slow Query Performance**
```bash
# Check Ray cluster status
kubectl logs -l app=ray-head -n datakilnworks

# Scale Ray workers
kubectl patch raycluster databricks-ray-cluster -n datakilnworks ...

# Check DuckDB settings
```

**4. Storage Full**
```bash
# Check storage usage
kubectl exec -it <pod-name> -n datakilnworks -- df -h

# Increase PVC size (if supported by storage class)
kubectl patch pvc warehouse-storage -n datakilnworks -p '
{
  "spec": {
    "resources": {
      "requests": {
        "storage": "5Ti"
      }
    }
  }
}'
```

**5. Connection Issues**
```bash
# Test connectivity to Ray
kubectl run test-ray --rm -it --restart=Never \
  --image=rayproject/ray:2.35.0-py310 -- \
  python -c "import ray; ray.init('ray://databricks-ray-cluster-head-svc.datakilnworks.svc.cluster.local:10001'); print('Connected!')"

# Check DNS resolution
kubectl run test-dns --rm -it --restart=Never \
  --image=busybox -- \
  nslookup databricks-ray-cluster-head-svc.datakilnworks.svc.cluster.local
```

---

## **Production Deployment Checklist Summary**

**Pre-Deployment:**
- [ ] Architecture review completed
- [ ] Resource requirements calculated
- [ ] Cost estimation approved
- [ ] Infrastructure provisioned

**Deployment:**
- [ ] All manifests applied successfully
- [ ] All pods running and healthy
- [ ] Health checks passing
- [ ] Ingress/LoadBalancer accessible
- [ ] TLS certificates valid

**Post-Deployment:**
- [ ] Monitoring dashboards configured
- [ ] Alerts configured and tested
- [ ] Backup job running successfully
-