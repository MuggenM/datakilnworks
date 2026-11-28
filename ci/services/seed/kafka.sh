#!/bin/bash
# Seed of the kafka profile (runs in the redpanda image): the SCRAM user of redpanda-sasl, a cluster id on every cluster,
# then out/kafka.env.
set -e
rpk security user create dkwci -p dkwci-test-password --mechanism SCRAM-SHA-256 -X admin.hosts=redpanda-sasl:9644 >/dev/null

# test_stream_ops tells two clusters apart by the cluster id in the Kafka metadata (= the cluster_id config value). A fresh
# Redpanda sets it some time after start: on a slow CI runner one cluster had none yet when the test asked. Wait for it,
# else set one, so the stack is only "up" once every cluster reports its own id.
cid() { rpk cluster config get cluster_id -X "admin.hosts=$1:9644" 2>/dev/null || true; }
for b in redpanda redpanda2 redpanda-sasl; do
  for _ in $(seq 30); do [ -n "$(cid "$b")" ] && break; sleep 1; done
  if [ -z "$(cid "$b")" ]; then
    echo "$b has no cluster id after 30 s: setting one"
    rpk cluster config set cluster_id "$(cat /proc/sys/kernel/random/uuid)" -X "admin.hosts=$b:9644"
  fi
done

cat > /out/kafka.env <<'ENV'
KAFKA=redpanda:9092
KAFKA2=redpanda2:9092
REGISTRY=http://redpanda:8081
SASL_KAFKA=redpanda-sasl:9092
SASL_USER=dkwci
SASL_PASSWORD=dkwci-test-password
ENV
