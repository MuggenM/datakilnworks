#!/usr/bin/env bash
# Host test of the services stack itself (ci/services/up.sh, down.sh): start, seed, refuse a second start, time out on an
# unhealthy service, leave nothing behind, and never touch containers or networks outside compose project dkwci.
#   bash ci/services/test_services_harness.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
S=ci/services
FAILS=0
check() { if [ "$2" = 0 ]; then echo "  [PASS] $1"; else echo "  [FAIL] $1${3:+ -> $3}"; FAILS=$((FAILS + 1)); fi; }
others() { { docker ps -a --format '{{.Names}}'; docker network ls --format '{{.Name}}'; docker volume ls --format '{{.Name}}'; } | grep -v '^dkwci' | sort; }

bash $S/down.sh >/dev/null 2>&1
before="$(others)"

echo "up and seeded"
out="$(bash $S/up.sh ldap 2>&1)"; rc=$?
check "up.sh ldap exits 0" $rc "$(echo "$out" | tail -5)"
[ -f $S/out/ldap.env ]; check "out/ldap.env is written" $?
set -a; . $S/out/ldap.env 2>/dev/null; set +a
search() {   # the entry of uid=$1 with its groups, through the stack's network
  docker run --rm --network dkwci_default --dns-search . alpine:3.22 sh -c "apk add -q openldap-clients >/dev/null 2>&1 && ldapsearch -x -H ldap://${LLDAP_TEST_HOST:-lldap}:3890 \
    -D '${LLDAP_BIND_DN:-}' -w '${LLDAP_BIND_PASSWORD:-}' -b 'ou=people,${LLDAP_BASE_DN:-}' '(uid=$1)' memberOf" 2>&1
}
r="$(search jdoe)"
echo "$r" | grep -q "^dn: uid=jdoe,ou=people,dc=example,dc=com"; check "jdoe exists" $? "$(echo "$r" | tail -3)"
echo "$r" | grep -q "memberOf: cn=dkw_engineers,ou=groups,dc=example,dc=com"; check "jdoe is in dkw_engineers" $?
bind="$(docker run --rm --network dkwci_default --dns-search . alpine:3.22 sh -c "apk add -q openldap-clients >/dev/null 2>&1 && ldapwhoami -x -H ldap://lldap:3890 \
  -D uid=jdoe,ou=people,dc=example,dc=com -w '${LLDAP_USER_PASSWORD:-}'" 2>&1)"
echo "$bind" | grep -q "dn:uid=jdoe"; check "jdoe can bind with the seeded password" $? "$bind"

# A search domain inherited from the host (this dev machine's resolves every unknown name to an outside host) must not apply:
# curl in a seed tried lldap.<search domain> first and connected to that outside host instead of the stack's lldap.
sd="$(docker compose -p dkwci exec -T lldap sh -c 'grep ^search /etc/resolv.conf' 2>/dev/null)"
[ -z "$sd" ]; check "the stack's containers have no DNS search domain" $? "$sd"

echo "second up refuses while the stack is up"
out2="$(bash $S/up.sh ldap 2>&1)"; rc2=$?
[ $rc2 -ne 0 ] && echo "$out2" | grep -q "dkwci is already running"; check "second up.sh exits 1 with 'dkwci is already running'" $? "$out2"
docker inspect -f '{{.State.Running}}' "$(docker compose -p dkwci ps -q lldap)" 2>/dev/null | grep -q true
check "the first stack is still running" $?

echo "a leftover volume from a crashed run is not reused"
docker compose -p dkwci exec -T lldap sh -c "curl -s -X POST localhost:17170/auth/simple/login -H 'Content-Type: application/json' \
  -d '{\"username\":\"admin\",\"password\":\"${LLDAP_BIND_PASSWORD:-}\"}'" > /tmp/dkwci_tok.json 2>/dev/null
tok="$(sed -n 's/.*"token":"\([^"]*\)".*/\1/p' /tmp/dkwci_tok.json)"; rm -f /tmp/dkwci_tok.json
docker compose -p dkwci exec -T lldap sh -c "curl -s localhost:17170/api/graphql -H 'Authorization: Bearer $tok' -H 'Content-Type: application/json' \
  -d '{\"query\":\"mutation{deleteUser(userId:\\\"jdoe\\\"){ok}}\"}'" >/dev/null 2>&1
search jdoe | grep -q "^dn: uid=jdoe"; [ $? -ne 0 ]; check "jdoe deleted (setup)" $?
docker compose -p dkwci stop >/dev/null 2>&1; rm -f $S/out/.lock          # a crash: containers stopped, volume and no lock left
bash $S/up.sh ldap >/dev/null 2>&1; check "up.sh after a crash exits 0" $?
r2="$(search jdoe)"; echo "$r2" | grep -q "^dn: uid=jdoe"; check "jdoe exists again (started from empty volumes)" $? "$(echo "$r2" | tail -4)"

echo "ci/run_test.sh with CI_SERVICES joins the stack"
bash $S/down.sh >/dev/null 2>&1
probe=scratch/test_zz_services_probe.py
cat > $probe <<'PY'
import os, socket
ok = os.environ.get("REQUIRE_SERVICES") == "1" and os.environ.get("LLDAP_TEST_HOST") == "lldap"
socket.create_connection(("lldap", 3890), timeout=5).close()
search = [l for l in open("/etc/resolv.conf") if l.startswith("search")]
print("  [PASS] probe" if ok and not search else f"  [FAIL] probe env={ok} search={search}")
PY
out4="$(CI_SERVICES=ldap bash ci/run_test.sh zz_services_probe 2>&1)"; rc4=$?; rm -f $probe
[ $rc4 -eq 0 ] && echo "$out4" | grep -q "\[PASS\] probe"; check "the test ran on the stack with its env file and REQUIRE_SERVICES=1" $? "$(echo "$out4" | tail -4)"
left4="$(docker ps -a --format '{{.Names}}' | grep '^dkwci')"
[ -z "$left4" ]; check "run_test.sh stopped the stack afterwards" $? "$left4"

echo "an unhealthy service times out"
bash $S/down.sh >/dev/null 2>&1
cat > /tmp/dkwci_unhealthy.yml <<'EOF'
services:
  lldap:
    healthcheck:
      test: ["CMD", "false"]
      interval: 1s
      retries: 1
EOF
t0=$(date +%s)
out3="$(CI_SERVICES_TIMEOUT=5 CI_SERVICES_EXTRA_COMPOSE=/tmp/dkwci_unhealthy.yml bash $S/up.sh ldap 2>&1)"; rc3=$?
t1=$(date +%s); rm -f /tmp/dkwci_unhealthy.yml
[ $rc3 -ne 0 ]; check "up.sh exits non-zero" $?
[ $((t1 - t0)) -le 60 ]; check "within 60 s (took $((t1 - t0)) s)" $?
echo "$out3" | grep -q "lldap"; check "it prints lldap's log lines" $? "$(echo "$out3" | tail -3)"

echo "down leaves nothing"
bash $S/down.sh >/dev/null 2>&1
left="$( { docker ps -a --format '{{.Names}}'; docker network ls --format '{{.Name}}'; docker volume ls --format '{{.Name}}'; } | grep '^dkwci' )"
[ -z "$left" ]; check "no dkwci container, network or volume remains" $? "$left"
[ ! -e $S/out/.lock ]; check "no lock file remains" $?
bash $S/down.sh >/dev/null 2>&1; check "down.sh twice is fine" $?
after="$(others)"
[ "$before" = "$after" ]; check "nothing outside dkwci was touched" $? "$(diff <(echo "$before") <(echo "$after") | head -5)"

echo
if [ $FAILS -ne 0 ]; then echo "$FAILS FAILED"; exit 1; fi
echo "ALL PASS"
