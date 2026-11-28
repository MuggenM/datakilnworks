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

echo "a failing seed is reported with its exit code"
bash $S/down.sh >/dev/null 2>&1
cat > /tmp/dkwci_badseed.yml <<'EOF'
services:
  lldap-seed:
    entrypoint: ["/bin/sh", "-c", "exit 3"]
    command: []
EOF
out6="$(CI_SERVICES_EXTRA_COMPOSE=/tmp/dkwci_badseed.yml bash $S/up.sh ldap 2>&1)"; rc6=$?; rm -f /tmp/dkwci_badseed.yml
[ $rc6 -ne 0 ] && echo "$out6" | grep -q "seed lldap-seed exited with 3"; check "up.sh exits non-zero naming the seed and its code 3" $? "$(echo "$out6" | grep -m1 'did not come up')"

echo "CI_SERVICES_PUBLISH: loopback only, and a bad entry changes nothing"
bash $S/down.sh >/dev/null 2>&1
CI_SERVICES_PUBLISH="lldap:0.0.0.0:17170" bash $S/up.sh ldap >/dev/null 2>&1; rc7=$?
[ $rc7 -eq 2 ] && [ ! -e $S/out/.lock ] && [ -z "$(docker ps -aq --filter label=com.docker.compose.project=dkwci)" ]
check "an entry that tries to name a bind address is refused before anything starts (no lock, no container)" $?
CI_SERVICES_PUBLISH="lldap:18917:17170" bash $S/up.sh ldap >/dev/null 2>&1; check "a valid entry starts the stack" $?
ports="$(docker ps --filter label=com.docker.compose.project=dkwci --format '{{.Ports}}' | tr ',' '\n' | grep -- '->' | sed 's/^ *//')"
[ "$ports" = "127.0.0.1:18917->17170/tcp" ]; check "the only published port is 127.0.0.1:18917" $? "$ports"
curl -sf -o /dev/null http://127.0.0.1:18917/; check "...and it answers on the host" $?
bash $S/down.sh >/dev/null 2>&1; [ ! -e $S/out/publish.yml ]; check "down.sh removes the override" $?

echo "a refused run_test.sh leaves the running stack alone"
bash $S/down.sh >/dev/null 2>&1; bash $S/up.sh ldap >/dev/null 2>&1
CI_SERVICES=ldap bash ci/run_test.sh ci_services_wiring >/dev/null 2>&1; rc5=$?
[ $rc5 -ne 0 ]; check "run_test.sh exits non-zero while the stack is already up" $?
docker inspect -f '{{.State.Running}}' "$(docker compose -p dkwci ps -q lldap)" 2>/dev/null | grep -q true
check "...and the stack it did not start is still running" $?

echo "secrets_not_in_output: no generated or dummy secret appears in what CI prints"
bash $S/down.sh >/dev/null 2>&1
outU="$(bash $S/up.sh ldap git kafka azure s3 sftp idp 2>&1)"; check "up.sh with every profile exits 0" $? "$(echo "$outU" | tail -3)"
outT="$(CI_SERVICES=git CI_SERVICES_UP=0 CI_SERVICES_KEEP=1 CI_TEST_ENV=GIT_HOST=127.0.0.1:9 bash ci/run_test.sh git_sync 2>&1)"; rcT=$?
[ $rcT -ne 0 ] && echo "$outT" | grep -q "\[FAIL\] Gitea not reachable at 127.0.0.1:9"; check "git_sync against an unreachable Gitea fails (REQUIRE_SERVICES=1)" $? "$(echo "$outT" | tail -3)"
tf="$(mktemp)"; echo "$outT" > "$tf"; outA="$(python3 ci/annotate.py "$tf" test_git_sync 2>&1)"; rm -f "$tf"
outL="$(docker compose -p dkwci logs --no-color --tail 200 2>&1)"          # the workflow's failure step prints exactly this
secrets="$(grep -hE '^[A-Z0-9_]*(TOK|PASSWORD|SECRET)[A-Z0-9_]*=' $S/out/*.env | cut -d= -f2-; sed -n 2p $S/out/sftp/id)"
n=0; leaks=""
while IFS= read -r v; do
  [ -n "$v" ] || continue; n=$((n + 1))
  for part in up:"$outU" test:"$outT" annotation:"$outA" logs:"$outL"; do
    case "${part#*:}" in *"$v"*) leaks="$leaks ${part%%:*}:secret#$n";; esac
  done
done <<< "$secrets"
[ $n -ge 6 ]; check "the env files hold the expected secrets ($n)" $?
[ -z "$leaks" ]; check "none of them is in up.sh's output, the test's output, its annotation or the services' logs" $? "$leaks"

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
