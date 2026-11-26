#!/usr/bin/env python3
"""Groups in governance policies: `except_groups` on masking and row-filter policies, group-valued row-filter attributes, group deletion, and the
upgrade of an existing governance.db. Throwaway WAREHOUSE_DIR.
  docker run --rm -v $PWD/web:/workspace/web -v $PWD/scratch:/workspace/scratch localspark-lakehouse-notebook python /workspace/scratch/test_governance_groups.py"""
import os, shutil, sqlite3, sys, tempfile
TMP = tempfile.mkdtemp(prefix="govgroups_"); os.environ["WAREHOUSE_DIR"] = TMP
os.environ["INIT_ADMIN_USERNAME"] = "admin"; os.environ["INIT_ADMIN_PASSWORD_HASH"] = "pbkdf2_sha256$100000$" + "0" * 32 + "$" + "0" * 64
sys.path.insert(0, "/workspace")
os.makedirs(os.path.join(TMP, ".metadata"), exist_ok=True)

# an OLD governance.db (before groups): no except_groups columns, principal_attributes CHECK without 'group', with data in it
old = sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db"))
old.executescript("""
CREATE TABLE governance_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE tag_definitions (tag_key TEXT PRIMARY KEY, description TEXT, allowed_values TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE masking_policies (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT, tag_key TEXT NOT NULL, tag_value TEXT, mask_type TEXT NOT NULL,
    mask_expr TEXT, applies_to_types TEXT, except_roles TEXT NOT NULL DEFAULT '["admin"]', except_users TEXT NOT NULL DEFAULT '[]', priority INTEGER NOT NULL DEFAULT 100,
    enabled INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE principal_attributes (id INTEGER PRIMARY KEY AUTOINCREMENT, principal_type TEXT NOT NULL CHECK (principal_type IN ('user','role')),
    principal_value TEXT NOT NULL, attribute_key TEXT NOT NULL, attribute_value TEXT NOT NULL, created_by TEXT NOT NULL, created_at TEXT NOT NULL,
    UNIQUE (principal_type, principal_value, attribute_key, attribute_value));
INSERT INTO masking_policies VALUES ('pol_old','Old policy',NULL,'pii',NULL,'redact',NULL,NULL,'["admin"]','[]',100,1,'admin','2026-01-01','2026-01-01');
INSERT INTO principal_attributes (principal_type, principal_value, attribute_key, attribute_value, created_by, created_at) VALUES ('user','alice','region','US','admin','2026-01-01');
""")
old.commit(); old.close()

from web import auth, groups
from web.governance import gateway, policies, row_filters, store, tags
from web.governance.policies import Principal
FAIL = []
def check(n, c, d=""):
    print(f"  [{'PASS' if c else 'FAIL'}] {n}" + (f" -> {str(d)[:300]}" if d and not c else ""))
    if not c: FAIL.append(n)
def raises(fn, *a, **k):
    try: fn(*a, **k); return False
    except ValueError: return True

print("upgrade of an existing database")
store.init_governance_db()
c = sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db")); c.row_factory = sqlite3.Row
check("except_groups columns were added", all("except_groups" in {r[1] for r in c.execute(f"PRAGMA table_info({t})")} for t in ("masking_policies", "row_policies")))
check("the old policy and attribute survived", c.execute("select count(*) from masking_policies").fetchone()[0] == 1 and c.execute("select count(*) from principal_attributes").fetchone()[0] == 1)
check("the old policy reads back with an empty group list", policies.get_policy("pol_old")["except_groups"] == [])
store.init_governance_db(); check("upgrading twice is harmless", c.execute("select count(*) from principal_attributes").fetchone()[0] == 1)
c.close()

admin = auth.get_user_by_username("admin")
alice = auth.create_user("alice", "alicepass123", "Alice", "user"); bob = auth.create_user("bob", "bobpass1234", "Bob", "user")
ldapu = auth.upsert_external_user("carol", "Carol", "user", "ldap")
G = groups.create_group("PII readers", "", "admin"); H = groups.create_group("EMEA team", "", "admin"); K = groups.create_group("APAC team", "", "admin")
groups.add_members(G["id"], [alice["id"], ldapu["id"]], "admin"); groups.add_members(H["id"], [alice["id"]], "admin"); groups.add_members(K["id"], [alice["id"]], "admin")
P = lambda u: Principal.from_user(u)
check("a Principal carries its group ids", P(alice).groups == {G["id"], H["id"], K["id"]} and P(bob).groups == frozenset())
check("the anonymous / LLM principals never have groups", not gateway.ANONYMOUS.groups and not gateway.LLM_CONTEXT.groups)

policies.delete_policy("pol_old", "admin")          # (the migrated legacy policy masks pii for everyone; it would mask alice too)
print("masking: exemption by group")
tags.set_tag(catalog="warehouse", schema_name="hr", table_name="people", column_name="email", tag_key="pii", tag_value="email")
cols = [{"column": "email", "type": "VARCHAR"}]
pol = policies.create_policy({"name": "Mask PII", "tag_key": "pii", "mask_type": "redact", "except_roles": ["admin"]}, "admin")
masked = lambda u: bool(policies.masks_for_table("warehouse", "hr", "people", cols, P(u)))
check("without a group exemption everyone but admin is masked", masked(alice) and masked(bob) and masked(ldapu) and not masked(admin))
check("...and alice/bob are subjects of the policy", gateway.is_subject(P(alice)) and gateway.is_subject(P(bob)))
policies.update_policy(pol["id"], {"except_groups": [G["id"]]}, "admin")
check("exempting a group unmasks its members (local and external)", not masked(alice) and not masked(ldapu))
check("...and only them", masked(bob))
check("an exempt member is no longer a subject of that policy", not gateway.is_subject(P(alice)) and gateway.is_subject(P(bob)))
groups.remove_member(G["id"], "alice", "admin")
check("leaving the group ends the exemption at once", masked(alice) and not masked(ldapu))
groups.add_members(G["id"], [alice["id"]], "admin")
check("an unknown group id is refused", raises(policies.update_policy, pol["id"], {"except_groups": ["grp_nope"]}, "admin"))
check("except_groups must be a list", raises(policies.update_policy, pol["id"], {"except_groups": "grp_x"}, "admin"))
check("group exemption is stored and audited", policies.get_policy(pol["id"])["except_groups"] == [G["id"]] and
      any("POLICY_UPDATE" == r[0] for r in sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db")).execute("select action from governance_audit")))

print("row filters: exemption by group")
if "region_scoped" not in [t["tag_key"] for t in tags.list_definitions()]:
    tags.create_definition("region_scoped", "scope")
tags.set_tag(catalog="warehouse", schema_name="hr", table_name="people", tag_key="region_scoped", tag_value="")
rcols = [{"column": "region", "type": "VARCHAR"}]
rp = row_filters.create_row_policy({"name": "Region filter", "tag_key": "region_scoped", "filter_column": "region", "filter_mode": "attribute", "attribute_key": "region", "except_roles": ["admin"]}, "admin")
preds = lambda u: [f.predicate for f in row_filters.filters_for_table("warehouse", "hr", "people", rcols, P(u))]
check("nothing assigned: fail closed for everyone but admin", preds(bob) == ["1 = 0"] and preds(alice) == ["\"region\" IN ('US')"], (preds(bob), preds(alice)))

print("row filters: attributes carried by a group")
row_filters.set_attribute_values("group", H["id"], "region", ["EMEA"], "admin")
check("a group's values are added to its members' own", preds(alice) == ["\"region\" IN ('EMEA', 'US')"], preds(alice))
row_filters.set_attribute_values("group", K["id"], "region", ["APAC", "EMEA"], "admin")
check("several groups union their values (no duplicates)", preds(alice) == ["\"region\" IN ('APAC', 'EMEA', 'US')"], preds(alice))
check("non-members get nothing from a group's values", preds(bob) == ["1 = 0"])
check("an external (LDAP) user sees only what their groups give them", preds(ldapu) == ["1 = 0"])
groups.add_members(H["id"], [ldapu["id"]], "admin"); check("...after joining a group they get its values", preds(ldapu) == ["\"region\" IN ('EMEA')"], preds(ldapu))
check("a group attribute needs an existing group", raises(row_filters.set_attribute_values, "group", "grp_nope", "region", ["X"], "admin"))
check("attributes list shows the group principal", any(a["principal_type"] == "group" and a["principal_value"] == H["id"] for a in row_filters.list_attributes()))
row_filters.update_row_policy(rp["id"], {"except_groups": [G["id"]]}, "admin")
check("exempting a group from a row filter shows its members every row", preds(alice) == [] and preds(ldapu) == [], (preds(alice), preds(ldapu)))
check("...others are still filtered", preds(bob) == ["1 = 0"])

print("deleting a group")
groups.delete_group(G["id"], "admin")
check("the exemption disappears from both policy kinds (stricter, never looser)", policies.get_policy(pol["id"])["except_groups"] == [] and row_filters.get_row_policy(rp["id"])["except_groups"] == [])
check("...so its former members are masked / filtered again", masked(alice) and masked(ldapu) and preds(alice) != [])
groups.delete_group(H["id"], "admin")
check("its row-filter attributes are deleted with it", not any(a["principal_value"] == H["id"] for a in row_filters.list_attributes()) and preds(alice) == ["\"region\" IN ('APAC', 'EMEA', 'US')"])
groups.delete_group(K["id"], "admin")
check("and once the last group is gone only the user's own value remains", preds(alice) == ["\"region\" IN ('US')"], preds(alice))
acts = [r[0] for r in sqlite3.connect(os.path.join(TMP, ".metadata", "governance.db")).execute("select action from governance_audit")]
check("the cleanup is audited", "GROUP_REMOVED_FROM_POLICIES" in acts, set(acts))
shutil.rmtree(TMP, ignore_errors=True)
print("FAILED: " + ", ".join(FAIL) if FAIL else "ALL PASS"); sys.exit(1 if FAIL else 0)
