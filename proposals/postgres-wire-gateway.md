# Proposal: PostgreSQL-wire-protocol gateway for DKW (Metabase/Superset/psql compatible)

## 1. What this is

A new, optional network listener (`PG_WIRE_ENABLED`, off by default, like Delta Sharing's `DELTA_SHARING=off` or the Traefik profile) that speaks enough of the PostgreSQL wire protocol for `psql`, Metabase, Superset, and any other Postgres-JDBC/libpq-based tool to connect, authenticate as a real DKW user, and run read-only SQL -- through the exact same governance gateway every other DKW entry point uses. This closes the gap identified when evaluating Metabase/Superset connectivity: both tools can already reach DuckDB directly (via a community JDBC driver or `duckdb-engine`), but that bypasses masking, row filters and catalog ACLs entirely. This gateway is the governed alternative.

## 2. Protocol foundation: adopt, don't reimplement

Hand-rolling the PostgreSQL wire protocol (startup/SSL negotiation, SCRAM/cleartext auth exchange, simple *and* extended query sub-protocols, `RowDescription`/`DataRow`/`CommandComplete`/`ErrorResponse` framing) is a large, correctness-sensitive undertaking where the only grading is "does a real, unmodified Metabase/Superset/psql actually work against it" -- exactly the kind of thing this project has consistently chosen to wrap an existing implementation for rather than reimplement (`python3-saml`, `py_webauthn`, `fabric-emulator`, `duckrun` itself). The concrete candidate is **buenavista** (a small, pure-Python PostgreSQL-wire server built specifically to front DuckDB) -- if it covers enough of the protocol, DKW's own code sits entirely above it: an auth hook and a query-execution hook, nothing lower-level. Evaluating it (or the alternative `pg-gateway`, which has first-class SASL/SCRAM and TLS hooks) against a real `psql` and a real Metabase instance is the first implementation step, before any DKW-specific code is written -- this needs to be verified directly, the same way fabric-emulator was, not assumed from documentation.

A genuinely useful side effect of DuckDB being the engine underneath: DuckDB already implements a good amount of `information_schema.tables`/`columns` and `pg_catalog` compatibility itself, specifically because this exact "let Postgres-wire tools work against DuckDB" problem is well known in its community. That means schema introspection -- the unglamorous but essential part BI tools actually depend on (they browse `pg_catalog`, not just run user SQL) -- is largely inherited for free, not something DKW needs to hand-build. This needs verifying against real Metabase/Superset introspection queries, not assumed.

## 3. Authentication: a new, small, generically useful prerequisite

The protocol only has two usable credential shapes: a username + a startup `database` parameter, then either a cleartext or SCRAM password exchange. There is no interactive step -- **a second factor cannot be presented over this protocol**, which creates a real conflict with every MFA-covered account in this codebase (`web/mfa_policy.py`). Checking only a local password hash here would silently let an MFA-covered account skip its own policy through a side door -- unacceptable given how much weight this project puts on that gate elsewhere.

The resolution, matching how GitHub/GitLab/every serious system handles exactly this, is **personal access tokens** -- which DKW does not have today and is worth building as its own small, standing feature (useful beyond this one gateway, e.g. future CLI/scripting use):

- A user mints their own token from an authenticated, already-MFA-verified web session (`POST /api/auth/tokens`): `secrets.token_urlsafe(32)` prefixed `dkw_pat_`, shown once, only its SHA-256 stored (same shape as `scim_tokens`, Delta Sharing recipient tokens, connection secrets). Revocable, listable, last-used timestamp, optional expiry.
- The token's existence *is* the MFA proof -- it could only have been minted by a session that already cleared MFA, exactly like a WebAuthn passkey or an OIDC/SAML session is itself the factor.
- At the wire protocol's password prompt: a value matching `dkw_pat_...` is verified as a PAT; anything else is checked as an ordinary password through `web/auth.py::verify_password` or `web/ldap_auth.py::authenticate` (bind-as-user) -- the exact same dispatch `/api/auth/login` already makes. An MFA-covered account presenting a plain password (not a PAT) is refused with a clear message pointing at PATs, mirroring how a passwordless account already gets the same clear refusal for a password attempt today.
- OIDC/SAML/passkey-only accounts have no DKW-verifiable password at all (`upsert_external_user` writes a random, never-checked hash) -- for these, a PAT is the *only* way in, which is an existing, accepted limitation elsewhere in this codebase, not a new one.

## 4. The query path: identical governance, not a parallel implementation

Once authenticated, the connection resolves to a real user dict -- exactly `gateway.principal_for(user)` -- and every query runs through the same two calls every other gateway in this codebase does: `enforce_sql_permissions(sql, user, action="READ")` then `gateway.govern_sql(sql, user, client="pg_wire")`, on a `get_duckrun_conn().con.cursor()` scoped per query (the same lightweight-cursor-on-the-shared-connection pattern as `sandbox_gateway.py` and `web/apps.py`). Masking, row filters and catalog ACLs apply identically to a Metabase dashboard as to someone typing the same query into the SQL editor. Every query is audited (`log_query(..., client="POSTGRES_WIRE", user=...)`). **v1 is read-only** (`action="READ"`) -- BI tools don't need writes, and it sidesteps a large amount of transaction-semantics complexity (`BEGIN`/`COMMIT`/prepared-statement write ordering) for no real gain yet.

**Catalog mapping is a clean fit, not a workaround**: PostgreSQL's two-level `database.schema.table` maps naturally onto DKW's `catalog.schema.table` -- the wire protocol's startup `database` parameter selects the DKW *catalog* (`psql -h ... -d sales` -> the `sales` catalog), and schema/table underneath map through unchanged.

**Catalog-level ACLs must prune introspection, not just query results**: a user with no grant on a catalog should not see its schemas/tables in `pg_catalog`/`information_schema` either -- reusing `filter_catalogs_for_user`'s existing logic (today used by the catalog explorer) against whatever `database` the connection selected, refusing the whole connection at startup if the user has no access to it at all.

## 5. What a second TCP listener loses -- and must rebuild

This is the part worth stating plainly rather than glossing over: a raw TCP listener on a new port sits **entirely outside every HTTP middleware** this project already has (`_ip_allowlist_gate`, `_network_policy_gate`, `_mfa_policy_gate`, `_must_change_password_gate`). None of that is inherited for free. Each invariant that matters here has to be deliberately re-implemented at the protocol layer, not assumed:

- IP allowlist + per-user/per-role network policy: call `ip_allowlist.resolve_client`/`check_user_network` against the raw socket peer before even starting the handshake.
- `must_change_password` / deactivated / deleted account: checked at the same point the password/PAT is verified.
- MFA policy: solved structurally by the PAT design above, not re-checked per query.

**TLS is mandatory, not optional.** The protocol's `SSLRequest` negotiation is the only way a password (or PAT) is ever safe to send; a connection that declines TLS must be refused before any credential is read, never silently downgraded. Certificate handling follows the SAML SP-keypair precedent exactly: auto-generate a self-signed cert on first use (`.metadata/pg_wire.{key,crt}`) so there's nothing to provision by default, with the existing optional Traefik proxy able to front it with a real certificate the same way it already can for the studio and Delta Sharing.

## 6. New surface area

- `web/pg_wire.py`: the asyncio TCP server (started as a background task in `startup_event`, same shape as every other daemon loop there), the auth hook, the query-execution hook, catalog-introspection filtering.
- `web/pat.py` (or folded into `web/auth.py`): personal access tokens -- table `access_tokens` in auth.db (id, user_id, token_hash, name, created_at, last_used_at, expires_at), `POST`/`GET`/`DELETE /api/auth/tokens`, a card in the user's own profile/settings.
- Env: `PG_WIRE_ENABLED` (default off), `PG_WIRE_PORT` (default 5433, avoiding collision with a real Postgres on 5432), published in `docker-compose.yml` only when enabled.

## 7. Explicitly out of scope (v1)

- Writes (`INSERT`/`UPDATE`/`CREATE TABLE AS`) and transaction semantics beyond what's needed for a read-only session.
- SSL client-certificate auth (password/PAT over mandatory TLS covers the real need).
- Routing a connection to a specific SQL Warehouse's compute node (v1 always runs in-process on the studio's own duckrun connection, like the SQL editor's default path) -- worth a phase 2 if warehouse-level isolation turns out to matter for this use case.

## 8. Testing

- `scratch/test_pg_wire.py`: real `psycopg`/`pg8000` client against a live instance -- auth (password, PAT, wrong credential, MFA-covered account refusing a plain password, deactivated/must-change-password accounts refused), TLS-required refusal, catalog ACL pruning both query results *and* introspection, masking/row-filter parity (the same real-Delta-table-plus-policy proof style as `scratch/test_apps.py`), audit log entries.
- `scratch/verify_pg_wire_metabase_ui.py` (or similar): a real Metabase container actually added as a data source and a dashboard built against a masked table, end to end -- the only way to honestly claim "Metabase works," matching this project's habit of verifying against the real external tool rather than a client library.

## 9. Effort

The honest center of gravity is step 2 (evaluating/adopting a wire-protocol library) and step 5 (TLS + the duplicated middleware surface) -- both need to be right before anything else matters, and both are the kind of thing that's only provable by testing against real clients, not by code review. Personal access tokens are small and independently useful regardless of how the rest lands. Overall this is comparable in size to the embedded-apps feature, maybe a bit larger given the protocol-correctness risk is external (a library we don't control) rather than internal.

## 10. Suggested next step

Start with step 2: stand up buenavista (or pg-gateway) against a throwaway DuckDB and a real `psql`/Metabase to confirm it's a viable foundation, before writing any DKW-specific code.
