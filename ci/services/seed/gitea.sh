#!/bin/sh
# Seed of the git profile (runs in the gitea image against the server's own volumes): user ga, a token, empty repos.
# Runs as root only to write out/git.env; `gitea admin` runs as the server's own user so its database files stay its own.
set -e
as_git() { su git -s /bin/sh -c "$1"; }
as_git "gitea admin user create --username ga --password gapassword123 --email ga@example.org --admin --must-change-password=false" >/dev/null
TOK=$(as_git "gitea admin user generate-access-token --username ga --token-name dkwci --scopes all --raw" | tail -1)
# the union of what the git tests push to: test_git_sync, test_git_pr_mode, test_git_review, test_git_forges
for r in dbt-project notebooks dbt-pr nb-pr dbt-rv nb-rv dbt-fg; do
  curl -sf -X POST http://gitea:3000/api/v1/user/repos -H "Authorization: token $TOK" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$r\",\"private\":false}" >/dev/null
done
printf 'GIT_HOST=gitea:3000\nTOK=%s\n' "$TOK" > /out/git.env
