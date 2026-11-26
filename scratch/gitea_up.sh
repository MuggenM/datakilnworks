#!/bin/sh
# A THROWAWAY Gitea for the git tests (never your own): network gtest_net, container gtest_gitea, user ga, repos ga/{dbt-rv,nb-rv,dbt-fg}.
# Prints TOK=<token>. Remove with: docker rm -f gtest_gitea; docker network rm gtest_net
docker network create gtest_net >/dev/null 2>&1
docker rm -f gtest_gitea >/dev/null 2>&1
docker run -d --name gtest_gitea --network gtest_net -e GITEA__security__INSTALL_LOCK=true -e GITEA__server__ROOT_URL=http://gtest_gitea:3000 \
  -e GITEA__server__DOMAIN=gtest_gitea -e GITEA__service__DISABLE_REGISTRATION=true gitea/gitea:1.22-rootless >/dev/null
for i in $(seq 60); do docker exec gtest_gitea curl -sf localhost:3000/api/healthz >/dev/null 2>&1 && break; sleep 2; done
docker exec gtest_gitea gitea admin user create --username ga --password gapassword123 --email ga@example.org --admin --must-change-password=false >/dev/null 2>&1
TOK=$(docker exec gtest_gitea gitea admin user generate-access-token --username ga --scopes all --raw 2>/dev/null | tail -1)
for r in dbt-rv nb-rv dbt-fg; do
  docker exec gtest_gitea curl -s -X POST localhost:3000/api/v1/user/repos -H "Authorization: token $TOK" -H 'Content-Type: application/json' -d "{\"name\":\"$r\",\"private\":false}" >/dev/null
done
echo "TOK=$TOK"
