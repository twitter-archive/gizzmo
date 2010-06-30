#!/bin/bash
cd `dirname $0`
function g {
  # echo "> g $@" >&2
  ../bin/gizzmo -Cconfig.yaml "$@" 2>&1
}
function expect {
  diff -u - "expected/$1" && echo "    success." || echo "    failed." && exit 1
}

# set -ex

if ["$FLOCK_ENV" -eq ""]; then
  FLOCK_ENV=development
fi

for i in 1 2
do
  for type in edges groups
  do
    db="flock_${type}_${FLOCK_ENV}_${i}"
    echo "drop database if exists $db; create database $db; " | mysql -u"$DB_USERNAME" --password="$DB_PASSWORD"
    cat recreate.sql | mysql -u"$DB_USERNAME" --password="$DB_PASSWORD" "$db"
  done
done

for i in {0..9}
do
  g create localhost "table_repl_$i" com.twitter.service.flock.edges.ReplicatingShard
  g create localhost "table_a_$i" com.twitter.service.flock.edges.SqlShard --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g create localhost "table_b_$i" com.twitter.service.flock.edges.SqlShard --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g addlink "localhost/table_repl_$i" "localhost/table_a_$i" 2
  g addlink "localhost/table_repl_$i" "localhost/table_b_$i" 1
done

for i in `g find -h localhost`; do g info $i; done | expect info.txt
g find -hlocalhost | expect original-find.txt
g find -hlocalhost -tSqlShard | expect find-only-sql-shard-type.txt

# Dry run this

g -D wrap com.twitter.service.flock.edges.ReplicatingShard localhost/table_b_0 | expect dry-wrap-table_b_0.txt
g wrap com.twitter.service.flock.edges.ReplicatingShard localhost/table_b_0 | expect wrap-table_b_0.txt
g wrap com.twitter.service.flock.edges.ReplicatingShard localhost/table_b_0 | expect wrap-table_b_0.txt
g links localhost/table_b_0 | expect links-for-table_b_0.txt
g links localhost/table_repl_0 | expect links-for-table_repl_0.txt
g links localhost/replicating_table_b_0 | expect links-for-replicating_table_b_0.txt

g unwrap localhost/replicating_table_b_0 | expect unwrapped-replicating_table_b_0.txt
g links localhost/table_b_0 | expect unwrapped-table_b_0.txt

g unlink localhost/table_repl_0 localhost/table_b_0 | expect empty-file.txt
g links localhost/table_b_0 | expect empty-file.txt

g wrap com.twitter.gizzard.shards.BlockedShard localhost/table_a_3
g find -hlocalhost | xargs ../bin/gizzmo -Cconfig.yaml subtree 2>&1 | expect subtree.txt

g find -hlocalhost | xargs ../bin/gizzmo -Cconfig.yaml delete
g find -hlocalhost | expect empty-file.txt

