#!/bin/bash
cd `dirname $0`
function g {
  # echo "> g $@" >&2
  ../bin/gizzmo -Cconfig.yaml "$@" 2>&1
}

function g_silent {
  g "$@" > /dev/null
}

function expect {
  diff -u - "expected/$1" && echo -e "    success\t$1." || (echo -e "    failed\t$1." && exit 1)
}

function expect-string {
  echo "$1" > expected/tmp
  expect tmp
}

for shard in `g find -hlocalhost`; do
    for linkline in `g links $shard | awk '{print $1","$2}'`; do
        g_silent unlink `echo $linkline | awk -F, '{print $1" "$2}'`
    done
    g_silent delete $shard
done
g find -hlocalhost | expect empty-file.txt

for i in {0..9}
do
  g_silent create com.twitter.gizzard.shards.ReplicatingShard localhost/table_repl_$i
  g_silent create TestShard localhost/table_a_$i --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g_silent create TestShard localhost/table_b_$i --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g_silent addlink "localhost/table_repl_$i" "localhost/table_a_$i" 2
  g_silent addlink "localhost/table_repl_$i" "localhost/table_b_$i" 1
done

for i in `g find -h localhost`; do g info $i; done | expect info.txt
g find -hlocalhost | expect original-find.txt
g find -hlocalhost -tTestShard | expect find-only-sql-shard-type.txt


NOW=`date +%s` # unix timestamp
g addforwarding 13 $NOW localhost/table_a_3

g forwardings | egrep "13.$NOW.localhost/table_a_3" 
if [ $? -ne 0 ]; then
  echo "    failed."
  exit 1
fi

# g unforward 1 0 localhost/table_a_3

g -D wrap com.twitter.gizzard.shards.ReplicatingShard localhost/table_b_0 | expect dry-wrap-table_b_0.txt
g wrap com.twitter.gizzard.shards.ReplicatingShard localhost/table_b_0 | expect wrap-table_b_0.txt
g wrap com.twitter.gizzard.shards.ReplicatingShard localhost/table_b_0 | expect wrap-table_b_0.txt
g links localhost/table_b_0 | expect links-for-table_b_0.txt
g links localhost/table_repl_0 | expect links-for-table_repl_0.txt
g links localhost/replicating_table_b_0 | expect links-for-replicating_table_b_0.txt

g --subtree --info find -Hlocalhost | expect subtree-info.txt

g lookup 1 100 | expect-string "localhost/forward_1"
g lookup --fnv 1 "hello" | expect-string "localhost/forward_1"

g unwrap localhost/replicating_table_b_0 | expect unwrapped-replicating_table_b_0.txt
g links localhost/table_b_0 | expect unwrapped-table_b_0.txt

g unlink localhost/table_repl_0 localhost/table_b_0 | expect empty-file.txt
g links localhost/table_b_0 | expect empty-file.txt

g wrap com.twitter.gizzard.shards.BlockedShard localhost/table_a_3
g find -hlocalhost | xargs ../bin/gizzmo -Cconfig.yaml subtree 2>&1 | expect subtree.txt
g find -hlocalhost | ../bin/gizzmo -Cconfig.yaml subtree 2>&1 | expect subtree.txt

# test a deep tree
g create localhost "table_deep_repl_0" com.twitter.gizzard.shards.ReplicatingShard
for i in {1..9}
do
  last=$((i-1))
  g create localhost "table_deep_repl_$i" com.twitter.gizzard.shards.ReplicatingShard
  g addlink "localhost/table_deep_repl_$last" "localhost/table_deep_repl_$i" 2
done

g subtree localhost/table_deep_repl_5 | expect deep.txt

g flush --all
g flush 1
