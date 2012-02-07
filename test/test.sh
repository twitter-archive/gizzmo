#!/bin/bash
cd `dirname $0`
function g {
  ../bin/gizzmo -Cconfig.yaml "$@" 2>&1
}

function g_silent {
  ../bin/gizzmo -Cconfig.yaml "$@" > /dev/null
}

function expect {
  diff -U3 "expected/$1" - && echo -e "    success\t$1." || (echo -e "    failed\t$1." && exit 1)
}

function expect-string {
  echo "$1" > expected/tmp
  expect tmp
}

TABLE=13
REPLICATING_SHARD_CLASS="com.twitter.gizzard.shards.ReplicatingShard"
BLOCKED_SHARD_CLASS="com.twitter.gizzard.shards.BlockedShard"

function shard_id {
  printf "localhost/base_%d_%03d_%s" $TABLE $1 $2
}

function cleanup {
  for shard in `g find -hlocalhost`; do
    # links
    for linktuple in `g links $shard | awk '{print $1","$2}'`; do
      g_silent unlink `echo $linktuple | awk -F, '{print $1" "$2}'`
    done
    # shards
    g_silent delete $shard
  done
  g find -hlocalhost | expect empty-file.txt
}

function initialize {
  for i in {0..9}; do
    REPLICATING_SHARD=$(shard_id $i "replicating")
    g_silent create $REPLICATING_SHARD_CLASS $REPLICATING_SHARD
    g_silent create TestShard $(shard_id $i "a") --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
    g_silent create TestShard $(shard_id $i "b") --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
    g_silent addlink $REPLICATING_SHARD $(shard_id $i "a") 2
    g_silent addlink $REPLICATING_SHARD $(shard_id $i "b")  1
    g_silent addforwarding $TABLE `date +%s` $REPLICATING_SHARD
  done
}

cleanup
initialize

for i in `g find -h localhost`; do g info $i; done | expect info.txt
g find -hlocalhost | expect original-find.txt
g find -hlocalhost -tTestShard | expect find-only-sql-shard-type.txt

# execute a ping (we're connected to "two" identical hosts, so this only tests success)
g ping | expect empty-file.txt

function simple_transform {
  g -T $TABLE transform \
      "$REPLICATING_SHARD_CLASS(1) -> (TestShard(localhost,2,INT UNSIGNED,INT UNSIGNED), TestShard(localhost,1,INT UNSIGNED,INT UNSIGNED))" \
      "$REPLICATING_SHARD_CLASS(1) -> (TestShard(localhost,2,INT UNSIGNED,INT UNSIGNED)"
}

{ # test-busy-transform
  g_silent markbusy $(shard_id 3 "a")
  simple_transform | expect busy-transform-shard.txt
  g_silent markunbusy $(shard_id 3 "a")
}

{ # test-blocked-transform
  g_silent wrap $BLOCKED_SHARD_CLASS $(shard_id 3 "a")
  simple_transform | expect blocked-transform-shard.txt
  g_silent unwrap $(shard_id 3 "a_blocked")
}

# FIXME: remaining tests are out of date: see DATASERV-83
###############################################################################
exit
###############################################################################

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
