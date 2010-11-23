package com.twitter.gizzmo

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.ShardId

class PutParser(forwarding: Long => TestShard) extends JsonJobParser[JsonJob] {
  def apply(map: Map[String, Any]): JsonJob = {
    new PutJob(map("key").asInstanceOf[Int], map("value").asInstanceOf[String], forwarding)
  }
}

class PutJob(key: Int, value: String, forwarding: Long => TestShard) extends JsonJob {
  def toMap = Map("key" -> key, "value" -> value)
  def apply() { forwarding(key).put(key, value) }
}

class TestCopyFactory(ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJobFactory[TestShard] {
  def apply(src: ShardId, dest: ShardId) = new TestCopy(src, dest, 0, 500, ns, s)
}

class TestCopyParser(ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJobParser[TestShard] {
  def deserialize(m: Map[String, Any], src: ShardId, dest: ShardId, count: Int) = {
    val cursor = m("cursor").asInstanceOf[Int]
    val count  = m("count").asInstanceOf[Int]
    new TestCopy(src, dest, cursor, count, ns, s)
  }
}

class TestCopy(srcId: ShardId, destId: ShardId, cursor: Int, count: Int,
               ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJob[TestShard](srcId, destId, count, ns, s) {
  def copyPage(src: TestShard, dest: TestShard, count: Int) = {
    val rows = src.getAll(cursor, count).map { case (k,v,c) => (k,v) }

    dest.putAll(rows)

    if (rows.isEmpty) None
    else Some(new TestCopy(srcId, destId, rows.last._1, count, ns, s))
  }

  def serialize = Map("cursor" -> cursor)
}
