package com.twitter.gizzmo

import java.sql.{ResultSet, SQLException}
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.config.{Connection => ConnectionConfig}
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.gizzard.shards._

// Shard Definitions

trait TestShard extends Shard {
  def put(key: Int, value: String): Unit
  def putAll(kvs: Seq[(Int, String)]): Unit
  def get(key: Int): Option[(Int, String, Int)]
  def getAll(key: Int, count: Int): Seq[(Int, String, Int)]
}

class TestReadWriteAdapter(s: ReadWriteShard[TestShard]) extends ReadWriteShardAdapter(s) with TestShard {
  def put(k: Int, v: String)         = s.writeOperation(_.put(k,v))
  def putAll(kvs: Seq[(Int,String)]) = s.writeOperation(_.putAll(kvs))
  def get(k: Int)                    = s.readOperation(_.get(k))
  def getAll(k:Int, c: Int)          = s.readOperation(_.getAll(k,c))
}

class SqlShardFactory(qeFactory: QueryEvaluatorFactory, conn: ConnectionConfig)
extends ShardFactory[TestShard] {

  def instantiate(info: ShardInfo, weight: Int, children: Seq[TestShard]) =
    new SqlShard(qeFactory(conn.withHost(info.hostname)), info, weight, children)

  def materialize(info: ShardInfo) {
    val ddl =
      """create table if not exists %s (
           id int(11) not null,
           value varchar(255) not null,
           count int(11) not null default 1,
           primary key (id)
         ) engine=innodb default charset=utf8"""
    try {
      val e = qeFactory(conn.withHost(info.hostname).withoutDatabase)
      e.execute("create database if not exists " + conn.database)
      e.execute(ddl.format(conn.database + "." + info.tablePrefix))
    } catch {
      case e: SQLException             => throw new ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new ShardTimeoutException(e.timeout, info.id)
    }
  }
}

class SqlShard(
  evaluator: QueryEvaluator,
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[TestShard])
extends TestShard {
  private val table = shardInfo.tablePrefix

  private val putSql = """insert into %s (id, value, count) values (?,?,1) on duplicate key
                          update value = values(value), count = count+1""".format(table)
  private val getSql    = "select * from " + table + " where id = ?"
  private val getAllSql = "select * from " + table + " where id > ? limit ?"

  private def asResult(r: ResultSet) = (r.getInt("id"), r.getString("value"), r.getInt("count"))

  def put(key: Int, value: String) { evaluator.execute(putSql, key, value) }
  def putAll(kvs: Seq[(Int, String)]) {
    evaluator.executeBatch(putSql) { b => for ((k,v) <- kvs) b(k,v) }
  }

  def get(key: Int) = evaluator.selectOne(getSql, key)(asResult)
  def getAll(key: Int, count: Int) = evaluator.select(getSql, key, count)(asResult)
}
