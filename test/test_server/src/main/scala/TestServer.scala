package com.twitter.gizzmo

import com.twitter.gizzard.GizzardServer
import com.twitter.gizzard.scheduler.{JsonJob, PrioritizingJobScheduler}
import com.twitter.gizzmo.config.TestServerConfig

object Priority extends Enumeration {
  val High, Low = Value
}

object Main {
  var service: TestServer = null

  def main(args: Array[String]) {
    val config = args match {
      case Array(a)       => TestServerConfig("integration", a.toInt)
      case Array(a, b, c) => TestServerConfig("integration", a.toInt, b.toInt, c.toInt)
    }

    service = new TestServer(config)

    service.start()
  }
}

class TestServer(conf: config.TestServer) extends GizzardServer[TestShard, JsonJob](conf) {

  val readWriteShardAdapter = new TestReadWriteAdapter(_)
  val jobPriorities         = List(Priority.High.id, Priority.Low.id)
  val copyPriority          = Priority.Low.id
  val copyFactory           = new TestCopyFactory(nameServer, jobScheduler(Priority.Low.id))

  shardRepo += ("TestShard" -> new SqlShardFactory(conf.queryEvaluator(), conf.databaseConnection))

  jobCodec += ("Put".r  -> new PutParser(nameServer.findCurrentForwarding(0, _)))
  jobCodec += ("Copy".r -> new TestCopyParser(nameServer, jobScheduler(Priority.Low.id)))

  lazy val testThriftServer = {
    val service   = new TestServerIFace(nameServer.findCurrentForwarding(0, _), jobScheduler)
    val processor = new thrift.TestServer.Processor(service)
    conf.server(processor)
  }

  def start() {
    startGizzard()
    new Thread(new Runnable { def run() { testThriftServer.serve() } }, "TestServerThread").start()
  }

  def shutdown(quiesce: Boolean) {
    testThriftServer.stop()
    shutdownGizzard(quiesce)
  }
}


// Service Interface

class TestServerIFace(forwarding: Long => TestShard, scheduler: PrioritizingJobScheduler[JsonJob])
extends thrift.TestServer.Iface {
  import com.twitter.gizzard.thrift.conversions.Sequences._

  def put(key: Int, value: String) {
    scheduler.put(Priority.High.id, new PutJob(key, value, forwarding))
  }

  def get(key: Int) = forwarding(key).get(key).map(asTestResult).map(List(_).toJavaList) getOrElse List[thrift.TestResult]().toJavaList

  private def asTestResult(t: (Int, String, Int)) = new thrift.TestResult(t._1, t._2, t._3)
}
