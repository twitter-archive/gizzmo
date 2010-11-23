package com.twitter.gizzmo.config

import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.util.TimeConversions._


trait TestDBConnection extends Connection {
  val username = "root"
  val password = ""
  val hostnames = Seq("localhost")
}

object TestQueryEvaluator extends querulous.config.QueryEvaluator {
  val debug       = true
  val autoDisable = None
  val query       = new Query {}
  val database = new Database {
    val statsCollector = None
    val timeout        = None
    val pool = Some(new ApachePoolingDatabase {
      override val sizeMin = 1
      override val sizeMax = 3
      val testIdleMsec = 1.seconds
    })
  }
}

trait TestTHsHaServer extends THsHaServer {
  // val port     = 7919
  val timeout     = 100.millis
  val idleTimeout = 60.seconds

  val threadPool = new ThreadPool {
    val name       = "TestThriftServerThreadPool"
    val minThreads = 10
  }
}

trait TestServer extends gizzard.config.GizzardServer {
  def server: TServer
  def databaseConnection: Connection
  val queryEvaluator = TestQueryEvaluator
  val nsQueryEvaluator = TestQueryEvaluator
}

trait TestJobScheduler extends Scheduler {
  val schedulerType = new Kestrel {
    val queuePath = "/tmp"
    override val keepJournal = false
  }
  val threads           = 1
  val errorLimit        = 25
  val replayInterval    = 900.seconds
  val perFlushItemLimit = 1000
  val jitterRate        = 0.0f
  val badJobQueue       = None
}

class TestNameServer(name: String) extends gizzard.config.NameServer {
  val jobRelay = Some(new JobRelay {
    val priority = Priority.Low.id
    val framed   = true
    val timeout  = 200.milliseconds
  })
  val mappingFunction = Identity
  val replicas = Seq(new Mysql with TestDBConnection {
    val database = "gizzmo_test_" + name + "_ns"
  })
}

object TestServerConfig {
  def apply(name: String, sPort: Int, iPort: Int, mPort: Int) = {
    val queueBase = "gizzmo_test_" + name

    new TestServer {
      val server           = new TestTHsHaServer { val port = sPort }
      val jobInjector      = new JobInjector with TestTHsHaServer { override val port = iPort }
      val databaseConnection = new TestDBConnection { val database = "gizzard_test_" + name }
      val nameServer = new TestNameServer(name)
      val jobQueues = Map(
        Priority.High.id -> new TestJobScheduler { val name = queueBase+"_high" },
        Priority.Low.id  -> new TestJobScheduler { val name = queueBase+"_low" }
      )
      override val manager = new Manager with TThreadServer {
        override val port = mPort
        val threadPool   = new ThreadPool {
          val name       = "gizzard"
          val minThreads = 0
          override val maxThreads = 1
        }
      }
    }
  }

  def apply(name: String, port: Int): TestServer = apply(name, port, port + 1, port + 2)
}
