package com.twitter.gizzard.testserver

import com.twitter.gizzard.testserver.config.TestServerConfig

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
