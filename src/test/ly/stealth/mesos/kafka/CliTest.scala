/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import scala.collection.JavaConversions._
import java.io.{ByteArrayOutputStream, PrintStream}
import Util.Period

class CliTest extends MesosTestCase {
  val out: ByteArrayOutputStream = new ByteArrayOutputStream()

  @Before
  override def before {
    super.before
    Config.schedulerUrl = "http://localhost:8000"
    HttpServer.start(resolveDeps = false)
    Cli.out = new PrintStream(out, true)
  }

  @After
  override def after {
    Cli.out = System.out
    HttpServer.stop()
    super.after
  }

  @Test
  def help {
    exec("help")
    assertOutContains("Usage:")
    assertOutContains("scheduler")
    assertOutContains("start")
    assertOutContains("stop")

    // command help
    for (command <- "help scheduler status add update remove start stop".split(" ")) {
      exec("help " + command)
      assertOutContains("Usage: " + command)
    }
  }

  @Test
  def status {
    Scheduler.cluster.addBroker(new Broker("0"))
    Scheduler.cluster.addBroker(new Broker("1"))
    Scheduler.cluster.addBroker(new Broker("2"))

    exec("status")
    assertOutContains("status received")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
    assertOutContains("id: 2")
  }

  @Test
  def add {
    exec("add 0 --cpus=0.1 --mem=128")
    assertOutContains("Broker added")
    assertOutContains("id: 0")
    assertOutContains("cpus:0.10, mem:128")

    assertEquals(1, Scheduler.cluster.getBrokers.size())
    val broker = Scheduler.cluster.getBroker("0")
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)
  }

  @Test
  def update {
    val broker = Scheduler.cluster.addBroker(new Broker("0"))

    exec("update 0 --failoverDelay=10s --failoverMaxDelay=20s --options=log.dirs=/tmp/kafka-logs")
    assertOutContains("Broker updated")
    assertOutContains("delay:10s, maxDelay:20s")
    assertOutContains("options: log.dirs=/tmp/kafka-logs")

    assertEquals(new Period("10s"), broker.failover.delay)
    assertEquals(new Period("20s"), broker.failover.maxDelay)
    assertEquals(Util.parseMap("log.dirs=/tmp/kafka-logs"), broker.options)
  }

  @Test
  def remove {
    Scheduler.cluster.addBroker(new Broker("0"))
    exec("remove 0")

    assertOutContains("Broker 0 removed")
    assertNull(Scheduler.cluster.getBroker("0"))
  }

  @Test
  def start_stop {
    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))
    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))

    exec("start * --timeout=0")
    assertOutContains("Brokers 0,1")
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    exec("stop 0 --timeout=0")
    assertOutContains("Broker 0")
    assertFalse(broker0.active)
    assertTrue(broker1.active)

    exec("stop 1 --timeout=0")
    assertOutContains("Broker 1")
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  @Test
  def start_stop_timeout {
    val broker = Scheduler.cluster.addBroker(new Broker("0"))
    try { exec("start 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("Got timeout")) }
    assertTrue(broker.active)

    broker.task = new Broker.Task("id", "slave", "executor", "host", 1000, _running = true)
    try { exec("stop 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("Got timeout")) }
    assertFalse(broker.active)
  }

  @Test
  def rebalance {
    val cluster: Cluster = Scheduler.cluster
    val rebalancer: Rebalancer = cluster.rebalancer

    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    assertFalse(rebalancer.running)

    exec("rebalance *")
    assertTrue(rebalancer.running)
    assertOutContains("Rebalance started")
  }

  @Test
  def usage_errors {
    // no command
    try { exec(""); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("command required")) }

    // no id
    try { exec("add"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("argument required")) }

    // invalid command
    try { exec("unsupported 0"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("unsupported command")) }
  }

  @Test
  def connection_refused {
    HttpServer.stop()
    try {
      try { exec("add 0"); fail()  }
      catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("Connection refused")) }
    } finally {
      HttpServer.start()
    }
  }

  private def assertOutContains(s: String): Unit = assertTrue("" + out, out.toString.contains(s))

  private def exec(cmd: String): Unit = {
    out.reset()

    val args = new util.ArrayList[String]()
    for (arg <- cmd.split(" "))
      if (!cmd.isEmpty) args.add(arg)
    Cli.exec(args.toArray(new Array[String](args.length)))
  }
}
