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

import org.junit.{Test, After, Before}
import org.junit.Assert._
import java.io.{FileOutputStream, File}
import java.net.{HttpURLConnection, URL}
import Util.{Period, parseMap}
import Cli.sendRequest
import BrokerTest.assertBrokerEquals

class HttpServerTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    Config.schedulerUrl = "http://localhost:8000"
    HttpServer.start(resolveDeps = false)
  }
  
  @After
  override def after {
    HttpServer.stop()
    super.after
  }
  
  @Test
  def brokers_add {
    val json = sendRequest("/brokers/add", parseMap("id=0,cpus=0.1,mem=128"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(1, brokerNodes.size)
    val responseBroker = new Broker()
    responseBroker.fromJson(brokerNodes(0))

    assertEquals(1, Scheduler.cluster.getBrokers.size())
    val broker = Scheduler.cluster.getBrokers.get(0)
    assertEquals("0", broker.id)
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)

    BrokerTest.assertBrokerEquals(broker, responseBroker)
  }

  @Test
  def brokers_add_range {
    val json = sendRequest("/brokers/add", parseMap("id=0..4"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(5, brokerNodes.size)
    assertEquals(5, Scheduler.cluster.getBrokers.size)
  }

  @Test
  def brokers_update {
    sendRequest("/brokers/add", parseMap("id=0"))
    val json = sendRequest("/brokers/update", parseMap("id=0,cpus=1,heap=128,failoverDelay=5s"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(1, brokerNodes.size)
    val responseBroker = new Broker()
    responseBroker.fromJson(brokerNodes(0))

    val broker = Scheduler.cluster.getBroker("0")
    assertEquals(1, broker.cpus, 0.001)
    assertEquals(128, broker.heap)
    assertEquals(new Period("5s"), broker.failover.delay)

    BrokerTest.assertBrokerEquals(broker, responseBroker)
  }

  @Test
  def brokers_status {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.addBroker(new Broker("2"))

    val json = sendRequest("/brokers/status", parseMap(null))
    val read = new Cluster()
    read.fromJson(json)

    assertEquals(3, read.getBrokers.size())
    assertBrokerEquals(cluster.getBroker("0"), read.getBroker("0"))
    assertBrokerEquals(cluster.getBroker("1"), read.getBroker("1"))
    assertBrokerEquals(cluster.getBroker("2"), read.getBroker("2"))
  }

  @Test
  def brokers_remove {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.addBroker(new Broker("2"))

    var json = sendRequest("/brokers/remove", parseMap("id=1"))
    assertEquals("1", json("ids"))
    assertEquals(2, cluster.getBrokers.size)
    assertNull(cluster.getBroker("1"))

    json = sendRequest("/brokers/remove", parseMap("id=*"))
    assertEquals("0,2", json("ids"))
    assertTrue(cluster.getBrokers.isEmpty)
  }

  @Test
  def brokers_start_stop {
    val cluster = Scheduler.cluster
    val broker0 = cluster.addBroker(new Broker("0"))
    val broker1 = cluster.addBroker(new Broker("1"))

    var json = sendRequest("/brokers/start", parseMap("id=*,timeout=0s"))
    assertEquals("0,1", json("ids"))
    assertEquals("scheduled", json("status"))
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    json = sendRequest("/brokers/stop", parseMap("id=1,timeout=0s"))
    assertEquals("1", json("ids"))
    assertEquals("scheduled", json("status"))
    assertTrue(broker0.active)
    assertFalse(broker1.active)

    json = sendRequest("/brokers/stop", parseMap("id=0,timeout=0s"))
    assertEquals("0", json("ids"))
    assertEquals("scheduled", json("status"))
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  @Test
  def brokers_rebalance {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))

    val rebalancer: TestRebalancer = cluster.rebalancer.asInstanceOf[TestRebalancer]
    assertFalse(rebalancer.running)

    val json = sendRequest("/brokers/rebalance", parseMap("id=*"))
    assertTrue(rebalancer.running)

    assertEquals("started", json("status"))
    assertFalse(json.contains("error"))
    assertEquals(rebalancer.state, json("state").asInstanceOf[String])
  }

  @Test
  def executor_download {
    val file = download("/executor/kafka-mesos.jar")
    val content = scala.io.Source.fromFile(file).mkString
    assertEquals("executor", content)
  }

  @Test
  def kafka_download {
    val file = download("/kafka/kafka.tgz")
    val content = scala.io.Source.fromFile(file).mkString
    assertEquals("kafka", content)
  }

  def download(uri: String): File = {
    val url = new URL(Config.schedulerUrl + uri)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      val file = File.createTempFile(getClass.getSimpleName, new File(uri).getName)
      Util.copyAndClose(connection.getInputStream, new FileOutputStream(file))
      file.deleteOnExit()
      file
    } finally  {
      connection.disconnect()
    }
  }
}
