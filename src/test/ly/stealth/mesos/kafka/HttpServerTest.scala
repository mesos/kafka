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
import ly.stealth.mesos.kafka.Topics.Topic
import java.util

class HttpServerTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    startHttpServer()
    Cli.api = Config.api

    startZkServer()
  }
  
  @After
  override def after {
    stopHttpServer()
    super.after
    stopZkServer()
  }
  
  @Test
  def broker_add {
    val json = sendRequest("/broker/add", parseMap("broker=0,cpus=0.1,mem=128"))
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
  def broker_add_range {
    val json = sendRequest("/broker/add", parseMap("broker=0..4"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(5, brokerNodes.size)
    assertEquals(5, Scheduler.cluster.getBrokers.size)
  }

  @Test
  def broker_update {
    sendRequest("/broker/add", parseMap("broker=0"))
    val json = sendRequest("/broker/update", parseMap("broker=0,cpus=1,heap=128,failoverDelay=5s"))
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
  def broker_list {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.addBroker(new Broker("2"))

    var json = sendRequest("/broker/list", parseMap(null))
    var brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]
    assertEquals(3, brokerNodes.size)

    val broker = new Broker()
    broker.fromJson(brokerNodes(0))
    assertEquals("0", broker.id)

    // filtering
    json = sendRequest("/broker/list", parseMap("broker=1"))
    brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]
    assertEquals(1, brokerNodes.size)
  }

  @Test
  def broker_remove {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.addBroker(new Broker("2"))

    var json = sendRequest("/broker/remove", parseMap("broker=1"))
    assertEquals("1", json("ids"))
    assertEquals(2, cluster.getBrokers.size)
    assertNull(cluster.getBroker("1"))

    json = sendRequest("/broker/remove", parseMap("broker=*"))
    assertEquals("0,2", json("ids"))
    assertTrue(cluster.getBrokers.isEmpty)
  }

  @Test
  def broker_start_stop {
    val cluster = Scheduler.cluster
    val broker0 = cluster.addBroker(new Broker("0"))
    val broker1 = cluster.addBroker(new Broker("1"))

    var json = sendRequest("/broker/start", parseMap("broker=*,timeout=0s"))
    assertEquals(2, json("brokers").asInstanceOf[List[Map[String, Object]]].size)
    assertEquals("scheduled", json("status"))
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    json = sendRequest("/broker/stop", parseMap("broker=1,timeout=0s"))
    assertEquals(1, json("brokers").asInstanceOf[List[Map[String, Object]]].size)
    assertEquals("scheduled", json("status"))
    assertTrue(broker0.active)
    assertFalse(broker1.active)

    json = sendRequest("/broker/stop", parseMap("broker=0,timeout=0s"))
    assertEquals(1, json("brokers").asInstanceOf[List[Map[String, Object]]].size)
    assertEquals("scheduled", json("status"))
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  @Test
  def topic_list {
    var json = sendRequest("/topic/list", parseMap(""))
    assertTrue(json("topics").asInstanceOf[List[Map[String, Object]]].isEmpty)

    Scheduler.cluster.topics.addTopic("t0")
    Scheduler.cluster.topics.addTopic("t1")

    json = sendRequest("/topic/list", parseMap(""))
    val topicNodes: List[Map[String, Object]] = json("topics").asInstanceOf[List[Map[String, Object]]]
    assertEquals(2, topicNodes.size)

    val t0Node = topicNodes(0)
    assertEquals("t0", t0Node("name"))
    assertEquals(Map("0" -> "0"), t0Node("partitions"))
  }
  
  @Test
  def topic_add {
    val topics = Scheduler.cluster.topics

    // add t0 topic
    var json = sendRequest("/topic/add", parseMap("topic=t0"))
    val t0Node = json("topics").asInstanceOf[List[Map[String, Object]]](0)
    assertEquals("t0", t0Node("name"))
    assertEquals(Map("0" -> "0"), t0Node("partitions"))

    assertEquals("t0", topics.getTopic("t0").name)

    // add t1 topic
    json = sendRequest("/topic/add", parseMap("topic=t1,partitions=2,options=flush.ms\\=1000"))
    val topicNode = json("topics").asInstanceOf[List[Map[String, Object]]](0)
    assertEquals("t1", topicNode("name"))

    val t1: Topic = topics.getTopic("t1")
    assertNotNull(t1)
    assertEquals("t1", t1.name)
    assertEquals("flush.ms=1000", Util.formatMap(t1.options))

    assertEquals(2, t1.partitions.size())
    assertEquals(util.Arrays.asList(0), t1.partitions.get(0))
    assertEquals(util.Arrays.asList(0), t1.partitions.get(1))
  }
  
  @Test
  def topic_update {
    val topics = Scheduler.cluster.topics
    topics.addTopic("t")

    // update topic t
    val json = sendRequest("/topic/update", parseMap("topic=t,options=flush.ms\\=1000"))
    val topicNode = json("topics").asInstanceOf[List[Map[String, Object]]](0)
    assertEquals("t", topicNode("name"))

    val t = topics.getTopic("t")
    assertEquals("t", t.name)
    assertEquals("flush.ms=1000", Util.formatMap(t.options))
  }

  @Test
  def topic_rebalance {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))

    val rebalancer: TestRebalancer = cluster.rebalancer.asInstanceOf[TestRebalancer]
    assertFalse(rebalancer.running)

    cluster.topics.addTopic("t")
    val json = sendRequest("/topic/rebalance", parseMap("topic=*"))
    assertTrue(rebalancer.running)

    assertEquals("started", json("status"))
    assertFalse(json.contains("error"))
    assertEquals(rebalancer.state, json("state").asInstanceOf[String])
  }

  @Test
  def jar_download {
    val file = download("/jar/kafka-mesos.jar")
    val source = scala.io.Source.fromFile(file)
    val content = try source.mkString finally source.close()
    assertEquals("executor", content)
  }

  @Test
  def kafka_download {
    val file = download("/kafka/kafka.tgz")
    val source = scala.io.Source.fromFile(file)
    val content = try source.mkString finally source.close()
    assertEquals("kafka", content)
  }

  def download(uri: String): File = {
    val url = new URL(Config.api + uri)
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
