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

import org.apache.mesos.Protos.TaskState
import org.junit.{Test, After, Before}
import org.junit.Assert._
import java.io.{IOException, FileOutputStream, File}
import java.net.{HttpURLConnection, URL}
import net.elodina.mesos.util.{Period, IO}
import net.elodina.mesos.util.Strings.{parseMap, formatMap}
import Cli.sendRequest
import ly.stealth.mesos.kafka.Topics.Topic
import java.util
import scala.collection.JavaConversions._

class HttpServerTest extends KafkaMesosTestCase {
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
    var json = sendRequest("/broker/update", parseMap("broker=0,cpus=1,heap=128,failoverDelay=5s"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(1, brokerNodes.size)
    val responseBroker = new Broker()
    responseBroker.fromJson(brokerNodes(0))

    val broker = Scheduler.cluster.getBroker("0")
    assertEquals(1, broker.cpus, 0.001)
    assertEquals(128, broker.heap)
    assertEquals(new Period("5s"), broker.failover.delay)

    BrokerTest.assertBrokerEquals(broker, responseBroker)

    // needsRestart flag
    assertFalse(broker.needsRestart)
    // needsRestart is false despite update when broker stopped
    json = sendRequest("/broker/update", parseMap("broker=0,mem=2048"))
    assertFalse(broker.needsRestart)

    // when broker starting
    sendRequest("/broker/start", parseMap(s"broker=0,timeout=0s"))
    sendRequest("/broker/update", parseMap("broker=0,mem=4096"))
    assertTrue(broker.needsRestart)

    // modification is made before offer thus when it arrives needsRestart reset to false
    Scheduler.resourceOffers(schedulerDriver, Seq(offer("slave0", "cpus:2.0;mem:8192;ports:9042..65000")))
    assertFalse(broker.needsRestart)

    // when running
    Scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "slave0:9042"))
    assertEquals(Broker.State.RUNNING, broker.task.state)
    sendRequest("/broker/update", parseMap("broker=0,log4jOptions=log4j.logger.kafka\\=DEBUG\\\\\\, kafkaAppender"))
    assertTrue(broker.needsRestart)

    // once stopped needsRestart flag reset to false
    sendRequest("/broker/stop", parseMap("broker=0,timeout=0s"))
    assertTrue(broker.needsRestart)
    Scheduler.resourceOffers(schedulerDriver, Seq(offer("cpus:0.01;mem:128;ports:0..1")))
    Scheduler.statusUpdate(schedulerDriver, taskStatus(Broker.nextTaskId(broker), TaskState.TASK_FINISHED))
    assertFalse(broker.needsRestart)
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

  @Test(timeout = 5000)
  def broker_restart: Unit = {
    def assertErrorContains(params: String, str: String) =
      try { sendRequest("/broker/restart", parseMap(params)); fail() }
      catch { case e: IOException => assertTrue(e.getMessage.contains(str))}

    assertErrorContains("broker=0,timeout=0s", "broker 0 not found")

    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))
    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))

    assertErrorContains("broker=0,timeout=0s", "broker 0 is not running")

    // two nodes
    def started(broker: Broker) {
      Scheduler.resourceOffers(schedulerDriver, Seq(offer("slave" + broker.id, "cpus:2.0;mem:2048;ports:9042..65000")))
      Scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "slave" + broker.id + ":9042"))
      assertEquals(Broker.State.RUNNING, broker.task.state)
    }

    def stopped(broker: Broker): Unit = {
      Scheduler.resourceOffers(schedulerDriver, Seq(offer("cpus:0.01;mem:128;ports:0..1")))
      Scheduler.statusUpdate(schedulerDriver, taskStatus(Broker.nextTaskId(broker), TaskState.TASK_FINISHED))
      assertFalse(broker.active)
      assertNull(broker.task)
    }

    def start(broker: Broker) = sendRequest("/broker/start", parseMap(s"broker=${broker.id},timeout=0s"))
    def stop(broker: Broker) = sendRequest("/broker/stop", parseMap(s"broker=${broker.id},timeout=0s"))
    def restart(params: String): Map[String, Object] = sendRequest("/broker/restart", parseMap(params))

    start(broker0); started(broker0); start(broker1); started(broker1)

    // 0 stop timeout
    var json = restart("broker=*,timeout=300ms")
    assertEquals(Map("status" -> "timeout", "message" -> "broker 0 timeout on stop"), json)

    stopped(broker0); start(broker0); started(broker0)

    // 0 start timeout
    delay("150ms") { stopped(broker0) }
    json = restart("broker=*,timeout=300ms")
    assertEquals(Map("status" -> "timeout", "message" -> "broker 0 timeout on start"), json)

    started(broker0)

    // 0 start, but 1 isn't running
    delay("150ms") { stopped(broker0) }
    delay("175ms") { stop(broker1); stopped(broker1) }
    delay("300ms") { started(broker0) }
    assertErrorContains("broker=*,timeout=500ms", "broker 1 is not running")

    start(broker1); started(broker1)

    // 1 stop timeout
    delay("150ms") { stopped(broker0) }
    delay("250ms") { started(broker0) }
    json = restart("broker=*,timeout=400ms")
    assertEquals(Map("status" -> "timeout", "message" -> "broker 1 timeout on stop"), json)

    stopped(broker1); start(broker1); started(broker1)

    // restarted
    delay("150ms") { stopped(broker0) }
    delay("250ms") { started(broker0) }
    delay("350ms") { stopped(broker1) }
    delay("450ms") { started(broker1) }
    json = restart("broker=*,timeout=1s")

    assertEquals(json("status"), "restarted")
    for((brokerJson, expectedBroker) <- json("brokers").asInstanceOf[List[Map[String, Object]]].zip(Seq(broker0, broker1))) {
      val actualBroker = new Broker()
      actualBroker.fromJson(brokerJson)
      BrokerTest.assertBrokerEquals(expectedBroker, actualBroker)
    }
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
    assertEquals("flush.ms=1000", formatMap(t1.options))

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
    assertEquals("flush.ms=1000", formatMap(t.options))
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
      IO.copyAndClose(connection.getInputStream, new FileOutputStream(file))
      file.deleteOnExit()
      file
    } finally  {
      connection.disconnect()
    }
  }
}
