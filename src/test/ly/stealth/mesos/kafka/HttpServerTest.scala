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

import org.apache.mesos.Protos.{ExecutorID, SlaveID, TaskState}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.io.{File, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import net.elodina.mesos.util.{IO, Period, Version}
import net.elodina.mesos.util.Strings.{formatMap, parseMap}
import ly.stealth.mesos.kafka.cli.Cli.{sendRequest, sendRequestObj}
import java.util.Properties
import ly.stealth.mesos.kafka.Broker.{Container, ContainerType, Mount, MountMode}
import ly.stealth.mesos.kafka.cli.Cli
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.{AdminUtilsWrapper, Quota, Quotas}
import scala.collection.JavaConversions._
import scala.io.Source

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
    val brokers = sendRequestObj[BrokerStatusResponse]("/broker/add", parseMap("broker=0,cpus=0.1,mem=128"))
    assertEquals(1, brokers.brokers.size)

    assertEquals(1, registry.cluster.getBrokers.size())
    val broker = registry.cluster.getBrokers.get(0)
    assertEquals(0, broker.id)
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)

    BrokerTest.assertBrokerEquals(broker, brokers.brokers.head)
  }

  @Test
  def broker_add_docker_image: Unit = {
    val brokers = sendRequestObj[BrokerStatusResponse]("/broker/add",
      parseMap("broker=0,cpus=1,mem=128,containerImage=test,javaCmd=/usr/bin/java"))
    assertEquals(1, brokers.brokers.size)
    val broker = registry.cluster.getBroker(0)
    assertEquals(Some(Container(
      ctype = ContainerType.Docker,
      name = "test"
    )), broker.executionOptions.container)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
    BrokerTest.assertBrokerEquals(broker, brokers.brokers.head)
  }

  @Test
  def broker_add_mesos_image = {
    val brokers = sendRequestObj[BrokerStatusResponse]("/broker/add",
      parseMap("broker=0,cpus=1,mem=128,containerImage=test,containerType=mesos,javaCmd=/usr/bin/java"))
    assertEquals(1, brokers.brokers.size)
    val broker = registry.cluster.getBroker(0)
    assertEquals(Some(Container(
      ctype = ContainerType.Mesos,
      name = "test"
    )), broker.executionOptions.container)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
    BrokerTest.assertBrokerEquals(broker, brokers.brokers.head)
  }

  @Test
  def broker_update_image: Unit = {
    val brokers = sendRequestObj[BrokerStatusResponse]("/broker/add",
      parseMap("broker=0,cpus=1,mem=128,containerImage=test,javaCmd=/usr/bin/java"))
    assertEquals(1, brokers.brokers.size)
    val broker = registry.cluster.getBroker(0)
    assertEquals("test", broker.executionOptions.container.map(_.name).orNull)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
    BrokerTest.assertBrokerEquals(broker, brokers.brokers.head)

    // No key doesn't remove the image
    sendRequestObj[BrokerStatusResponse]("/broker/update",
      parseMap("broker=0,cpus=1,mem=128,javaCmd=/usr/bin/java"))
    assertEquals("test", broker.executionOptions.container.map(_.name).orNull)

    // Empty key does
    sendRequestObj[BrokerStatusResponse]("/broker/update",
      Map("broker" -> "0", "containerImage" -> ""))
    assertEquals(None, broker.executionOptions.container)

    sendRequestObj[BrokerStatusResponse]("/broker/update",
      Map(
        "broker" -> "0",
        "containerImage" -> "test",
        "containerMounts" -> "/a:/b,/c:/d:R"))

    // Add mount points
    assertEquals(Some(Container(
      ctype = ContainerType.Docker,
      name = "test",
      mounts = Seq(
        Mount("/a", "/b", MountMode.ReadWrite),
        Mount("/c", "/d", MountMode.ReadOnly)
      )
    )), registry.cluster.getBroker(0).executionOptions.container)

    // Remove them but keep the image
    sendRequestObj[BrokerStatusResponse]("/broker/update",
      Map(
        "broker" -> "0",
        "containerImage" -> "test",
        "containerMounts" -> ""))
    assertEquals(Some(Container(
      ctype = ContainerType.Docker,
      name = "test",
      mounts = Seq()
    )), registry.cluster.getBroker(0).executionOptions.container)

    // Switch the image type
    sendRequestObj[BrokerStatusResponse]("/broker/update", parseMap("broker=0,containerType=mesos"))
    assertEquals(Some(Container(
      ctype = ContainerType.Mesos,
      name = "test",
      mounts = Seq()
    )), registry.cluster.getBroker(0).executionOptions.container)
  }

  @Test
  def broker_add_range {
    val brokers = sendRequestObj[BrokerStatusResponse]("/broker/add", parseMap("broker=0..4"))
    assertEquals(5, brokers.brokers.size)
    assertEquals(5, registry.cluster.getBrokers.size)
  }

  @Test
  def broker_update {
    sendRequest("/broker/add", parseMap("broker=0"))
    var resp = sendRequestObj[BrokerStatusResponse]("/broker/update", parseMap("broker=0,cpus=1,heap=128,failoverDelay=5s"))

    assertEquals(1, resp.brokers.size)
    val responseBroker = resp.brokers.head

    val broker = registry.cluster.getBroker(0)
    assertEquals(1, broker.cpus, 0.001)
    assertEquals(128, broker.heap)
    assertEquals(new Period("5s"), broker.failover.delay)

    BrokerTest.assertBrokerEquals(broker, responseBroker)

    // needsRestart flag
    assertFalse(broker.needsRestart)
    // needsRestart is false despite update when broker stopped
    resp = sendRequestObj[BrokerStatusResponse]("/broker/update", parseMap("broker=0,mem=2048"))
    assertFalse(broker.needsRestart)

    // when broker starting
    sendRequest("/broker/start", parseMap(s"broker=0,timeout=0s"))
    sendRequest("/broker/update", parseMap("broker=0,mem=4096"))
    assertTrue(broker.needsRestart)

    // modification is made before offer thus when it arrives needsRestart reset to false
    registry.scheduler.resourceOffers(schedulerDriver, Seq(offer("slave0", "cpus:2.0;mem:8192;ports:9042..65000")))
    assertTrue(broker.waitFor(Broker.State.PENDING, new Period("1s"), 1))
    assertFalse(broker.needsRestart)

    // when running
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_STARTING, "slave0:9042"))
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "slave0:9042"))
    assertTrue(broker.waitFor(Broker.State.RUNNING, new Period("1s"), 1))
    sendRequest("/broker/update", parseMap("broker=0,log4jOptions=log4j.logger.kafka\\=DEBUG\\\\\\, kafkaAppender"))
    assertTrue(broker.needsRestart)

    // once stopped needsRestart flag reset to false
    sendRequest("/broker/stop", parseMap("broker=0,timeout=0s"))
    assertTrue(broker.needsRestart)
    registry.scheduler.resourceOffers(schedulerDriver, Seq(offer("cpus:0.01;mem:128;ports:0..1")))
    assertTrue(broker.waitFor(Broker.State.STOPPING, new Period("1s"), 1))
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_FINISHED))
    assertTrue(broker.waitFor(null, new Period("1s"), 1))
    assertFalse(broker.needsRestart)
  }

  @Test
  def broker_list {
    val cluster = registry.cluster
    cluster.addBroker(new Broker(0))
    cluster.addBroker(new Broker(1))
    cluster.addBroker(new Broker(2))

    var json = sendRequestObj[BrokerStatusResponse]("/broker/list", parseMap(null))
    var brokers = json.brokers
    assertEquals(3, brokers.size)

    val broker = brokers.head
    assertEquals(0, broker.id)

    // filtering
    json = sendRequestObj[BrokerStatusResponse]("/broker/list", parseMap("broker=1"))
    brokers = json.brokers
    assertEquals(1, brokers.size)
  }

  @Test
  def broker_clone {
    val cluster = registry.cluster
    cluster.addBroker(new Broker(0))

    val json = sendRequestObj[BrokerStatusResponse]("/broker/clone", Map("broker" -> "1", "source" -> "0"))
    val brokers = json.brokers
    assertEquals(1, brokers.size)

    val broker = brokers.head

    assertEquals(1, broker.id)
  }

  @Test
  def broker_remove {
    val cluster = registry.cluster
    cluster.addBroker(new Broker(0))
    cluster.addBroker(new Broker(1))
    cluster.addBroker(new Broker(2))

    var json = sendRequestObj[BrokerRemoveResponse]("/broker/remove", parseMap("broker=1"))
    assertEquals(Seq("1"), json.ids)
    assertEquals(2, cluster.getBrokers.size)
    assertNull(cluster.getBroker(1))

    json = sendRequestObj[BrokerRemoveResponse]("/broker/remove", parseMap("broker=*"))
    assertEquals(Seq("0", "2"), json.ids)
    assertTrue(cluster.getBrokers.isEmpty)
  }

  @Test
  def broker_start_stop {
    val cluster = registry.cluster
    val broker0 = cluster.addBroker(new Broker(0))
    val broker1 = cluster.addBroker(new Broker(1))

    var json = sendRequestObj[BrokerStartResponse]("/broker/start", parseMap("broker=*,timeout=0s"))
    assertEquals(2, json.brokers.size)
    assertEquals("scheduled", json.status)
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    json = sendRequestObj[BrokerStartResponse]("/broker/stop", parseMap("broker=1,timeout=0s"))
    assertEquals(1, json.brokers.size)
    assertEquals("scheduled", json.status)
    assertTrue(broker0.active)
    assertFalse(broker1.active)

    json = sendRequestObj[BrokerStartResponse]("/broker/stop", parseMap("broker=0,timeout=0s"))
    assertEquals(1, json.brokers.size)
    assertEquals("scheduled", json.status)
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  def assertErrorContains[R](url: String, params: Map[String, String], str: String)(implicit manifest: Manifest[R]): Unit =
    try { sendRequestObj[R](url, params); fail() }
    catch { case e: IOException =>
      assertTrue(
        s"String was not found in input\nExpected: $str\nSearched: ${e.getMessage}",
        e.getMessage.contains(str))
    }

  def start(broker: Broker) = sendRequestObj[BrokerStartResponse]("/broker/start", parseMap(s"broker=${broker.id},timeout=0s"))
  def stop(broker: Broker) = sendRequestObj[BrokerStartResponse]("/broker/stop", parseMap(s"broker=${broker.id},timeout=0s"))
  def restart(params: String) = sendRequestObj[BrokerStartResponse]("/broker/restart", parseMap(params))

  def prepareBrokers = {
    val broker0 = registry.cluster.addBroker(new Broker(0))
    val broker1 = registry.cluster.addBroker(new Broker(1))
    start(broker0); started(broker0); start(broker1); started(broker1)
    (broker0, broker1)
  }

  @Test(timeout = 10000)
  def broker_start_success: Unit = {
    assertErrorContains[BrokerStatusResponse](
      "/broker/restart",
      Map("broker" -> "0", "timeout" -> "0s"),
      "broker 0 not found")

    prepareBrokers
  }

  @Test
  def broker_restart_stop_timeout: Unit = {
    val (broker0, broker1) = prepareBrokers

    // 0 stop timeout
    val json = restart("broker=*,timeout=300ms")
    assertEquals("timeout", json.status)
    assertEquals(Some("broker 0 timeout on stop"), json.message)
    stopped(broker0); start(broker0); started(broker0)
  }

  @Test
  def broker_restart_start_timeout: Unit = {
    val (broker0, broker1) = prepareBrokers

    // 0 start timeout
    delay("150ms") { stopped(broker0) }
    val json = restart("broker=*,timeout=300ms")
    assertEquals("timeout", json.status)
    assertEquals(Some("broker 0 timeout on start"), json.message)
  }

  @Test
  def broker_restart_one_broker = {
    val (broker0, broker1) = prepareBrokers
    stop(broker1); stopped(broker1)

    // 0 start, but 1 isn't running
    delay("150ms") { stopped(broker0) }
    delay("300ms") { started(broker0) }
    restart("broker=*,timeout=500ms")
    assertEquals(true, broker0.active)
    assertNotNull(broker0.task)
    assertEquals(true, broker1.active)

    start(broker1); started(broker1)
    assertNotNull(broker1.task)
  }

  @Test
  def broker_restart_one_stop_timeout = {
    val (broker0, broker1) = prepareBrokers
    // 1 stop timeout
    delay("150ms") { stopped(broker0) }
    delay("250ms") { started(broker0) }
    val json = restart("broker=*,timeout=400ms")
    assertEquals("timeout", json.status)
    assertEquals(Some("broker 1 timeout on stop"), json.message)
  }

  @Test
  def broker_restart_both_success = {
    val (broker0, broker1) = prepareBrokers
    // restarted
    delay("100ms") {
      for (broker <- Seq(broker0, broker1)) {
        broker.waitFor(Broker.State.STOPPING, new Period("1s"), 1)
        stopped(broker)
        broker.waitFor(null, new Period("1s"), 1)
        broker.waitFor(Broker.State.STARTING, new Period("1s"), 1)
        started(broker)
        broker.waitFor(Broker.State.RUNNING, new Period("1s"), 1)
      }
    }
    val json = restart("broker=*,timeout=2s")

    assertEquals("restarted", json.status)
    for((actualBroker, expectedBroker) <- json.brokers.zip(Seq(broker0, broker1))) {
      BrokerTest.assertBrokerEquals(expectedBroker, actualBroker)
    }
  }

  @Test
  def broker_log {
    val broker0 = registry.cluster.addBroker(new Broker(0))
    assertErrorContains[HttpLogResponse]("/broker/log", Map("broker" -> "0"), "broker 0 is not active")

    broker0.active = true
    broker0.task = Broker.Task(
      id="t1",
      executorId=Broker.nextExecutorId(broker0),
      slaveId="s1")
    broker0.task.state = Broker.State.RUNNING

    val timeoutResponse =
      sendRequestObj[HttpLogResponse]("/broker/log", Map("broker" -> "0", "timeout" -> "1ms"))
    assertEquals("timeout", timeoutResponse.status)
    assertEquals(1, schedulerDriver.sentFrameworkMessages.size())
    val msg = schedulerDriver.sentFrameworkMessages(0)
    assertEquals(broker0.task.executorId, msg.executorId)
    assertEquals("s1", msg.slaveId)

    def setLogContent(content: String, delay: Period = new Period("100ms")) =
      new Thread {
        override def run(): Unit = {
          Thread.sleep(delay.ms)
          val lastData = schedulerDriver.sentFrameworkMessages.last.data
          val logRequest = LogRequest.parse(new String(lastData))
          registry.scheduler.frameworkMessage(
            schedulerDriver,
            ExecutorID.newBuilder().setValue(broker0.task.executorId).build(),
            SlaveID.newBuilder().setValue("s1").build(),
            JsonUtil.toJsonBytes(FrameworkMessage(log = Some(LogResponse(logRequest.requestId, content))))
          )
        }
      }.start()

    setLogContent("something")
    val json = sendRequestObj[HttpLogResponse]("/broker/log", Map("broker" -> "0"))
    assertEquals("something", json.content)
  }

  @Test
  def topic_list {
    var json = sendRequestObj[ListTopicsResponse]("/topic/list", parseMap(""))
    assertTrue(json.topics.isEmpty)

    registry.cluster.topics.addTopic("t0")
    registry.cluster.topics.addTopic("t1")

    json = sendRequestObj[ListTopicsResponse]("/topic/list", parseMap(""))
    val topicNodes = json.topics
    assertEquals(2, topicNodes.size)

    val t0Node = topicNodes.head
    assertEquals("t0", t0Node.name)
    assertEquals(Seq(0), t0Node.partitions(0))
  }
  
  @Test
  def topic_add {
    val topics = registry.cluster.topics

    // add t0 topic
    var json = sendRequestObj[ListTopicsResponse]("/topic/add", parseMap("topic=t0"))
    val t0Node = json.topics.head
    assertEquals("t0", t0Node.name)
    assertEquals(Map(0 -> List(0)), t0Node.partitions)

    assertEquals("t0", topics.getTopic("t0").name)

    // add t1 topic
    json = sendRequestObj[ListTopicsResponse]("/topic/add", parseMap("topic=t1,partitions=2,options=flush.ms\\=1000"))
    val topicNode = json.topics.head
    assertEquals("t1", topicNode.name)

    val t1: Topic = topics.getTopic("t1")
    assertNotNull(t1)
    assertEquals("t1", t1.name)
    assertEquals("flush.ms=1000", formatMap(t1.options))

    assertEquals(2, t1.partitions.size)
    assertEquals(Seq(0), t1.partitions(0))
    assertEquals(Seq(0), t1.partitions(1))
  }
  
  @Test
  def topic_update {
    val topics = registry.cluster.topics
    topics.addTopic("t")

    // update topic t
    val json = sendRequestObj[ListTopicsResponse]("/topic/update", parseMap("topic=t,options=flush.ms\\=1000"))
    val topicNode = json.topics.head
    assertEquals("t", topicNode.name)

    val t = topics.getTopic("t")
    assertEquals("t", t.name)
    assertEquals("flush.ms=1000", formatMap(t.options))
  }

  @Test
  def topic_rebalance {
    val cluster = registry.cluster
    cluster.addBroker(new Broker(0))
    cluster.addBroker(new Broker(1))

    val rebalancer: TestRebalancer = cluster.rebalancer.asInstanceOf[TestRebalancer]
    assertFalse(rebalancer.running)

    cluster.topics.addTopic("t")
    val json = sendRequestObj[RebalanceStartResponse]("/topic/rebalance", parseMap("topic=*"))
    assertTrue(rebalancer.running)

    assertEquals("started", json.status)
    assertFalse(json.error.isDefined)
    assertEquals(rebalancer.state, json.state)
  }

  @Test
  def quota_list: Unit = {
    if (!AdminUtilsWrapper().features.quotas) return

    val cluster = registry.cluster
    val configs = new Properties()
    configs.setProperty(Quotas.PRODUCER_BYTE_RATE, "100")
    configs.setProperty(Quotas.CONSUMER_BYTE_RATE, "200")
    cluster.quotas.setClientConfig("test", configs)

    val json = sendRequestObj[Map[String, Map[String, Int]]]("/quota/list", parseMap(""))
    assertEquals(json("test")("producer_byte_rate"), 100)
    assertEquals(json("test")("consumer_byte_rate"), 200)
  }

  @Test
  def quota_list_partial: Unit = {
    if (!AdminUtilsWrapper().features.quotas) return

    val cluster = registry.cluster
    val configs = new Properties()
    configs.setProperty(Quotas.PRODUCER_BYTE_RATE, "100")
    cluster.quotas.setClientConfig("test", configs)

    val json = sendRequestObj[Map[String, Map[String, Int]]]("/quota/list", parseMap(""))
    assertEquals(json("test")("producer_byte_rate"), 100)
    assertFalse(json("test").contains("consumer_byte_rate"))
  }

  @Test
  def quota_set: Unit = {
    if (!AdminUtilsWrapper().features.quotas) return

    sendRequest("/quota/set", parseMap("entityType=clients,entity=test,producerByteRate=100,consumerByteRate=200"))

    val quotas = registry.cluster.quotas.getClientQuotas()
    assertEquals(quotas.size, 1)
    assertEquals(quotas("test"), Quota(Some(100), Some(200)))
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

  private def makeRequest(url: String, method: String = "GET"): String = {
    val conn = new URL(Config.api + url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    Source.fromInputStream(conn.getInputStream).mkString
  }

  private def getFailsPostSucceeds(url: String) = {
    for (method <- Seq("GET", "POST"))
    try {
      makeRequest(url, method)
      if (method == "GET")
        fail("get shouldn't be allowed")
    } catch {
      case e: IOException if e.getMessage.startsWith("Server returned HTTP response code: 405") =>
      case e: Throwable => fail("Wrong exception\n" + e.toString)
    }
  }

  @Test
  def qqq {
    getFailsPostSucceeds("/quitquitquit")
  }

  @Test
  def health {
    var ret = makeRequest("/health")
    assertEquals("ok", ret)

    ret = makeRequest("/health", "POST")
    assertEquals("ok", ret)
  }
}
