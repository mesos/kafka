/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import org.apache.mesos.Protos.Resource.DiskInfo.Source
import org.junit.{Before, Test}
import org.junit.Assert._
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util.{Constraint, Period, Range}
import net.elodina.mesos.util.Strings.parseMap
import java.util.{Collections, Date}
import scala.collection.JavaConversions._
import ly.stealth.mesos.kafka.Broker._
import java.util
import ly.stealth.mesos.kafka.executor.LaunchConfig
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.mesos.OfferResult
import org.apache.mesos.Protos.{Offer, Resource}

class BrokerTest extends KafkaMesosTestCase {

  var broker: Broker = null

  @Before
  override def before {
    super.before
    broker = new Broker(0)
    broker.cpus = 0
    broker.mem = 0
  }

  @Test
  def options {
    {
      // $id substitution
      val li = LaunchConfig(0, Map("a" -> "$id", "b" -> "2"), false, Map(), null, Map())
      assertEquals(Map("a" -> "0", "b" -> "2"), li.interpolatedOptions)
    }
    {
      // defaults
      val li = LaunchConfig(0, Map("a" -> "2"), false, Map(), null, Map("b" -> "1"))
      assertEquals(Map("b" -> "1", "a" -> "2"), li.interpolatedOptions)
    }

    {
      // bind-address
      val li = LaunchConfig(0, Map("host.name" -> "123"), false, Map(), null, Map())
      assertEquals(Map("host.name" -> "123"), li.interpolatedOptions)
    }
    {
      val li = LaunchConfig(0, Map("host.name" -> "123"), false, Map(), new BindAddress("127.0.0.1"), Map())
      assertEquals(Map("host.name" -> "127.0.0.1"), li.interpolatedOptions)
    }
    {
      broker.bindAddress = new BindAddress("127.0.0.1")
      val li = LaunchConfig(0, Map("listeners" -> "PLAINTEXT://:3002", "port" -> "3002"), false, Map(), new BindAddress("127.0.0.1"), Map())
      assertEquals(Map("host.name" -> "127.0.0.1", "port" -> "3002", "listeners" -> "PLAINTEXT://127.0.0.1:3002"), li.interpolatedOptions)
    }
  }

  @Test
  def matches {
    // cpus
    broker.cpus = 0.5
    var theOffer = offer("cpus:0.2; cpus(role):0.3; ports:1000")
    BrokerTest.assertAccept(broker.matches(theOffer))

    theOffer = offer("cpus:0.2; cpus(role):0.2")
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "cpus < 0.5"),
      broker.matches(theOffer))
    broker.cpus = 0

    // mem
    broker.mem = 100
    theOffer = offer("mem:70; mem(role):30; ports:1000")
    BrokerTest.assertAccept(broker.matches(theOffer))

    theOffer = offer("mem:70; mem(role):29")
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "mem < 100"),
      broker.matches(theOffer))
    broker.mem = 0

    // port
    theOffer = offer("ports:1000")
    BrokerTest.assertAccept(broker.matches(theOffer))

    theOffer = offer("")
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "no suitable port"),
      broker.matches(theOffer))
  }

  @Test
  def matches_hostname {
    val now = new Date(0)
    val resources: String = "ports:0..10"

    BrokerTest.assertAccept(broker.matches(offer("master", resources)))
    BrokerTest.assertAccept(broker.matches(offer("slave", resources)))

    // token
    broker.constraints = parseMap("hostname=like:master").mapValues(new Constraint(_)).toMap
    var theOffer = offer("master", resources)
    BrokerTest.assertAccept(broker.matches(theOffer))

    theOffer = offer("slave", resources)
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "hostname doesn't match like:master"),
      broker.matches(theOffer))

    // like
    broker.constraints = parseMap("hostname=like:master.*").mapValues(new Constraint(_)).toMap
    BrokerTest.assertAccept(broker.matches(offer("master", resources)))
    BrokerTest.assertAccept(broker.matches(offer("master-2", resources)))
    theOffer = offer("slave", resources)
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "hostname doesn't match like:master.*"),
      broker.matches(theOffer))

    // unique
    broker.constraints = parseMap("hostname=unique").mapValues(new Constraint(_)).toMap
    theOffer = offer("master", resources)
    BrokerTest.assertAccept(broker.matches(theOffer))

    theOffer = offer("master", resources)
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "hostname doesn't match unique"),
      broker.matches(theOffer, now, _ => util.Arrays.asList("master")))
    BrokerTest.assertAccept(broker.matches(offer("master", resources), now, _ => util.Arrays.asList("slave")))

    // groupBy
    broker.constraints = parseMap("hostname=groupBy").mapValues(new Constraint(_)).toMap
    BrokerTest.assertAccept(broker.matches(offer("master", resources)))
    BrokerTest.assertAccept(broker.matches(offer("master", resources), now, _ => util.Arrays.asList("master")))
    theOffer = offer("master", resources)
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "hostname doesn't match groupBy"),
      broker.matches(theOffer, now, _ => util.Arrays.asList("slave")))
  }

  @Test
  def matches_stickiness {
    val host0 = "host0"
    val host1 = "host1"
    val resources = "ports:0..10"


    BrokerTest.assertAccept(broker.matches(offer(host0, resources), new Date(0)))
    BrokerTest.assertAccept(broker.matches(offer(host1, resources), new Date(0)))

    broker.registerStart(host0)
    broker.registerStop(new Date(0))


    BrokerTest.assertAccept(broker.matches(offer(host0, resources), new Date(0)))
    val theOffer = offer(host1, resources)
    assertEquals(
      OfferResult.eventuallyMatch(
        theOffer, broker,
        "hostname != stickiness host", broker.stickiness.period.ms().toInt / 1000),
      broker.matches(theOffer, new Date(0)))
  }

  @Test
  def matches_attributes {
    val now = new Date(0)

    def offer(attributes: String): Offer = this.offer("id", "fw-id", "slave-id", "host", "ports:0..10", attributes)

    // like
    broker.constraints = parseMap("rack=like:1-.*").mapValues(new Constraint(_)).toMap
    BrokerTest.assertAccept(broker.matches(offer("rack=1-1")))
    BrokerTest.assertAccept(broker.matches(offer("rack=1-2")))
    var theOffer = offer("rack=2-1")
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "rack doesn't match like:1-.*"),
      broker.matches(theOffer))

    // groupBy
    broker.constraints = parseMap("rack=groupBy").mapValues(new Constraint(_)).toMap
    BrokerTest.assertAccept(broker.matches(offer("rack=1")))
    BrokerTest.assertAccept(broker.matches(offer("rack=1"), now, _ => util.Arrays.asList("1")))
    theOffer = offer("rack=2")
    assertEquals(
      OfferResult.neverMatch(theOffer, broker, "rack doesn't match groupBy"),
      broker.matches(theOffer, now, _ => util.Arrays.asList("1")))
  }

  @Test
  def getReservations_dynamic = {
    broker.cpus = 2
    broker.mem = 200
    // ignore non-dynamically reserved disk
    var reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; disk:1000"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // Ignore resources with a principal
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(*,principal):100"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // Ignore resources with a principal + role
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(role,principal):100"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // pay attention resources with a role
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(role):100"))
    assertEquals(resources("cpus:2; mem:100; mem(role):100; ports:1000"), reservation.toResources)
  }

  @Test
  def getReservations_volume = {
    broker.cpus = 2
    broker.mem = 200
    broker.volume = "test"

    val reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; disk(role,principal)[test:mount_point]:100"))
    val resource = resources("cpus:2; mem:100; ports:1000; disk(role,principal)[test:data]:100")
    assertEquals(resource, reservation.toResources)
  }

  def diskResourceWithSource(disk: String, `type`: Source.Type, path: String): Resource = {
    val withoutSource = resources(disk).get(0)
    val sourceBuilder = Source.newBuilder()
    sourceBuilder.setType(`type`)
    if (`type` == Source.Type.MOUNT) {
      sourceBuilder.setMount(Source.Mount.newBuilder().setRoot(path))
    } else {
      sourceBuilder.setPath(Source.Path.newBuilder().setRoot(path))
    }

    withoutSource.
      toBuilder.
      setDisk(
        withoutSource.
          getDisk.
          toBuilder.
          setSource(sourceBuilder)).
      build
  }
  @Test
  def getReservations_volumeMount(): Unit = {
    broker.cpus = 2
    broker.cpus = 2
    broker.mem = 200
    broker.volume = "test"

    val persistentVolumeResource = diskResourceWithSource(
      "disk(role,principal)[test:mount_point]:100",
      Source.Type.MOUNT,
      "/mnt/path")


    val thisOffer = offer("").toBuilder().
      addAllResources(resources("cpus:2; mem:100; ports:1000")).
      addResources(persistentVolumeResource).
      build()

    val reservation = broker.getReservation(thisOffer)

    assertEquals(reservation.diskSource, persistentVolumeResource.getDisk().getSource())
  }

  @Test
  def getReservations {
    broker.cpus = 2
    broker.mem = 100

    // shared resources
    var reservation = broker.getReservation(offer("cpus:3; mem:200; ports:1000..2000"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // role resources
    reservation = broker.getReservation(offer("cpus(role):3; mem(role):200; ports(role):1000..2000"))
    assertEquals(resources("cpus(role):2; mem(role):100; ports(role):1000"), reservation.toResources)

    // mixed resources
    reservation = broker.getReservation(offer("cpus:2; cpus(role):1; mem:100; mem(role):99; ports:1000..2000; ports(role):3000"))
    assertEquals(resources("cpus:1; cpus(role):1; mem:1; mem(role):99; ports(role):3000"), reservation.toResources)

    // not enough resources
    reservation = broker.getReservation(offer("cpus:0.5; cpus(role):0.5; mem:1; mem(role):1; ports:1000"))
    assertEquals(resources("cpus:0.5; cpus(role):0.5; mem:1; mem(role):1; ports:1000"), reservation.toResources)

    // no port
    reservation = broker.getReservation(offer(""))
    assertEquals(-1, reservation.port)

    // two non-default roles
    try {
      broker.getReservation(offer("cpus(r1):0.5; mem(r2):100"))
      fail()
    } catch {
      case e: IllegalArgumentException =>
        val m: String = e.getMessage
        assertTrue(m, m.contains("r1") && m.contains("r2"))
    }
  }


  @Test
  def getSuitablePort {
    def ranges(s: String): util.List[Range] = {
      if (s.isEmpty) return Collections.emptyList()
      s.split(",").toList.map(s => new Range(s.trim))
    }

    // no port restrictions
    assertEquals(-1, broker.getSuitablePort(ranges("")))
    assertEquals(100, broker.getSuitablePort(ranges("100..100")))
    assertEquals(100, broker.getSuitablePort(ranges("100..200")))

    // order
    assertEquals(10, broker.getSuitablePort(ranges("30,10,20,40")))
    assertEquals(50, broker.getSuitablePort(ranges("100..200, 50..60")))

    // single port restriction
    broker.port = new Range(92)
    assertEquals(-1, broker.getSuitablePort(ranges("0..91")))
    assertEquals(-1, broker.getSuitablePort(ranges("93..100")))
    assertEquals(92, broker.getSuitablePort(ranges("90..100")))

    // port range restriction
    broker.port = new Range("92..100")
    assertEquals(-1, broker.getSuitablePort(ranges("0..91")))
    assertEquals(-1, broker.getSuitablePort(ranges("101..200")))
    assertEquals(92, broker.getSuitablePort(ranges("0..100")))
    assertEquals(92, broker.getSuitablePort(ranges("0..92")))

    assertEquals(100, broker.getSuitablePort(ranges("100..200")))
    assertEquals(95, broker.getSuitablePort(ranges("0..90,95..96,101..200")))
    assertEquals(96, broker.getSuitablePort(ranges("0..90,96,101..200")))
  }

  @Test
  def shouldStart {
    val host = "host"
    // active
    broker.active = false
    assertFalse(broker.shouldStart(host))
    broker.active = true
    assertTrue(broker.shouldStart(host))

    // has task
    broker.task = new Task()
    assertFalse(broker.shouldStart(host))
    broker.task = null
    assertTrue(broker.shouldStart(host))

    // failover waiting delay
    val now = new Date(0)
    broker.failover.delay = new Period("1s")
    broker.registerStop(now, failed = true)
    assertTrue(broker.failover.isWaitingDelay(now))

    assertFalse(broker.failover.isWaitingDelay(now = new Date(now.getTime + broker.failover.delay.ms)))
    broker.failover.resetFailures()
    assertFalse(broker.failover.isWaitingDelay(now = now))
  }

  @Test
  def shouldStop {
    assertFalse(broker.shouldStop)

    broker.task = new Broker.Task()
    assertTrue(broker.shouldStop)

    broker.active = true
    assertFalse(broker.shouldStop)
  }

  @Test
  def state {
    assertEquals("stopped", broker.state())

    broker.task = Task()
    broker.task.state = State.STOPPING
    assertEquals("stopping", broker.state())

    broker.task = null
    broker.active = true
    assertEquals("starting", broker.state())

    broker.task = Task()
    assertEquals("pending", broker.state())

    broker.task.state = State.RUNNING
    assertEquals("running", broker.state())

    broker.task = null
    broker.failover.delay = new Period("1s")
    broker.failover.registerFailure(new Date(0))
    var state = broker.state(new Date(0))
    assertTrue(state, state.startsWith("failed 1"))

    state = broker.state(new Date(1000))
    assertTrue(state, state.startsWith("starting 2"))
  }

  @Test(timeout = 5000)
  def waitFor {
    def deferStateSwitch(state: String, delay: Long) {
      new Thread() {
        override def run() {
          setName(classOf[BrokerTest].getSimpleName + "-scheduleState")
          Thread.sleep(delay)

          if (state != null) {
            broker.task = Task()
            broker.task.state = state
          }
          else broker.task = null
        }
      }.start()
    }

    deferStateSwitch(Broker.State.RUNNING, 100)
    assertTrue(broker.waitFor(State.RUNNING, new Period("200ms")))

    deferStateSwitch(null, 100)
    assertTrue(broker.waitFor(null, new Period("200ms")))

    // timeout
    assertFalse(broker.waitFor(State.RUNNING, new Period("50ms")))
  }

  @Test
  def toJson_fromJson {
    broker.active = true
    broker.cpus = 0.5
    broker.mem = 128
    broker.heap = 128
    broker.port = new Range("0..100")
    broker.volume = "volume"
    broker.bindAddress = new Util.BindAddress("192.168.0.1")
    broker.syslog = true

    broker.constraints = parseMap("a=like:1").mapValues(new Constraint(_)).toMap
    broker.options = parseMap("a=1").toMap
    broker.log4jOptions = parseMap("b=2").toMap
    broker.failover.registerFailure(new Date())
    broker.task = Task("1", "slave", "executor", "host")

    broker.executionOptions = ExecutionOptions(
      container = Some(Container(
        ctype = ContainerType.Docker,
        name = "test",
        mounts = Seq(Mount("/a", "/b", MountMode.ReadWrite))
      )),
      javaCmd = "/usr/bin/java",
      jvmOptions = "-Xms512m"
    )

    val read = JsonUtil.fromJson[Broker](JsonUtil.toJson(broker))

    BrokerTest.assertBrokerEquals(broker, read)
  }

  // static part
  @Test
  def idFromTaskId {
    assertEquals(0, Broker.idFromTaskId(Broker.nextTaskId(new Broker(0))))
    assertEquals(100, Broker.idFromTaskId(Broker.nextTaskId(new Broker(100))))
  }

  // Reservation
  @Test
  def Reservation_toResources {
    // shared
    var reservation = new Broker.Reservation(null, sharedCpus = 0.5, sharedMem = 100, sharedPort = 1000)
    assertEquals(resources("cpus:0.5; mem:100; ports:1000"), reservation.toResources)

    // role
    reservation = new Broker.Reservation("role", roleCpus = 0.5, roleMem = 100, rolePort = 1000)
    assertEquals(resources("cpus(role):0.5; mem(role):100; ports(role):1000"), reservation.toResources)

    // shared + role
    reservation = new Broker.Reservation("role", sharedCpus = 0.3, roleCpus = 0.7, sharedMem = 50, roleMem = 100, sharedPort = 1000, rolePort = 2000)
    assertEquals(resources("cpus:0.3; cpus(role):0.7; mem:50; mem(role):100; ports:1000; ports(role):2000"), reservation.toResources)
  }

  // Stickiness
  @Test
  def Stickiness_matchesHostname {
    val stickiness = new Stickiness()
    assertTrue(stickiness.matchesHostname("host0"))
    assertTrue(stickiness.matchesHostname("host1"))

    stickiness.registerStart("host0")
    stickiness.registerStop(new Date(0))
    assertTrue(stickiness.matchesHostname("host0"))
    assertFalse(stickiness.matchesHostname("host1"))
  }

  @Test
  def Stickiness_sickyPeriod: Unit = {
    val stickiness = new Stickiness()
    assertEquals(stickiness.stickyTimeLeft(), 0)

    stickiness.registerStart("host0")
    stickiness.registerStop(new Date(0))
    val stickyTimeSec = (stickiness.period.ms() / 1000).toInt
    assertEquals(stickiness.stickyTimeLeft(new Date(0)), stickyTimeSec)
    assertEquals(stickiness.stickyTimeLeft(new Date(stickyTimeSec * 1000)), 0)
  }

  @Test
  def Stickiness_registerStart_registerStop {
    val stickiness = new Stickiness()
    assertNull(stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStart("host")
    assertEquals("host", stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStop(new Date(0))
    assertEquals("host", stickiness.hostname)
    assertEquals(new Date(0), stickiness.stopTime)

    stickiness.registerStart("host1")
    assertEquals("host1", stickiness.hostname)
    assertNull(stickiness.stopTime)
  }

  @Test
  def Stickiness_toJson_fromJson {
    val stickiness = new Stickiness()
    stickiness.registerStart("localhost")
    stickiness.registerStop(new Date(0))

    val read = JsonUtil.fromJson[Stickiness](JsonUtil.toJson(stickiness))
    BrokerTest.assertStickinessEquals(stickiness, read)
  }

  @Test
  def Stickiness_default_if_empty: Unit = {
    val json = "{\"id\": \"5\"}"
    val broker = JsonUtil.fromJson[Broker](json)
    assertNotNull(broker.stickiness)
    assertEquals(new Stickiness(), broker.stickiness)
  }

  // Failover
  @Test
  def Failover_currentDelay {
    val failover = new Failover(new Period("1s"), new Period("5s"))

    failover.failures = 0
    assertEquals(new Period("0s"), failover.currentDelay)

    failover.failures = 1
    assertEquals(new Period("1s"), failover.currentDelay)

    failover.failures = 2
    assertEquals(new Period("2s"), failover.currentDelay)

    failover.failures = 3
    assertEquals(new Period("4s"), failover.currentDelay)

    failover.failures = 4
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 32
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 33
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 100
    assertEquals(new Period("5s"), failover.currentDelay)

    // multiplier boundary
    failover.maxDelay = new Period(Integer.MAX_VALUE + "s")

    failover.failures = 30
    assertEquals(new Period((1 << 29) + "s"), failover.currentDelay)

    failover.failures = 31
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)

    failover.failures = 32
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)

    failover.failures = 100
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)
  }

  @Test
  def Failover_delayExpires {
    val failover = new Failover(new Period("1s"))
    assertEquals(new Date(0), failover.delayExpires)

    failover.registerFailure(new Date(0))
    assertEquals(new Date(1000), failover.delayExpires)

    failover.failureTime = new Date(1000)
    assertEquals(new Date(2000), failover.delayExpires)
  }

  @Test
  def Failover_isWaitingDelay {
    val failover = new Failover(new Period("1s"))
    assertFalse(failover.isWaitingDelay(new Date(0)))

    failover.registerFailure(new Date(0))

    assertTrue(failover.isWaitingDelay(new Date(0)))
    assertTrue(failover.isWaitingDelay(new Date(500)))
    assertTrue(failover.isWaitingDelay(new Date(999)))
    assertFalse(failover.isWaitingDelay(new Date(1000)))
  }

  @Test
  def Failover_isMaxTriesExceeded {
    val failover = new Failover()

    failover.failures = 100
    assertFalse(failover.isMaxTriesExceeded)

    failover.maxTries = 50
    assertTrue(failover.isMaxTriesExceeded)
  }

  @Test
  def Failover_registerFailure_resetFailures {
    val failover = new Failover()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure(new Date(1))
    assertEquals(1, failover.failures)
    assertEquals(new Date(1), failover.failureTime)

    failover.registerFailure(new Date(2))
    assertEquals(2, failover.failures)
    assertEquals(new Date(2), failover.failureTime)

    failover.resetFailures()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure()
    assertEquals(1, failover.failures)
  }

  @Test
  def Failover_toJson_fromJson {
    val failover = new Failover(new Period("1s"), new Period("5s"))
    failover.maxTries = 10
    failover.resetFailures()
    failover.registerFailure(new Date(0))

    val read = JsonUtil.fromJson[Failover](JsonUtil.toJson(failover))
    BrokerTest.assertFailoverEquals(failover, read)
  }

  // Task
  @Test
  def Task_toJson_fromJson {
    val task = Task("id", "slave", "executor", "host", parseMap("a=1,b=2").toMap)
    task.state = State.RUNNING
    task.endpoint = new Endpoint("localhost:9092")

    val read = JsonUtil.fromJson[Task](JsonUtil.toJson(task))
    BrokerTest.assertTaskEquals(task, read)
  }
}

object BrokerTest {
  def assertBrokerEquals(expected: Broker, actual: Broker) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.active, actual.active)

    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.heap, actual.heap)
    assertEquals(expected.port, actual.port)
    assertEquals(expected.volume, actual.volume)
    assertEquals(expected.bindAddress, actual.bindAddress)
    assertEquals(expected.syslog, actual.syslog)

    assertEquals(expected.constraints, actual.constraints)
    assertEquals(expected.options, actual.options)
    assertEquals(expected.log4jOptions, actual.log4jOptions)

    assertFailoverEquals(expected.failover, actual.failover)
    assertTaskEquals(expected.task, actual.task)

    assertEquals(expected.needsRestart, actual.needsRestart)
    assertEquals(expected.executionOptions, actual.executionOptions)
  }

  def assertFailoverEquals(expected: Failover, actual: Failover) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.delay, actual.delay)
    assertEquals(expected.maxDelay, actual.maxDelay)
    assertEquals(expected.maxTries, actual.maxTries)

    assertEquals(expected.failures, actual.failures)
    assertEquals(expected.failureTime, actual.failureTime)
  }

  def assertStickinessEquals(expected: Stickiness, actual: Stickiness) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.period, actual.period)
    assertEquals(expected.stopTime, actual.stopTime)
    assertEquals(expected.hostname, actual.hostname)
  }

  def assertTaskEquals(expected: Task, actual: Task) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.executorId, actual.executorId)
    assertEquals(expected.slaveId, actual.slaveId)

    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.endpoint, actual.endpoint)
    assertEquals(expected.attributes, actual.attributes)

    assertEquals(expected.state, actual.state)
  }

  def assertAccept(expected: OfferResult): Unit =
    assertTrue(expected.isInstanceOf[OfferResult.Accept])

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
