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

import org.junit.Test
import org.junit.Assert._
import ly.stealth.mesos.kafka.Util.Period
import java.util.Date
import ly.stealth.mesos.kafka.Broker.{Task, Failover}

class BrokerTest extends MesosTestCase {
  @Test
  def attributeMap {
    val broker = new Broker()
    broker.attributes = "a:1;b:2"
    assertEquals(broker.attributeMap, Util.parseMap("a=1,b=2"))
  }

  @Test
  def optionMap {
    val broker = new Broker("id")

    // $var substitution
    broker.host = "host"
    broker.options = "a=$id;b=2;c=$host"
    assertEquals(broker.optionMap(), Util.parseMap("a=id,b=2,c=host,log.dirs=kafka-logs"))

    // log.dirs override
    broker.options = "log.dirs=logs"
    assertEquals(broker.optionMap(), Util.parseMap("log.dirs=logs"))

    // option override
    broker.options = "a=1;log.dirs=logs"
    assertEquals(broker.optionMap(Util.parseMap("a=2")), Util.parseMap("a=2,log.dirs=logs"))
  }

  @Test
  def matches {
    val broker = new Broker()
    broker.host = null
    broker.cpus = 0
    broker.mem = 0

    // host
    assertTrue(broker.matches(offer(host = "master")))
    assertTrue(broker.matches(offer(host = "slave")))

    broker.host = "master"
    assertTrue(broker.matches(offer(host = "master")))
    assertFalse(broker.matches(offer(host = "slave")))

    broker.host = "master*"
    assertTrue(broker.matches(offer(host = "master")))
    assertTrue(broker.matches(offer(host = "master-2")))
    assertFalse(broker.matches(offer(host = "slave")))

    broker.host = null

    // cpus
    broker.cpus = 0.5
    assertTrue(broker.matches(offer(cpus = 0.5)))
    assertFalse(broker.matches(offer(cpus = 0.49)))
    broker.cpus = 0

    // mem
    broker.mem = 100
    assertTrue(broker.matches(offer(mem = 100)))
    assertFalse(broker.matches(offer(mem = 99)))
    broker.mem = 0

    // attributes
    broker.attributes = "rack:1-*"
    assertTrue(broker.matches(offer(attributes = "rack:1-1")))
    assertTrue(broker.matches(offer(attributes = "rack:1-2")))
    assertFalse(broker.matches(offer(attributes = "rack:2-1")))
  }

  @Test
  def shouldStart {
    val broker = new Broker()
    val offer = this.offer(cpus = broker.cpus, mem = broker.mem.toInt)

    // active
    broker.active = false
    assertFalse(broker.shouldStart(offer))
    broker.active = true
    assertTrue(broker.shouldStart(offer))

    // has task
    broker.task = new Task()
    assertFalse(broker.shouldStart(offer))
    broker.task = null
    assertTrue(broker.shouldStart(offer))

    // matches offer
    broker.mem += 100
    assertFalse(broker.shouldStart(offer))
    broker.mem -= 100
    assertTrue(broker.shouldStart(offer))

    // failover waiting delay
    val now = new Date(0)
    broker.failover.delay = new Period("1s")
    broker.failover.registerFailure(now)
    assertTrue(broker.failover.isWaitingDelay(now))

    assertFalse(broker.shouldStart(offer, now))
    assertTrue(broker.shouldStart(offer, new Date(now.getTime + broker.failover.delay.ms)))
    broker.failover.resetFailures()
    assertTrue(broker.shouldStart(offer, now))
  }

  @Test
  def shouldStop {
    val broker = new Broker()
    assertTrue(broker.shouldStop)

    broker.active = true
    assertFalse(broker.shouldStop)
  }

  @Test
  def state {
    val broker = new Broker()
    assertEquals("stopped", broker.state())

    broker.task = new Task()
    assertEquals("stopping", broker.state())

    broker.task = null
    broker.active = true
    assertEquals("starting", broker.state())

    broker.task = new Task()
    assertEquals("starting", broker.state())

    broker.task.running = true
    assertEquals("running", broker.state())

    broker.task = null
    broker.failover.delay = new Period("1s")
    broker.failover.registerFailure(new Date(0))
    var state = broker.state(new Date(0))
    assertTrue(state, state.startsWith("failed 1"))

    state = broker.state(new Date(1000))
    assertTrue(state, state.startsWith("starting 2"))
  }

  @Test
  def waitFor {
    val broker = new Broker()

    def scheduleStateSwitch(running: Boolean, delay: Long) {
      new Thread() {
        override def run() {
          setName(classOf[BrokerTest].getSimpleName + "-scheduleState")
          Thread.sleep(delay)

          if (running) {
            broker.task = new Task()
            broker.task.running = running
          } else
            broker.task = null
        }
      }.start()
    }

    scheduleStateSwitch(running = true, 100)
    assertTrue(broker.waitFor(running = true, new Period("200ms")))

    scheduleStateSwitch(running = false, 100)
    assertTrue(broker.waitFor(running = false, new Period("200ms")))

    // timeout
    assertFalse(broker.waitFor(running = true, new Period("50ms")))
  }

  @Test
  def toJson_fromJson {
    val broker = new Broker("1")
    broker.active = true

    broker.host = "host"
    broker.cpus = 0.5
    broker.mem = 128
    broker.heap = 128

    broker.attributes = "a:1"
    broker.options = "a=1"

    broker.failover.registerFailure(new Date(0))
    broker.task = new Task("1", "host", 9092)

    val read: Broker = new Broker()
    read.fromJson(Util.parseJson("" + broker.toJson))

    BrokerTest.assertBrokerEquals(broker, read)
  }

  // static part
  @Test
  def idFromTaskId {
    assertEquals("0", Broker.idFromTaskId(Broker.nextTaskId(new Broker("0"))))
    assertEquals("100", Broker.idFromTaskId(Broker.nextTaskId(new Broker("100"))))
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

    failover.failures = 100
    assertEquals(new Period("5s"), failover.currentDelay)
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
  }

  @Test
  def Failover_toJson_fromJson {
    val failover = new Failover(new Period("1s"), new Period("5s"))
    failover.maxTries = 10
    failover.registerFailure(new Date(0))

    val read: Failover = new Failover()
    read.fromJson(Util.parseJson("" + failover.toJson))

    BrokerTest.assertFailoverEquals(failover, read)
  }

  // Task
  @Test
  def Task_toJson_fromJson {
    val task = new Task("id", "host", 9092)
    task.running = true

    val read: Task = new Task()
    read.fromJson(Util.parseJson("" + task.toJson))

    BrokerTest.assertTaskEquals(task, read)
  }

}

object BrokerTest {
  def assertBrokerEquals(expected: Broker, actual: Broker) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.active, actual.active)

    assertEquals(expected.host, actual.host)
    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.heap, actual.heap)

    assertEquals(expected.attributes, actual.attributes)
    assertEquals(expected.options, actual.options)

    assertFailoverEquals(expected.failover, actual.failover)
    assertTaskEquals(expected.task, actual.task)
  }

  def assertFailoverEquals(expected: Failover, actual: Failover) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.delay, actual.delay)
    assertEquals(expected.maxDelay, actual.maxDelay)
    assertEquals(expected.maxTries, actual.maxTries)

    assertEquals(expected.failures, actual.failures)
    assertEquals(expected.failureTime, actual.failureTime)
  }

  def assertTaskEquals(expected: Task, actual: Task) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.running, actual.running)
    assertEquals(expected.host, actual.host)
    assertEquals(expected.port, actual.port)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
