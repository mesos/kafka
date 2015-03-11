package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._
import ly.stealth.mesos.kafka.Util.Period
import java.util.Date
import ly.stealth.mesos.kafka.Broker.{Task, Failover}

class BrokerTest {
  @Test
  def attributeMap {
    val broker = new Broker()
    broker.attributes = "a:1;b:2"
    assertEquals(broker.attributeMap, Util.parseMap("a=1,b=2"))
  }

  @Test
  def optionMap {
    val broker = new Broker("id")
    broker.host = "host"
    broker.options = "a=$id;b=2;c=$host"
    assertEquals(broker.optionMap, Util.parseMap("a=id,b=2,c=host"))
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
    read.fromJson(broker.toJson.obj.asInstanceOf[Map[String, Object]])

    assertEquals(broker.id, read.id)
    assertEquals(broker.active, read.active)

    assertEquals(broker.host, read.host)
    assertEquals(broker.cpus, read.cpus, 0.001)
    assertEquals(broker.mem, read.mem)
    assertEquals(broker.heap, read.heap)

    assertEquals(broker.attributes, read.attributes)
    assertEquals(broker.options, read.options)

    assertFailoverEquals(broker.failover, read.failover)
    assertTaskEquals(broker.task, read.task)
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
    read.fromJson(failover.toJson.obj.asInstanceOf[Map[String, Object]])

    assertFailoverEquals(failover, read)
  }

  private def assertFailoverEquals(expected: Failover, actual: Failover) {
    if (expected == actual) return
    if (expected == null && actual != null) throw new AssertionError("actual != null")
    if (expected != null && actual == null) throw new AssertionError("actual == null")

    assertEquals(expected.delay, actual.delay)
    assertEquals(expected.maxDelay, actual.maxDelay)
    assertEquals(expected.maxTries, actual.maxTries)

    assertEquals(expected.failures, actual.failures)
    assertEquals(expected.failureTime, actual.failureTime)
  }

  // Task
  @Test
  def Task_toJson_fromJson {
    val task = new Task("id", "host", 9092)
    task.running = true

    val read: Task = new Task()
    read.fromJson(task.toJson.obj.asInstanceOf[Map[String, Object]])

    assertTaskEquals(task, read)
  }

  private def assertTaskEquals(expected: Task, actual: Task) {
    if (expected == actual) return
    if (expected == null && actual != null) throw new AssertionError("actual != null")
    if (expected != null && actual == null) throw new AssertionError("actual == null")

    assertEquals(expected.id, actual.id)
    assertEquals(expected.running, actual.running)
    assertEquals(expected.host, actual.host)
    assertEquals(expected.port, actual.port)
  }
}
