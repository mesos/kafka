package ly.stealth.mesos.kafka

import java.util
import scala.collection.JavaConversions._
import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskState, Resource}
import java.util.{Date, Properties}
import java.io.StringReader

class SchedulerTest extends MesosTest {
  @Test
  def newExecutor {
    val broker = new Broker("1")
    broker.heap = 512

    val executor = Scheduler.newExecutor(broker)
    val command = executor.getCommand
    assertTrue(command.getUrisCount > 0)

    val cmd: String = command.getValue
    assertTrue(cmd, cmd.contains("-Xmx" + broker.heap + "m"))
    assertTrue(cmd, cmd.contains(Executor.getClass.getName.replace("$", "")))
  }

  @Test
  def newTask {
    val broker = new Broker("1")
    broker.options = "a=1"
    broker.cpus = 0.5
    broker.mem = 256

    val offer = this.offer(slaveId = "slave", ports = Pair(1000, 1000))

    val task = Scheduler.newTask(broker, offer)
    assertEquals("slave", task.getSlaveId.getValue)
    assertNotNull(task.getExecutor)

    // resources
    val resources = new util.HashMap[String, Resource]()
    for (resource <- task.getResourcesList) resources.put(resource.getName, resource)

    val cpuResource = resources.get("cpus")
    assertNotNull(cpuResource)
    assertEquals(broker.cpus, cpuResource.getScalar.getValue, 0.001)

    val memResource = resources.get("mem")
    assertNotNull(memResource)
    assertEquals(broker.mem, memResource.getScalar.getValue.toInt)

    val portsResource = resources.get("ports")
    assertNotNull(portsResource)
    assertEquals(1, portsResource.getRanges.getRangeCount)

    val range = portsResource.getRanges.getRangeList.get(0)
    assertEquals(1000, range.getBegin)
    assertEquals(1000, range.getEnd)

    // options
    val options = new Properties()
    options.load(new StringReader(task.getData.toStringUtf8))
    assertEquals(broker.id, options.getProperty("broker.id"))
    assertEquals("" + 1000, options.getProperty("port"))

    assertEquals("kafka-logs", options.getProperty("log.dirs"))
    assertEquals("1", options.getProperty("a"))
  }

  @Test
  def syncBrokers {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val offer = this.offer(cpus = broker.cpus, mem = broker.mem, ports = Pair(100, 100))

    // broker !active
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(0, driver.launchedTasks.size())

    // broker active
    broker.active = true
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(1, driver.launchedTasks.size())
    assertEquals(0, driver.killedTasks.size())

    // broker !active
    broker.active = false
    Scheduler.syncBrokers(util.Arrays.asList())
    assertEquals(1, driver.launchedTasks.size())
    assertEquals(1, driver.killedTasks.size())
  }

  @Test
  def onBrokerStatus {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task(Broker.nextTaskId(broker))
    assertFalse(broker.task.running)

    // broker started
    Scheduler.onBrokerStatus(taskStatus(id = broker.task.id, state = TaskState.TASK_RUNNING))
    assertTrue(broker.task.running)

    // broker finished
    Scheduler.onBrokerStatus(taskStatus(id = broker.task.id, state = TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
  }

  @Test
  def onBrokerStarted {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task("task")
    assertFalse(broker.task.running)

    Scheduler.onBrokerStarted(broker, taskStatus(state = TaskState.TASK_RUNNING))
    assertTrue(broker.task.running)
  }

  @Test
  def onBrokerStopped {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val task = new Broker.Task("task", _running = true)

    // finished
    broker.task = task
    Scheduler.onBrokerStopped(broker, taskStatus(state = TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)

    // failed
    broker.active = true
    broker.task = task
    Scheduler.onBrokerStopped(broker, taskStatus(state = TaskState.TASK_FAILED), new Date(0))
    assertNull(broker.task)
    assertEquals(1, broker.failover.failures)
    assertEquals(new Date(0), broker.failover.failureTime)

    // failed maxRetries exceeded
    broker.failover.maxTries = 2
    broker.task = task
    Scheduler.onBrokerStopped(broker, taskStatus(state = TaskState.TASK_FAILED), new Date(1))
    assertNull(broker.task)
    assertEquals(2, broker.failover.failures)
    assertEquals(new Date(1), broker.failover.failureTime)

    assertTrue(broker.failover.isMaxTriesExceeded)
    assertFalse(broker.active)
  }

  @Test
  def launchTask {
    val broker = Scheduler.cluster.addBroker(new Broker("100"))
    val offer = this.offer(cpus = broker.cpus, mem = broker.mem, ports = Pair(1000, 1000))

    Scheduler.launchTask(broker, offer)
    assertEquals(1, driver.launchedTasks.size())

    assertNotNull(broker.task)
    assertFalse(broker.task.running)

    val task = driver.launchedTasks.get(0)
    assertEquals(task.getTaskId.getValue, broker.task.id)
    assertTrue(Scheduler.taskIds.contains(broker.task.id))
  }

  @Test
  def findBrokerPort {
    assertEquals(100, Scheduler.findBrokerPort(offer(ports = Pair(100, 200))))
    assertEquals(100, Scheduler.findBrokerPort(offer(ports = Pair(100, 100))))

    // no ports
    try { Scheduler.findBrokerPort(offer()); fail() }
    catch { case e: IllegalArgumentException => }
  }
}
