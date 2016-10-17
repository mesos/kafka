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

import java.util
import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.TaskState
import java.util.Date
import net.elodina.mesos.util.Strings.parseMap

class SchedulerTest extends KafkaMesosTestCase {
  @Test
  def newExecutor {
    val broker = new Broker("1")
    broker.heap = 512
    broker.jvmOptions = "-Xms64m"

    val executor = Scheduler.newExecutor(broker)
    val command = executor.getCommand
    assertTrue(command.getUrisCount > 0)

    val cmd: String = command.getValue
    assertTrue(cmd, cmd.contains("-Xmx" + broker.heap + "m"))
    assertTrue(cmd, cmd.contains(broker.jvmOptions))
    assertTrue(cmd, cmd.contains(Executor.getClass.getName.replace("$", "")))
  }

  @Test
  def newTask {
    val broker = new Broker("1")
    broker.options = parseMap("a=1")
    broker.log4jOptions = parseMap("b=2")
    broker.cpus = 0.5
    broker.mem = 256

    val offer = this.offer("id", "fw-id", "slave", "host", s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000", "")
    val reservation = broker.getReservation(offer)

    val task = Scheduler.newTask(broker, offer, reservation)
    assertEquals("slave", task.getSlaveId.getValue)
    assertNotNull(task.getExecutor)

    // resources
    assertEquals(resources(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000"), task.getResourcesList)

    // data
    val data: util.Map[String, String] = parseMap(task.getData.toStringUtf8)
    
    val readBroker: Broker = new Broker()
    readBroker.fromJson(Util.parseJson(data.get("broker")))
    BrokerTest.assertBrokerEquals(broker, readBroker)

    val defaults = parseMap(data.get("defaults"))
    assertEquals(broker.id, defaults.get("broker.id"))
    assertEquals("" + 1000, defaults.get("port"))
    assertEquals(Config.zk, defaults.get("zookeeper.connect"))

    assertEquals("kafka-logs", defaults.get("log.dirs"))
    assertEquals(offer.getHostname, defaults.get("host.name"))
  }

  @Test
  def syncBrokers {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val offer = this.offer(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000")

    // broker !active
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(0, schedulerDriver.launchedTasks.size())

    // broker active
    broker.active = true
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(0, schedulerDriver.killedTasks.size())

    // broker !active
    broker.active = false
    Scheduler.syncBrokers(util.Arrays.asList())
    assertTrue(broker.task.stopping)
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(1, schedulerDriver.killedTasks.size())
  }

  @Test
  def acceptOffer {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.active = true

    broker.task = new Broker.Task(_state = Broker.State.RECONCILING)
    assertEquals("reconciling", Scheduler.acceptOffer(null))

    broker.task = null
    assertEquals(s"broker ${broker.id}: cpus < ${broker.cpus}", Scheduler.acceptOffer(offer(s"cpus:0.4; mem:${broker.mem}")))
    assertEquals(s"broker ${broker.id}: mem < ${broker.mem}", Scheduler.acceptOffer(offer(s"cpus:${broker.cpus}; mem:99")))

    assertNull(Scheduler.acceptOffer(offer(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000")))
    assertEquals(1, schedulerDriver.launchedTasks.size())

    assertEquals("", Scheduler.acceptOffer(offer(s"cpus:${broker.cpus}; mem:${broker.mem}")))
  }

  @Test
  def onBrokerStatus {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task(Broker.nextTaskId(broker), "slave", "executor", "host")
    assertEquals(Broker.State.STARTING, broker.task.state)

    // broker started
    Scheduler.onBrokerStatus(taskStatus(broker.task.id, TaskState.TASK_RUNNING, "localhost:9092"))
    assertEquals(Broker.State.RUNNING, broker.task.state)
    assertEquals("localhost:9092", "" + broker.task.endpoint)

    // broker finished
    Scheduler.onBrokerStatus(taskStatus(broker.task.id, TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
  }

  @Test
  def onBrokerStarted {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task("task")
    assertEquals(Broker.State.STARTING, broker.task.state)

    Scheduler.onBrokerStarted(broker, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "localhost:9092"))
    assertEquals(Broker.State.RUNNING, broker.task.state)
    assertEquals("localhost:9092", "" + broker.task.endpoint)
  }

  @Test
  def onBrokerStopped {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val task = new Broker.Task("task", _state = Broker.State.RUNNING)

    // finished
    broker.task = task
    broker.needsRestart = true
    Scheduler.onBrokerStopped(broker, taskStatus(TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
    assertFalse(broker.needsRestart)

    // failed
    broker.active = true
    broker.task = task
    broker.needsRestart = true
    Scheduler.onBrokerStopped(broker, taskStatus(TaskState.TASK_FAILED), new Date(0))
    assertNull(broker.task)
    assertEquals(1, broker.failover.failures)
    assertEquals(new Date(0), broker.failover.failureTime)
    assertFalse(broker.needsRestart)

    // failed maxRetries exceeded
    broker.failover.maxTries = 2
    broker.task = task
    Scheduler.onBrokerStopped(broker, taskStatus(TaskState.TASK_FAILED), new Date(1))
    assertNull(broker.task)
    assertEquals(2, broker.failover.failures)
    assertEquals(new Date(1), broker.failover.failureTime)

    assertTrue(broker.failover.isMaxTriesExceeded)
    assertFalse(broker.active)
  }

  @Test
  def launchTask {
    val broker = Scheduler.cluster.addBroker(new Broker("100"))
    val offer = this.offer("id", "fw-id", "slave-id", "host", s"cpus:${broker.cpus}; mem:${broker.mem}", "a=1,b=2")
    broker.needsRestart = true

    Scheduler.launchTask(broker, offer)
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertFalse(broker.needsRestart)

    assertNotNull(broker.task)
    assertEquals(Broker.State.STARTING, broker.task.state)
    assertEquals(parseMap("a=1,b=2"), broker.task.attributes)

    val task = schedulerDriver.launchedTasks.get(0)
    assertEquals(task.getTaskId.getValue, broker.task.id)
  }

  @Test
  def reconcileTasksIfRequired {
    Scheduler.reconcileTime = null
    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))

    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(_id = "1", _state = Broker.State.RUNNING)

    val broker2 = Scheduler.cluster.addBroker(new Broker("2"))
    broker2.task = new Broker.Task(_id = "2", _state = Broker.State.STARTING)

    Scheduler.reconcileTasksIfRequired(force = true, now = new Date(0))
    assertEquals(1, Scheduler.reconciles)
    assertEquals(new Date(0), Scheduler.reconcileTime)

    assertNull(broker0.task)
    assertEquals(Broker.State.RECONCILING, broker1.task.state)
    assertEquals(Broker.State.RECONCILING, broker2.task.state)

    for (i <- 2 until Config.reconciliationAttempts + 1) {
      Scheduler.reconcileTasksIfRequired(now = new Date(Config.reconciliationTimeout.ms * i))
      assertEquals(i, Scheduler.reconciles)
      assertEquals(Broker.State.RECONCILING, broker1.task.state)
    }
    assertEquals(0, schedulerDriver.killedTasks.size())

    // last reconcile should stop broker
    Scheduler.reconcileTasksIfRequired(now = new Date(Config.reconciliationTimeout.ms * (Config.reconciliationAttempts + 1)))
    assertNull(broker1.task)
    assertEquals(2, schedulerDriver.killedTasks.size())
  }

  @Test
  def otherTasksAttributes {
    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))
    broker0.task = new Broker.Task(_hostname = "host0", _attributes = parseMap("a=1,b=2"))

    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(_hostname = "host1", _attributes = parseMap("b=3"))

    assertEquals(util.Arrays.asList("host0", "host1"), Scheduler.otherTasksAttributes("hostname"))
    assertEquals(util.Arrays.asList("1"), Scheduler.otherTasksAttributes("a"))
    assertEquals(util.Arrays.asList("2", "3"), Scheduler.otherTasksAttributes("b"))
  }

  @Test
  def onFrameworkMessage = {
    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))
    broker0.active = true
    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))
    broker1.active = true

    val metrics0 = new Broker.Metrics()
    metrics0.fromJson(Map[String, Object](
      "underReplicatedPartitions" -> 2.asInstanceOf[Object],
      "offlinePartitionsCount" -> 3.asInstanceOf[Object],
      "activeControllerCount" -> 1.asInstanceOf[Object],
      "timestamp" -> System.currentTimeMillis().asInstanceOf[Object]
    ))

    val data = scala.util.parsing.json.JSONObject(Map("metrics" -> metrics0.toJson)).toString().getBytes

    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker0)), slaveId(), data)

    // metrics updated for corresponding broker
    assertNotNull(broker0.metrics)

    def assertMetricsEquals(expected: Broker.Metrics, actual: Broker.Metrics): Unit = {
      assertEquals(expected("underReplicatedPartitions"), actual("underReplicatedPartitions"))
      assertEquals(expected("offlinePartitionsCount"), actual("offlinePartitionsCount"))
      assertEquals(expected("activeControllerCount"), actual("activeControllerCount"))
      assertEquals(expected.timestamp, actual.timestamp)
    }

    assertMetricsEquals(metrics0, broker0.metrics)

    // metrics updated only for active brokers
    broker1.active = false

    val metrics1 = new Broker.Metrics()
    metrics1.fromJson(Map(
      "offlinePartitionsCount" -> 1.asInstanceOf[Object],
      "timestamp" -> System.currentTimeMillis().asInstanceOf[Object]
    ))

    val data1 = scala.util.parsing.json.JSONObject(Map("metrics" -> metrics1.toJson)).toString().getBytes

    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker1)), slaveId(), data1)
  }

  @Test
  def sendReceiveBrokerLog = {
    val broker = Scheduler.cluster.addBroker(new Broker("0"))
    broker.task = new Broker.Task("task-id", "slave-id", "executor-id")

    // driver connected
    val requestId = Scheduler.requestBrokerLog(broker, "stdout", 111)
    assertEquals(1, schedulerDriver.sentFrameworkMessages.size())
    val message = schedulerDriver.sentFrameworkMessages.get(0)
    assertEquals(broker.task.executorId, message.executorId)
    assertEquals(broker.task.slaveId, message.slaveId)
    assertEquals(LogRequest(requestId, 111, "stdout").toString, new String(message.data))

    val content = "1\n2\n3\n"
    val data = LogResponse(requestId, content).toJson.toString().getBytes

    // skip log response when broker is null
    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(new Broker("100"))), slaveId(), data)
    assertEquals(None, Scheduler.logs.get(requestId))

    // skip log response when not active
    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertEquals(None, Scheduler.logs.get(requestId))

    // skip log response when no task
    broker.active = true
    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertEquals(None, Scheduler.logs.get(requestId))

    // skip log response when has task but no running
    broker.task = new Broker.Task()
    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertEquals(None, Scheduler.logs.get(requestId))

    // broker has to be and task has to be running
    broker.task = new Broker.Task(_state = Broker.State.RUNNING)
    Scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertEquals(Some(content), Scheduler.logs.get(requestId))

    // driver disconnected
    Scheduler.disconnected(schedulerDriver)
    val sizeBeforeRequest = Scheduler.logs.size()
    assertEquals(-1L, Scheduler.requestBrokerLog(broker, "stdout", 1))
    assertEquals(sizeBeforeRequest, Scheduler.logs.size())
  }
}
