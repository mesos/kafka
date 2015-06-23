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
import scala.collection.JavaConversions._
import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{TaskState, Resource}
import java.util.Date

class SchedulerTest extends MesosTestCase {
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
    broker.options = Util.parseMap("a=1")
    broker.log4jOptions = Util.parseMap("b=2")
    broker.cpus = 0.5
    broker.mem = 256

    val offer = this.offer(slaveId = "slave", hostname = "host")

    val task = Scheduler.newTask(broker, offer, 1000)
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

    // data
    val data: util.Map[String, String] = Util.parseMap(task.getData.toStringUtf8)
    
    val readBroker: Broker = new Broker()
    readBroker.fromJson(Util.parseJson(data.get("broker")))
    BrokerTest.assertBrokerEquals(broker, readBroker)

    val defaults = Util.parseMap(data.get("defaults"))
    assertEquals(broker.id, defaults.get("broker.id"))
    assertEquals("" + 1000, defaults.get("port"))
    assertEquals(Config.zk, defaults.get("zookeeper.connect"))

    assertEquals("kafka-logs", defaults.get("log.dirs"))
    assertEquals(offer.getHostname, defaults.get("host.name"))
  }

  @Test
  def syncBrokers {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val offer = this.offer(cpus = broker.cpus, mem = broker.mem)

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
    assertEquals(s"broker ${broker.id}: cpus 0.4 < ${broker.cpus}", Scheduler.acceptOffer(offer(cpus = 0.4, mem = broker.mem)))
    assertEquals(s"broker ${broker.id}: mem 99 < ${broker.mem}", Scheduler.acceptOffer(offer(cpus = broker.cpus, mem = 99)))

    assertNull(Scheduler.acceptOffer(offer(cpus = broker.cpus, mem = broker.mem)))
    assertEquals(1, schedulerDriver.launchedTasks.size())

    assertEquals("", Scheduler.acceptOffer(offer(cpus = broker.cpus, mem = broker.mem)))
  }

  @Test
  def onBrokerStatus {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task(Broker.nextTaskId(broker), "slave", "executor", "host", 9092)
    assertEquals(Broker.State.STARTING, broker.task.state)

    // broker started
    Scheduler.onBrokerStatus(taskStatus(id = broker.task.id, state = TaskState.TASK_RUNNING))
    assertEquals(Broker.State.RUNNING, broker.task.state)

    // broker finished
    Scheduler.onBrokerStatus(taskStatus(id = broker.task.id, state = TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
  }

  @Test
  def onBrokerStarted {
    val broker = Scheduler.cluster.addBroker(new Broker())
    broker.task = new Broker.Task("task")
    assertEquals(Broker.State.STARTING, broker.task.state)

    Scheduler.onBrokerStarted(broker, taskStatus(id = broker.task.id, state = TaskState.TASK_RUNNING))
    assertEquals(Broker.State.RUNNING, broker.task.state)
  }

  @Test
  def onBrokerStopped {
    val broker = Scheduler.cluster.addBroker(new Broker())
    val task = new Broker.Task("task", _state = Broker.State.RUNNING)

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
    val offer = this.offer(cpus = broker.cpus, mem = broker.mem, attributes = "a=1,b=2")

    Scheduler.launchTask(broker, offer)
    assertEquals(1, schedulerDriver.launchedTasks.size())

    assertNotNull(broker.task)
    assertEquals(Broker.State.STARTING, broker.task.state)
    assertEquals(Util.parseMap("a=1,b=2"), broker.task.attributes)

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

    for (i <- 2 until Scheduler.RECONCILE_MAX_TRIES + 1) {
      Scheduler.reconcileTasksIfRequired(now = new Date(Scheduler.RECONCILE_DELAY.ms * i))
      assertEquals(i, Scheduler.reconciles)
      assertEquals(Broker.State.RECONCILING, broker1.task.state)
    }
    assertEquals(0, schedulerDriver.killedTasks.size())

    // last reconcile should stop broker
    Scheduler.reconcileTasksIfRequired(now = new Date(Scheduler.RECONCILE_DELAY.ms * (Scheduler.RECONCILE_MAX_TRIES + 1)))
    assertNull(broker1.task)
    assertEquals(2, schedulerDriver.killedTasks.size())
  }

  @Test
  def otherTasksAttributes {
    val broker0 = Scheduler.cluster.addBroker(new Broker("0"))
    broker0.task = new Broker.Task(_hostname = "host0", _attributes = Util.parseMap("a=1,b=2"))

    val broker1 = Scheduler.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(_hostname = "host1", _attributes = Util.parseMap("b=3"))

    assertArrayEquals(Array[AnyRef]("host0", "host1"), Scheduler.otherTasksAttributes("hostname").asInstanceOf[Array[AnyRef]])
    assertArrayEquals(Array[AnyRef]("1"), Scheduler.otherTasksAttributes("a").asInstanceOf[Array[AnyRef]])
    assertArrayEquals(Array[AnyRef]("2", "3"), Scheduler.otherTasksAttributes("b").asInstanceOf[Array[AnyRef]])
  }
}
