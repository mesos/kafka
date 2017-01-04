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
import org.apache.mesos.Protos.{Offer, TaskID, TaskState, TaskStatus}
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit
import ly.stealth.mesos.kafka.executor.{Executor, LaunchConfig}
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.mesos.{OfferManager, OfferResult}
import net.elodina.mesos.util.Period
import net.elodina.mesos.util.Strings.parseMap
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

class SchedulerTest extends KafkaMesosTestCase {
  @Test
  def newTask {
    val broker = new Broker("1")
    broker.options = parseMap("a=1").toMap
    broker.log4jOptions = parseMap("b=2").toMap
    broker.cpus = 0.5
    broker.mem = 256
    broker.heap = 512
    broker.jvmOptions = "-Xms64m"

    val offer = this.offer("id", "fw-id", "slave", "host", s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000", "")
    val reservation = broker.getReservation(offer)

    val task = registry.taskFactory.newTask(broker, offer, reservation)
    assertEquals("slave", task.getSlaveId.getValue)
    assertNotNull(task.getExecutor)

    // executor
    val command = task.getExecutor.getCommand
    assertTrue(command.getUrisCount > 0)

    val cmd: String = command.getValue
    assertTrue(cmd, cmd.contains("-Xmx" + broker.heap + "m"))
    assertTrue(cmd, cmd.contains(broker.jvmOptions))
    assertTrue(cmd, cmd.contains(Executor.getClass.getName.replace("$", "")))

    // resources
    assertEquals(resources(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000"), task.getResourcesList)

    // data
    val launchConfig = JsonUtil.fromJson[LaunchConfig](task.getData.toByteArray)

    assertEquals(broker.id, launchConfig.id)
    assertEquals(broker.options, launchConfig.options)
    assertEquals(broker.log4jOptions, launchConfig.log4jOptions)

    val defaults = launchConfig.interpolatedOptions
    assertEquals(broker.id, defaults("broker.id"))
    assertEquals("" + 1000, defaults("port"))
    assertEquals(Config.zk, defaults("zookeeper.connect"))

    assertEquals("kafka-logs", defaults("log.dirs"))
    assertEquals(offer.getHostname, defaults("host.name"))
  }

  @Test
  def syncBrokers {
    val broker = registry.cluster.addBroker(new Broker())
    val offer = this.offer(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000")

    // broker !active
    registry.brokerLifecycleManager.tryLaunchBrokers(Seq(offer))
    assertEquals(0, schedulerDriver.launchedTasks.size())

    // broker active
    broker.active = true
    registry.brokerLifecycleManager.tryLaunchBrokers(Seq(offer))
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(0, schedulerDriver.killedTasks.size())

    // broker !active
    broker.task = Broker.Task(id = "1")
    registry.brokerLifecycleManager.stopBroker(broker)
    assertTrue(broker.task.stopping)
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertEquals(1, schedulerDriver.killedTasks.size())
  }

  @Test
  def acceptOffer {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true

    broker.task = null
    var theOffer = offer(s"cpus:0.4; mem:${broker.mem}")
    assertEquals(
      Right(Seq(OfferResult.neverMatch(theOffer, broker, s"cpus < ${broker.cpus}"))),
      registry.offerManager.tryAcceptOffer(theOffer, Seq(broker)))
    theOffer = offer(s"cpus:${broker.cpus}; mem:99")
    assertEquals(
      Right(Seq(OfferResult.neverMatch(theOffer, broker, s"mem < ${broker.mem}"))),
      registry.offerManager.tryAcceptOffer(theOffer, Seq(broker)))

    theOffer = offer(s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000")
    assertTrue(registry.brokerLifecycleManager.tryLaunchBrokers(Seq(theOffer)))
    assertEquals(1, schedulerDriver.launchedTasks.size())

    theOffer = offer(s"cpus:${broker.cpus}; mem:${broker.mem}")
    assertEquals(
      Right(Seq(OfferResult.NoWork(theOffer))),
      registry.offerManager.tryAcceptOffer(theOffer, Seq(broker)))
  }

  @Test
  def onBrokerStatus {
    val broker = registry.cluster.addBroker(new Broker())
    broker.task = new Broker.Task(Broker.nextTaskId(broker), "slave", "executor", "host")
    assertEquals(Broker.State.STARTING, broker.task.state)

    // broker started
    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "localhost:9092"))
    assertEquals(Broker.State.RUNNING, broker.task.state)
    assertEquals("localhost:9092", "" + broker.task.endpoint)

    // broker finished
    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(broker.task.id, TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
  }

  @Test
  def onBrokerStarted {
    val broker = registry.cluster.addBroker(new Broker())
    broker.task = new Broker.Task(id = "0-" + UUID.randomUUID())
    assertEquals(Broker.State.STARTING, broker.task.state)

    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "localhost:9092"))
    assertEquals(Broker.State.RUNNING, broker.task.state)
    assertEquals("localhost:9092", "" + broker.task.endpoint)
  }

  @Test
  def onBrokerStopped {
    val broker = registry.cluster.addBroker(new Broker())
    val task = new Broker.Task(id = "0-" + UUID.randomUUID(), _state = Broker.State.RUNNING)

    // finished
    broker.task = task
    broker.needsRestart = true
    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(TaskState.TASK_FINISHED))
    assertNull(broker.task)
    assertEquals(0, broker.failover.failures)
    assertFalse(broker.needsRestart)

    // failed
    broker.active = true
    broker.task = task
    broker.needsRestart = true
    MockWallClock.overrideNow(Some(new Date(0)))
    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(TaskState.TASK_FAILED)) //, new Date(0))
    assertNull(broker.task)
    assertEquals(1, broker.failover.failures)
    assertEquals(new Date(0), broker.failover.failureTime)
    assertFalse(broker.needsRestart)

    // failed maxRetries exceeded
    broker.failover.maxTries = 2
    broker.task = task
    MockWallClock.overrideNow(Some(new Date(1)))
    registry.brokerLifecycleManager.onBrokerStatus(broker, taskStatus(TaskState.TASK_FAILED)) //, new Date(1))
    assertNull(broker.task)
    assertEquals(2, broker.failover.failures)
    assertEquals(new Date(1), broker.failover.failureTime)

    assertTrue(broker.failover.isMaxTriesExceeded)
    assertFalse(broker.active)
  }

  @Test
  def declineFailedBroker: Unit = {
    val broker = registry.cluster.addBroker(new Broker("0"))
  }

  @Test
  def launchTask {
    val broker = registry.cluster.addBroker(new Broker("100"))
    val offer = this.offer("id", "fw-id", "slave-id", "host", s"cpus:${broker.cpus}; mem:${broker.mem}; ports:1000", "a=1,b=2")
    broker.needsRestart = true
    broker.active = true
    registry.brokerLifecycleManager.tryLaunchBrokers(Seq(offer))
    //registry.scheduler.launchBroker(broker, offer)
    assertEquals(1, schedulerDriver.launchedTasks.size())
    assertFalse(broker.needsRestart)

    assertNotNull(broker.task)
    assertEquals(Broker.State.STARTING, broker.task.state)
    assertEquals(parseMap("a=1,b=2").toMap, broker.task.attributes)

    val task = schedulerDriver.launchedTasks.get(0)
    assertEquals(task.getTaskId.getValue, broker.task.id)
  }

  @Test
  def reconcileTasksIfRequired {
    val broker0 = registry.cluster.addBroker(new Broker("0"))

    val broker1 = registry.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(id = "1", _state = Broker.State.RUNNING)

    val broker2 = registry.cluster.addBroker(new Broker("2"))
    broker2.task = new Broker.Task(id = "2", _state = Broker.State.STARTING)

    MockWallClock.overrideNow(Some(new Date(0)))
    registry.taskReconciler.start()
    Thread.sleep(100)

    assertEquals(1, registry.taskReconciler.attempts)
    assertEquals(new Date(0), registry.taskReconciler.lastReconcile)

    assertNull(broker0.task)
    assertEquals(Broker.State.RECONCILING, broker1.task.state)
    assertEquals(Broker.State.RECONCILING, broker2.task.state)

    for (i <- 2 until Config.reconciliationAttempts + 1) {
      registry.taskReconciler.asInstanceOf[{def retryReconciliation()}].retryReconciliation()
      assertEquals(i, registry.taskReconciler.attempts)
      assertEquals(Broker.State.RECONCILING, broker1.task.state)
    }
    assertEquals(0, schedulerDriver.killedTasks.size())
    // last reconcile should stop broker
    registry.taskReconciler.asInstanceOf[{def retryReconciliation()}].retryReconciliation()
    assertEquals(Broker.State.STOPPING, broker1.task.state)
    assertEquals(2, schedulerDriver.killedTasks.size())
  }

  @Test
  def reconciliationFullRun = {
    Config.reconciliationTimeout = new Period("1ms")

    val mockRegistry = registry

    val broker0 = registry.cluster.addBroker(new Broker("0"))

    val broker1 = registry.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(id = "1", _state = Broker.State.RUNNING)

    val broker2 = registry.cluster.addBroker(new Broker("2"))
    broker2.task = new Broker.Task(id = "2", _state = Broker.State.STARTING)

    registry.taskReconciler.start().get
    while(registry.taskReconciler.isReconciling) {
      Thread.sleep(10)
    }

    assertEquals(Broker.State.STOPPING, broker1.task.state)
    assertEquals(2, schedulerDriver.killedTasks.size())
  }

  @Test
  def reconciliationSucceeds: Unit = {
    Config.reconciliationTimeout = new Period("100ms")

    val broker0 = registry.cluster.addBroker(new Broker("0"))

    val broker1 = registry.cluster.addBroker(new Broker("1"))
    broker1.task = new Broker.Task(id = "1", _state = Broker.State.RUNNING)

    registry.taskReconciler.start()
    while(!registry.taskReconciler.isReconciling) {
      Thread.sleep(10)
    }

    val status = TaskStatus.newBuilder()
    status.setState(TaskState.TASK_RUNNING)
    status.setTaskId(TaskID.newBuilder().setValue("1"))
    registry.brokerLifecycleManager.onBrokerStatus(broker1, status.build())

    while(registry.taskReconciler.isReconciling) {
      Thread.sleep(10)
    }

    assertFalse(broker1.task.reconciling)
    // Starting again should reset the attempts
    registry.taskReconciler.start()
    assertEquals(1, registry.taskReconciler.attempts)
  }

  @Test
  def otherTasksAttributes {
    val broker0 = registry.cluster.addBroker(new Broker("0"))
    broker0.task = Broker.Task(hostname = "host0", attributes = parseMap("a=1,b=2").toMap)

    val broker1 = registry.cluster.addBroker(new Broker("1"))
    broker1.task = Broker.Task(hostname = "host1", attributes = parseMap("b=3").toMap)

    val brokers = Seq(broker0, broker1)
    assertEquals(Seq("host0", "host1"), OfferManager.otherTasksAttributes("hostname", brokers))
    assertEquals(Seq("1"), OfferManager.otherTasksAttributes("a", brokers))
    assertEquals(Seq("2", "3"), OfferManager.otherTasksAttributes("b", brokers))
  }

  @Test
  def onFrameworkMessage = {
    val broker0 = registry.cluster.addBroker(new Broker("0"))
    broker0.active = true
    val broker1 = registry.cluster.addBroker(new Broker("1"))
    broker1.active = true

    val metrics0 = new Broker.Metrics(Map[String, Number](
      "underReplicatedPartitions" -> 2,
      "offlinePartitionsCount" -> 3,
      "activeControllerCount" -> 1
    ), System.currentTimeMillis())

    val data = JsonUtil.toJsonBytes(FrameworkMessage(metrics = Some(metrics0)))
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker0)), slaveId(), data)

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

    val metrics1 = new Broker.Metrics(Map(
      "offlinePartitionsCount" -> 1),
      System.currentTimeMillis()
    )
    val data1 = JsonUtil.toJsonBytes(FrameworkMessage(metrics=Some(metrics1)))
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker1)), slaveId(), data1)
  }

  @Test
  def sendReceiveBrokerLog = {
    val broker = registry.cluster.addBroker(new Broker("0"))
    broker.task = new Broker.Task("task-id", "slave-id", "executor-id")

    // driver connected
    val future = registry.scheduler.requestBrokerLog(broker, "stdout", 111, Duration(1, TimeUnit.SECONDS))
    assertEquals(1, schedulerDriver.sentFrameworkMessages.size())
    val message = schedulerDriver.sentFrameworkMessages.get(0)
    val messageData = LogRequest.parse(new String(message.data))
    val requestId = messageData.requestId
    assertEquals(broker.task.executorId, message.executorId)
    assertEquals(broker.task.slaveId, message.slaveId)
    assertEquals(LogRequest(requestId, 111, "stdout").toString, new String(message.data))

    val content = "1\n2\n3\n"
    val data = JsonUtil.toJsonBytes(FrameworkMessage(log = Some(LogResponse(requestId, content))))

    // skip log response when broker is null
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(new Broker("100"))), slaveId(), data)
    assertFalse(future.isCompleted)

    // skip log response when not active
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertFalse(future.isCompleted)

    // skip log response when no task
    broker.active = true
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertFalse(future.isCompleted)

    // skip log response when has task but no running
    broker.task = Broker.Task()
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertFalse(future.isCompleted)

    // broker has to be and task has to be running
    broker.task = Broker.Task(_state = Broker.State.RUNNING)
    registry.scheduler.frameworkMessage(schedulerDriver, executorId(Broker.nextExecutorId(broker)), slaveId(), data)
    assertTrue(future.isCompleted)
  }
}
