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

import ly.stealth.mesos.kafka.executor.{Executor, LaunchConfig}
import ly.stealth.mesos.kafka.json.JsonUtil
import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos.{Status, TaskState}
import scala.collection.JavaConversions._

class ExecutorTest extends KafkaMesosTestCase {
  @Test(timeout = 5000)
  def startBroker_success {
    val data = JsonUtil.toJson(LaunchConfig(0))
    Executor.startBroker(executorDriver, task("id", "task", "slave", data))
    executorDriver.waitForStatusUpdates(2)
    assertEquals(2, executorDriver.statusUpdates.size())

    assertEquals(
      Seq(TaskState.TASK_STARTING, TaskState.TASK_RUNNING),
      executorDriver.statusUpdates.map(_.getState))
    assertTrue(Executor.server.isStarted)

    Executor.server.stop()
    executorDriver.waitForStatusUpdates(3)

    assertEquals(3, executorDriver.statusUpdates.size())
    val status = executorDriver.statusUpdates.get(2)
    assertEquals(TaskState.TASK_FINISHED, status.getState)
    assertFalse(Executor.server.isStarted)
  }

  @Test(timeout = 5000)
  def startBroker_failure {
    Executor.server.asInstanceOf[TestBrokerServer].failOnStart = true
    Executor.startBroker(executorDriver, task())

    executorDriver.waitForStatusUpdates(1)
    assertEquals(1, executorDriver.statusUpdates.size())

    val status = executorDriver.statusUpdates.get(0)
    assertEquals(TaskState.TASK_FAILED, status.getState)
    assertFalse(Executor.server.isStarted)
  }

  @Test
  def stopExecutor {
    Executor.server.start(null, null)
    assertTrue(Executor.server.isStarted)
    assertEquals(Status.DRIVER_RUNNING, executorDriver.status)

    Executor.stopExecutor(executorDriver)
    assertFalse(Executor.server.isStarted)
    assertEquals(Status.DRIVER_STOPPED, executorDriver.status)

    Executor.stopExecutor(executorDriver) // no error
    assertEquals(Status.DRIVER_STOPPED, executorDriver.status)
  }

  @Test(timeout = 5000)
  def launchTask {
    val data = JsonUtil.toJson(LaunchConfig(0))
    Executor.launchTask(executorDriver, task("id", "task", "slave", data))

    executorDriver.waitForStatusUpdates(1)
    assertTrue(Executor.server.isStarted)
  }

  @Test(timeout = 5000)
  def killTask {
    Executor.server.start(null, null)
    Executor.killTask(executorDriver, taskId())

    Executor.server.waitFor()
    assertFalse(Executor.server.isStarted)
  }

  @Test
  def shutdown {
    Executor.server.start(null, null)
    Executor.shutdown(executorDriver)
    assertFalse(Executor.server.isStarted)
  }
}
