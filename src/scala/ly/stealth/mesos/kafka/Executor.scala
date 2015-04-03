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

import org.apache.mesos.{ExecutorDriver, MesosExecutorDriver}
import org.apache.mesos.Protos._
import java.io._
import org.apache.log4j._
import Util.Str

object Executor extends org.apache.mesos.Executor {
  val logger: Logger = Logger.getLogger(Executor.getClass)
  var server: BrokerServer = new KafkaServer()

  def registered(driver: ExecutorDriver, executor: ExecutorInfo, framework: FrameworkInfo, slave: SlaveInfo): Unit = {
    logger.info("[registered] framework:" + Str.framework(framework) + " slave:" + Str.slave(slave))
  }

  def reregistered(driver: ExecutorDriver, slave: SlaveInfo): Unit = {
    logger.info("[reregistered] " + Str.slave(slave))
  }

  def disconnected(driver: ExecutorDriver): Unit = {
    logger.info("[disconnected]")
  }

  def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    logger.info("[launchTask] " + Str.task(task))
    startBroker(driver, task)
  }

  def killTask(driver: ExecutorDriver, id: TaskID): Unit = {
    logger.info("[killTask] " + id.getValue)
    stopExecutor(driver, async = true)
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    logger.info("[frameworkMessage] " + new String(data))
    handleMessage(driver, new String(data))
  }

  def shutdown(driver: ExecutorDriver): Unit = {
    logger.info("[shutdown]")
    stopExecutor(driver)
  }

  def error(driver: ExecutorDriver, message: String): Unit = {
    logger.info("[error] " + message)
  }

  private[kafka] def startBroker(driver: ExecutorDriver, task: TaskInfo): Unit = {
    def runBroker0 {
      try {
        server.start(Util.parseMap(task.getData.toStringUtf8))

        var status = TaskStatus.newBuilder.setTaskId(task.getTaskId).setState(TaskState.TASK_RUNNING).build
        driver.sendStatusUpdate(status)

        server.waitFor()
        status = TaskStatus.newBuilder.setTaskId(task.getTaskId).setState(TaskState.TASK_FINISHED).build
        driver.sendStatusUpdate(status)
      } catch {
        case t: Throwable =>
          logger.warn("", t)
          sendTaskFailed(driver, task, t)
      } finally {
        stopExecutor(driver)
      }
    }

    new Thread {
      override def run() {
        setName("BrokerServer")
        runBroker0
      }
    }.start()
  }

  private[kafka] def stopExecutor(driver: ExecutorDriver, async: Boolean = false): Unit = {
    def stop0 {
      if (server.isStarted) server.stop()
      driver.stop()
    }

    if (async)
      new Thread() {
        override def run(): Unit = {
          setName("ExecutorStopper")
          stop0
        }
      }.start()
    else
      stop0
  }

  private[kafka] def handleMessage(driver: ExecutorDriver, message: String): Unit = {
    if (message == "stop") driver.stop()
  }

  private def sendTaskFailed(driver: ExecutorDriver, task: TaskInfo, t: Throwable) {
    val stackTrace = new StringWriter()
    t.printStackTrace(new PrintWriter(stackTrace, true))

    driver.sendStatusUpdate(TaskStatus.newBuilder
      .setTaskId(task.getTaskId).setState(TaskState.TASK_FAILED)
      .setMessage("" + stackTrace)
      .build
    )
  }

  def main(args: Array[String]) {
    configureLogging()

    val driver = new MesosExecutorDriver(Executor)
    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1

    System.exit(status)
  }

  private def configureLogging() {
    System.setProperty("log4j.ignoreTCL", "true") // fix  log4j class loading issue
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    val logger = Logger.getLogger(Executor.getClass.getPackage.getName)
    logger.setLevel(if (System.getProperty("debug") != null) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")
    root.addAppender(new ConsoleAppender(layout))
  }
}

