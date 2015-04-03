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

import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}
import java.util
import com.google.protobuf.ByteString
import java.util.Date
import scala.collection.JavaConversions._
import Util.Str

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val cluster: Cluster = new Cluster()
  cluster.load(clearTasks = true)

  private var driver: SchedulerDriver = null

  private[kafka] def newExecutor(broker: Broker): ExecutorInfo = {
    var cmd = "java -cp " + HttpServer.jar.getName
    cmd += " -Xmx" + broker.heap + "m"

    if (Config.debug) cmd += " -Ddebug"
    cmd += " ly.stealth.mesos.kafka.Executor"

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder.setValue(Broker.nextExecutorId(broker)))
      .setCommand(
        CommandInfo.newBuilder
          .addUris(CommandInfo.URI.newBuilder().setValue(Config.schedulerUrl + "/executor/" + HttpServer.jar.getName))
          .addUris(CommandInfo.URI.newBuilder().setValue(Config.schedulerUrl + "/kafka/" + HttpServer.kafkaDist.getName))
          .setValue(cmd)
      )
      .setName("BrokerExecutor")
      .build()
  }

  private[kafka] def newTask(broker: Broker, offer: Offer): TaskInfo = {
    val port = findBrokerPort(offer)

    def taskData: ByteString = {
      val defaults: Map[String, String] = Map(
        "broker.id" -> broker.id,
        "port" -> ("" + port),
        "log.dirs" -> "kafka-logs",

        "zookeeper.connect" -> Config.kafkaZkConnect,
        "host.name" -> offer.getHostname
      )

      val options = Util.formatMap(broker.options(defaults))
      ByteString.copyFromUtf8(options)
    }

    val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
      .setName("BrokerTask")
      .setTaskId(TaskID.newBuilder.setValue(Broker.nextTaskId(broker)).build)
      .setSlaveId(offer.getSlaveId)
      .setData(taskData)
      .setExecutor(newExecutor(broker))

    taskBuilder
      .addResources(Resource.newBuilder.setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(broker.cpus)))
      .addResources(Resource.newBuilder.setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder.setValue(broker.mem)))
      .addResources(Resource.newBuilder.setName("ports").setType(Value.Type.RANGES).setRanges(
      Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(port).setEnd(port)))
      )

    taskBuilder.build
  }

  def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo): Unit = {
    logger.info("[registered] framework:" + Str.id(id.getValue) + " master:" + Str.master(master))
    this.driver = driver
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    logger.info("[reregistered] master:" + Str.master(master))
    this.driver = driver
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    logger.info("[resourceOffers]\n" + Str.offers(offers))
    syncBrokers(offers)
  }

  def offerRescinded(driver: SchedulerDriver, id: OfferID): Unit = {
    logger.info("[offerRescinded] " + Str.id(id.getValue))
  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    logger.info("[statusUpdate] " + Str.taskStatus(status))
    onBrokerStatus(status)
  }

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = {
    logger.info("[frameworkMessage] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " data: " + new String(data))
  }

  def disconnected(driver: SchedulerDriver): Unit = {
    logger.info("[disconnected]")
    this.driver = null
  }

  def slaveLost(driver: SchedulerDriver, id: SlaveID): Unit = {
    logger.info("[slaveLost] " + Str.id(id.getValue))
  }

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = {
    logger.info("[executorLost] executor:" + Str.id(executorId.getValue) + " slave:" + Str.id(slaveId.getValue) + " status:" + status)
  }

  def error(driver: SchedulerDriver, message: String): Unit = {
    logger.info("[error] " + message)
  }

  private[kafka] def syncBrokers(offers: util.List[Offer]): Unit = {
    def startBroker(offer: Offer): Boolean = {
      for (broker <- cluster.getBrokers) {
        if (broker.shouldStart(offer, otherTasksAttributes)) {
          launchTask(broker, offer)
          return true
        }
      }

      false
    }

    for (offer <- offers) {
      val started = startBroker(offer)
      if (!started) driver.declineOffer(offer.getId)
    }

    for (broker <- cluster.getBrokers) {
      if (broker.shouldStop) {
        logger.info(s"Stopping broker ${broker.id}: killing task ${broker.task.id}")
        driver.killTask(TaskID.newBuilder.setValue(broker.task.id).build)
        broker.task.stopping = true
      }
    }

    cluster.save()
  }

  private[kafka] def onBrokerStatus(status: TaskStatus): Unit = {
    val broker = cluster.getBroker(Broker.idFromTaskId(status.getTaskId.getValue))

    status.getState match {
      case TaskState.TASK_RUNNING =>
        onBrokerStarted(broker, status)
      case TaskState.TASK_LOST | TaskState.TASK_FINISHED |
           TaskState.TASK_FAILED | TaskState.TASK_KILLED |
           TaskState.TASK_ERROR =>
        onBrokerStopped(broker, status)
      case _ => logger.warn("Got unexpected task state: " + status.getState)
    }

    cluster.save()
  }

  private[kafka] def onBrokerStarted(broker: Broker, status: TaskStatus): Unit = {
    broker.task.running = true
    broker.failover.resetFailures()
  }

  private[kafka] def onBrokerStopped(broker: Broker, status: TaskStatus, now: Date = new Date()): Unit = {
    broker.task = null
    val failed = broker.active && status.getState != TaskState.TASK_FINISHED && status.getState != TaskState.TASK_KILLED

    if (failed) {
      broker.failover.registerFailure(now)

      var msg = s"Broker ${broker.id} failed to start ${broker.failover.failures}"
      if (broker.failover.maxTries != null) msg += "/" + broker.failover.maxTries

      if (!broker.failover.isMaxTriesExceeded) {
        msg += ", waiting " + broker.failover.currentDelay
        msg += ", next start ~ " + Str.dateTime(broker.failover.delayExpires)
      } else {
        broker.active = false
        msg += ", failure limit exceeded"
        msg += ", deactivating broker"
      }

      logger.info(msg)
    }
  }

  private[kafka] def launchTask(broker: Broker, offer: Offer): Unit = {
    val task_ = newTask(broker, offer)
    val id = task_.getTaskId.getValue

    val attributes = new util.LinkedHashMap[String, String]()
    for (attribute <- offer.getAttributesList)
      if (attribute.hasText) attributes.put(attribute.getName, attribute.getText.getValue)

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task_))
    broker.task = new Broker.Task(id, task_.getSlaveId.getValue, task_.getExecutor.getExecutorId.getValue, offer.getHostname, findBrokerPort(offer), attributes)

    logger.info(s"Starting broker ${broker.id}: launching task $id by offer ${Str.id(offer.getId.getValue)}\n ${Str.task(task_)}")
  }

  def forciblyStopBroker(broker: Broker): Unit = {
    if (driver != null && broker.task != null) {
      logger.info(s"Stopping broker ${broker.id} forcibly: sending 'stop' message")

      driver.sendFrameworkMessage(
        ExecutorID.newBuilder().setValue(broker.task.executorId).build(),
        SlaveID.newBuilder().setValue(broker.task.slaveId).build(),
        "stop".getBytes
      )
    }
  }

  private[kafka] def otherTasksAttributes(name: String): Array[String] = {
    def value(task: Broker.Task, name: String): String = {
      if (name == "hostname") return task.hostname
      task.attributes.get(name)
    }

    val values = new util.ArrayList[String]()
    for (broker <- cluster.getBrokers)
      if (broker.task != null) {
        val v = value(broker.task, name)
        if (v != null) values.add(v)
      }

    values.toArray(Array[String]())
  }

  private[kafka] def findBrokerPort(offer: Offer): Int = {
    for (resource <- offer.getResourcesList) {
      if (resource.getName == "ports") {
        val ranges: util.List[Value.Range] = resource.getRanges.getRangeList
        val range = if (ranges.isEmpty) null else ranges.get(0)

        assert(range.hasBegin)
        if (range == null) throw new IllegalArgumentException("Invalid port range in offer " + Str.offer(offer))
        return range.getBegin.toInt
      }
    }

    throw new IllegalArgumentException("No port range in offer " + Str.offer(offer))
  }

  def main(args: Array[String]) {
    initLogging()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(Config.mesosUser)
    frameworkBuilder.setName("Kafka Mesos")
    frameworkBuilder.setFailoverTimeout(Config.failoverTimeout)
    frameworkBuilder.setCheckpoint(true)

    val driver = new MesosSchedulerDriver(Scheduler, frameworkBuilder.build, Config.masterConnect)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = {
        if (driver != null) driver.stop()
        HttpServer.stop()
      }
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  def initLogging() {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper.ZooKeeper").setLevel(Level.WARN)

    val logger = Logger.getLogger(Scheduler.getClass)
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")
    root.addAppender(new ConsoleAppender(layout))
  }

  class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
    private var logger: Logger = Logger.getLogger("Jetty")

    def this(logger: Logger) {
      this()
      this.logger = logger
    }

    def isDebugEnabled: Boolean = logger.isDebugEnabled
    def setDebugEnabled(enabled: Boolean) = logger.setLevel(if (enabled) Level.DEBUG else Level.INFO)

    def getName: String = logger.getName
    def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger.getLogger(name))

    def info(s: String, args: AnyRef*) = logger.info(format(s, args))
    def info(s: String, t: Throwable) = logger.info(s, t)
    def info(t: Throwable) = logger.info("", t)

    def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))
    def debug(s: String, t: Throwable) = logger.debug(s, t)

    def debug(t: Throwable) = logger.debug("", t)
    def warn(s: String, args: AnyRef*) = logger.warn(format(s, args))

    def warn(s: String, t: Throwable) = logger.warn(s, t)
    def warn(s: String) = logger.warn(s)
    def warn(t: Throwable) = logger.warn("", t)

    def ignore(t: Throwable) = logger.info("Ignored", t)
  }

  private def format(s: String, args: AnyRef*): String = {
    var result: String = ""
    var i: Int = 0

    for (token <- s.split("\\{\\}")) {
      result += token
      if (args.length > i) result += args(i)
      i += 1
    }

    result
  }
}