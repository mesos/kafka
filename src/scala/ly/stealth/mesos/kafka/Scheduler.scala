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
import java.util.{Collections, Date}
import scala.collection.JavaConversions._
import ly.stealth.mesos.kafka.Util.{Period, Str}

object Scheduler extends org.apache.mesos.Scheduler {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val cluster: Cluster = new Cluster()
  private var driver: SchedulerDriver = null

  private[kafka] def newExecutor(broker: Broker): ExecutorInfo = {
    var cmd = "java -cp " + HttpServer.jar.getName
    cmd += " -Xmx" + broker.heap + "m"

    if (Config.debug) cmd += " -Ddebug"
    cmd += " ly.stealth.mesos.kafka.Executor"

    val commandBuilder = CommandInfo.newBuilder
    if (Config.jre != null) {
      commandBuilder.addUris(CommandInfo.URI.newBuilder().setValue(Config.api + "/jre/" + Config.jre.getName))
      cmd = "jre/bin/" + cmd
    }

    commandBuilder
      .addUris(CommandInfo.URI.newBuilder().setValue(Config.api + "/jar/" + HttpServer.jar.getName))
      .addUris(CommandInfo.URI.newBuilder().setValue(Config.api + "/kafka/" + HttpServer.kafkaDist.getName))
      .setValue(cmd)

    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder.setValue(Broker.nextExecutorId(broker)))
      .setCommand(commandBuilder)
      .setName("broker-" + broker.id)
      .build()
  }

  private[kafka] def newTask(broker: Broker, offer: Offer, port: Int): TaskInfo = {
    def taskData: ByteString = {
      val defaults: Map[String, String] = Map(
        "broker.id" -> broker.id,
        "port" -> ("" + port),
        "log.dirs" -> "kafka-logs",
        "log.retention.bytes" -> ("" + 10l * 1024 * 1024 * 1024),

        "zookeeper.connect" -> Config.zk,
        "host.name" -> offer.getHostname
      )

      val data = new util.HashMap[String, String]()
      data.put("broker", "" + broker.toJson)
      data.put("defaults", Util.formatMap(defaults))
      ByteString.copyFromUtf8(Util.formatMap(data))
    }

    val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
      .setName("broker-" + broker.id)
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

    cluster.frameworkId = id.getValue
    cluster.save()

    this.driver = driver
    reconcileTasksIfRequired(force = true)
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    logger.info("[reregistered] master:" + Str.master(master))
    this.driver = driver
    reconcileTasksIfRequired(force = true)
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
    val declineReasons = new util.ArrayList[String]()
    for (offer <- offers) {
      val declineReason = acceptOffer(offer)

      if (declineReason != null) {
        driver.declineOffer(offer.getId)
        if (!declineReason.isEmpty) declineReasons.add(offer.getHostname + Str.id(offer.getId.getValue) + " - " + declineReason)
      }
    }
    
    if (!declineReasons.isEmpty) logger.info("Declined offers:\n" + declineReasons.mkString("\n"))

    for (broker <- cluster.getBrokers) {
      if (broker.shouldStop) {
        logger.info(s"Stopping broker ${broker.id}: killing task ${broker.task.id}")
        driver.killTask(TaskID.newBuilder.setValue(broker.task.id).build)
        broker.task.state = Broker.State.STOPPING
      }
    }

    reconcileTasksIfRequired()
    cluster.save()
  }

  private[kafka] def acceptOffer(offer: Offer): String = {
    if (isReconciling) return "reconciling"

    var reason = ""
    for (broker <- cluster.getBrokers.filter(_.shouldStart())) {
      val diff = broker.matches(offer, otherTasksAttributes)

      if (diff == null) {
        launchTask(broker, offer)
        return null
      } else {
        if (!reason.isEmpty) reason += ", "
        reason += s"broker ${broker.id}: $diff"
      }
    }

    reason
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
    if (broker == null || broker.task == null || broker.task.id != status.getTaskId.getValue) {
      logger.info(s"Got ${status.getState} for unknown/stopped broker, killing task ${status.getTaskId}")
      driver.killTask(status.getTaskId)
      return
    }

    if (broker.task.reconciling)
      logger.info(s"Finished reconciling of broker ${broker.id}, task ${broker.task.id}")

    broker.task.state = Broker.State.RUNNING
    broker.failover.resetFailures()
  }

  private[kafka] def onBrokerStopped(broker: Broker, status: TaskStatus, now: Date = new Date()): Unit = {
    broker.task = null
    val failed = broker.active && status.getState != TaskState.TASK_FINISHED && status.getState != TaskState.TASK_KILLED

    if (failed) {
      broker.failover.registerFailure(now)

      var msg = s"Broker ${broker.id} failed ${broker.failover.failures}"
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

  private def isReconciling: Boolean = cluster.getBrokers.exists(b => b.task != null && b.task.reconciling)

  private[kafka] def launchTask(broker: Broker, offer: Offer): Unit = {
    val port = broker.getSuitablePort(offer)
    if (port == -1) throw new IllegalStateException("no suitable port")

    val task_ = newTask(broker, offer, port)
    val id = task_.getTaskId.getValue

    val attributes = new util.LinkedHashMap[String, String]()
    for (attribute <- offer.getAttributesList)
      if (attribute.hasText) attributes.put(attribute.getName, attribute.getText.getValue)

    driver.launchTasks(util.Arrays.asList(offer.getId), util.Arrays.asList(task_))
    broker.task = new Broker.Task(id, task_.getSlaveId.getValue, task_.getExecutor.getExecutorId.getValue, offer.getHostname, port, attributes)

    logger.info(s"Starting broker ${broker.id}: launching task $id by offer ${offer.getHostname + Str.id(offer.getId.getValue)}\n ${Str.task(task_)}")
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

  private[kafka] val RECONCILE_DELAY = new Period("10s")
  private[kafka] val RECONCILE_MAX_TRIES = 3

  private[kafka] var reconciles: Int = 0
  private[kafka] var reconcileTime: Date = null

  private[kafka] def reconcileTasksIfRequired(force: Boolean = false, now: Date = new Date()): Unit = {
    if (reconcileTime != null && now.getTime - reconcileTime.getTime < RECONCILE_DELAY.ms)
      return

    if (!isReconciling) reconciles = 0
    reconciles += 1
    reconcileTime = now

    if (reconciles > RECONCILE_MAX_TRIES) {
      for (broker <- cluster.getBrokers.filter(b => b.task != null && b.task.reconciling)) {
        logger.info(s"Reconciling exceeded $RECONCILE_MAX_TRIES tries for broker ${broker.id}, sending killTask for task ${broker.task.id}")
        driver.killTask(TaskID.newBuilder().setValue(broker.task.id).build())
        broker.task = null
      }

      return
    }

    val statuses = new util.ArrayList[TaskStatus]

    for (broker <- cluster.getBrokers.filter(_.task != null))
      if (force || broker.task.reconciling) {
        broker.task.state = Broker.State.RECONCILING
        logger.info(s"Reconciling $reconciles/$RECONCILE_MAX_TRIES state of broker ${broker.id}, task ${broker.task.id}")

        statuses.add(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder().setValue(broker.task.id))
          .setState(TaskState.TASK_STAGING)
          .build()
        )
      }

    if (force || !statuses.isEmpty)
      driver.reconcileTasks(if (force) Collections.emptyList() else statuses)
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

  def start() {
    initLogging()
    logger.info(s"Starting ${getClass.getSimpleName}:\n$Config")

    cluster.load()
    HttpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(if (Config.user != null) Config.user else "")
    if (cluster.frameworkId != null) frameworkBuilder.setId(FrameworkID.newBuilder().setValue(cluster.frameworkId))
    frameworkBuilder.setRole(Config.frameworkRole)

    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.ms / 1000)
    frameworkBuilder.setCheckpoint(true)

    var credsBuilder: Credential.Builder = null
    if (Config.principal != null) {
      frameworkBuilder.setPrincipal(Config.principal)

      credsBuilder = Credential.newBuilder()
      credsBuilder.setPrincipal(Config.principal)
      if (Config.secret != null) credsBuilder.setSecret(ByteString.copyFromUtf8(Config.secret))
    }

    val driver =
      if (credsBuilder != null) new MesosSchedulerDriver(Scheduler, frameworkBuilder.build, Config.master, credsBuilder.build)
      else new MesosSchedulerDriver(Scheduler, frameworkBuilder.build, Config.master)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = HttpServer.stop()
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  private def initLogging() {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN)

    val logger = Logger.getLogger(Scheduler.getClass)
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n")

    var appender: Appender = null
    if (Config.log == null) appender = new ConsoleAppender(layout)
    else appender = new DailyRollingFileAppender(layout, Config.log.getPath, "'.'yyyy-MM-dd")
    
    root.addAppender(appender)
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