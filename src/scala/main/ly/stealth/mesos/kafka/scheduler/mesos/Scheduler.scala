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

package ly.stealth.mesos.kafka.scheduler.mesos

import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService}
import ly.stealth.mesos.kafka._
import ly.stealth.mesos.kafka.RunnableConversions._
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.{BrokerLifecycleManagerComponent, BrokerLogManagerComponent, BrokerState, Registry}
import net.elodina.mesos.util.{Repr, Version}
import org.apache.log4j._
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, SchedulerDriver}
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class MesosDriverException(status: Status) extends Exception
class DriverDisconnectedException extends Exception

object Driver {
  private[this] val logger = Logger.getLogger("Driver")

  def call(fn: SchedulerDriver => Status)(implicit driver: SchedulerDriver): Unit = {
    if (driver == null) {
      logger.error("Attempting to call method on null driver.")
      throw new NullPointerException()
    }
    val ret = fn(driver)
    if (ret != Status.DRIVER_RUNNING) {
      logger.error(s"Error calling method on driver, returned status result $ret")
      throw new MesosDriverException(ret)
    }
  }
}

trait EventLoopComponent {
  val eventLoop: ScheduledExecutorService
}

trait DefaultEventLoopComponent extends EventLoopComponent {
  val eventLoop: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
}

trait SchedulerDriverComponent {
  implicit def driver: SchedulerDriver
}

trait SchedulerComponent {
  val scheduler: KafkaMesosScheduler

  trait KafkaMesosScheduler extends org.apache.mesos.Scheduler {
    def tryLaunchBrokers(offers: Seq[Offer]): Boolean
    def requestBrokerLog(broker: Broker, name: String, lines: Int, timeout: Duration): Future[String]
    def stop(): Unit
    def kill(): Unit
  }
}

trait SchedulerComponentImpl extends SchedulerComponent with SchedulerDriverComponent {
  this: TaskReconcilerComponent
    with MesosTaskFactoryComponent
    with OfferManagerComponent
    with ClusterComponent
    with BrokerLogManagerComponent
    with SchedulerDriverComponent
    with BrokerLifecycleManagerComponent
    with BrokerTaskManagerComponent
    with EventLoopComponent =>

  val scheduler: KafkaMesosScheduler = new KafkaMesosSchedulerImpl
  private[this] var _driver: SchedulerDriver = _
  implicit def driver = _driver

  class KafkaMesosSchedulerImpl extends KafkaMesosScheduler {
    private val logger: Logger = Logger.getLogger("KafkaMesosScheduler")

    def registered(driver: SchedulerDriver, id: FrameworkID, master: MasterInfo): Unit = {
      logger
        .info("[registered] framework:" + Repr.id(id.getValue) + " master:" + Repr.master(master))

      cluster.frameworkId = id.getValue
      cluster.save()

      _driver = driver
      checkMesosVersion(master)

      if (cluster.getBrokers.nonEmpty)
        taskReconciler.start()
    }

    def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
      logger.info("[reregistered] master:" + Repr.master(master))
      _driver = driver
      taskReconciler.start()
    }

    def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
      if (logger.isDebugEnabled)
        logger.debug("[resourceOffers]\n" + Repr.offers(offers))

      eventLoop.execute(() =>
        if (taskReconciler.isReconciling) {
          offers.foreach(o => offerManager.declineOffer(o.getId))
        }
        else {
          try {
            if (tryLaunchBrokers(offers)) {
              cluster.save()
            }
          } catch {
            case e: Exception =>
              logger.error("Error accepting offers, declining", e)
              offers.foreach(o => offerManager.declineOffer(o.getId))
          }
        })
    }

    private def debugLog(result: Either[OfferResult.Accept, Seq[OfferResult.Decline]]): Unit = {
      if (logger.isDebugEnabled)
        logger.debug(
          result match {
            case Left(r) => s"[ACCEPT ]: ${ Repr.offer(r.offer) } => ${ r.broker }"
            case Right(r) => "[DECLINE]: " + r
              .map(d => s"\t${ Repr.offer(d.offer) } for ${ d.duration }s because ${ d.reason } ")
              .mkString("\n")
          })
    }

    def tryLaunchBrokers(offers: Seq[Offer]): Boolean = {
      val brokers = cluster.getBrokers.toSet
      offers.foldLeft(false)((launched, o) => {
        val r = offerManager.tryAcceptOffer(o, brokers)
        // If the broker matched, remove it from the list so it doesn't match other offers too.
        debugLog(r)
        r match {
          case Left(accept) =>
            brokerLifecycleManager.tryTransition(accept.broker, BrokerState.Starting(accept))
            true
          case _ => launched
        }
      })
    }

    def offerRescinded(driver: SchedulerDriver, id: OfferID): Unit = {
      logger.info("[offerRescinded] " + Repr.id(id.getValue))
    }

    def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
      logger.info("[statusUpdate] " + Repr.status(status))
      eventLoop.execute(() => brokerLifecycleManager.tryTransition(status))
    }

    def frameworkMessage(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      data: Array[Byte]
    ): Unit = {
      if (logger.isTraceEnabled)
        logger.trace(
          "[frameworkMessage] executor:" + Repr.id(executorId.getValue) + " slave:" +
            Repr.id(slaveId.getValue) + " data: " + new String(data))
      else if (logger.isDebugEnabled)
        logger.debug(
          s"[frameworkMessage] executor: ${ Repr.id(executorId.getValue) } slave:" +
          s" ${Repr.id(slaveId.getValue)}")

      val broker = cluster.getBroker(Broker.idFromExecutorId(executorId.getValue))

      try {
        val msg = JsonUtil.fromJson[FrameworkMessage](data)
        msg.metrics.foreach(metrics => {
          if (broker != null && broker.active) {
            broker.metrics = metrics
          }
        })

        msg.log.foreach(logResponse => {
          if (broker != null
            && broker.active
            && broker.task != null
            && broker.task.running) {
            brokerLogManager.putLog(logResponse.requestId, logResponse.content)
          }
        })
      } catch {
        case e: IllegalArgumentException =>
          logger.warn("Unable to parse framework message as JSON", e)
      }
    }

    def disconnected(driver: SchedulerDriver): Unit = {
      logger.info("[disconnected]")
      _driver = null
    }

    def slaveLost(driver: SchedulerDriver, id: SlaveID): Unit = {
      logger.info("[slaveLost] " + Repr.id(id.getValue))
    }

    def executorLost(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      status: Int
    ): Unit = {
      logger.info(
        "[executorLost] executor:" + Repr.id(executorId.getValue) +
        " slave:" + Repr.id(slaveId.getValue) + " status:" + status)
    }

    def error(driver: SchedulerDriver, message: String): Unit = {
      logger.info("[error] " + message)
    }

    private def checkMesosVersion(master: MasterInfo): Unit = {
      val version = if (master.getVersion != null)
        new Version(master.getVersion)
      else {
        logger
          .warn("Unable to detect mesos version, mesos < 0.23 is unsupported, proceed with caution.")
        new Version("0.22.1")
      }

      val hasSuppressOffers = new Version("0.25.0")
      if (version.compareTo(hasSuppressOffers) >= 0) {
        logger.info("Enabling offer suppression")
        offerManager.enableOfferSuppression()
      }
    }

    def requestBrokerLog(broker: Broker, name: String, lines: Int, timeout: Duration): Future[String] = {
      val (requestId, future) = brokerLogManager.initLogRequest(timeout)
      val executorId = ExecutorID.newBuilder().setValue(broker.task.executorId).build()
      val slaveId = SlaveID.newBuilder().setValue(broker.task.slaveId).build()

      Driver
        .call(_
          .sendFrameworkMessage(executorId, slaveId, LogRequest(requestId, lines, name).toString
            .getBytes))
      future
    }

    def stop(): Unit = {
      if (driver != null) {
        // Warning: stop(false) and stop() are dangerous, calling them will destroy the framework
        // and kill all running tasks / executors.
        driver.stop(true)
      }
    }

    def kill(): Unit = {
      System.exit(1)
    }
  }
}

object KafkaMesosScheduler {
  private val logger = Logger.getLogger(KafkaMesosScheduler.getClass)
  private[this] var driver: SchedulerDriver = _

  def start(registry: Registry) {
    initLogging()
    logger.info(s"Starting ${getClass.getSimpleName}:\n$Config")

    registry.httpServer.initLogging()
    registry.httpServer.start()

    val frameworkBuilder = FrameworkInfo.newBuilder()
    frameworkBuilder.setUser(if (Config.user != null) Config.user else "")
    if (registry.cluster.frameworkId != null) frameworkBuilder.setId(FrameworkID.newBuilder().setValue(registry.cluster.frameworkId))
    frameworkBuilder.setRole(Config.frameworkRole)

    frameworkBuilder.setName(Config.frameworkName)
    frameworkBuilder.setFailoverTimeout(Config.frameworkTimeout.ms / 1000)
    frameworkBuilder.setCheckpoint(true)

    var credsBuilder: Credential.Builder = null
    if (Config.principal != null && Config.secret != null) {
      frameworkBuilder.setPrincipal(Config.principal)

      credsBuilder = Credential.newBuilder()
      credsBuilder.setPrincipal(Config.principal)
      credsBuilder.setSecret(Config.secret)
    }

    driver =
      if (credsBuilder != null)
        new MesosSchedulerDriver(registry.scheduler, frameworkBuilder.build, Config.master, credsBuilder.build)
      else
        new MesosSchedulerDriver(registry.scheduler, frameworkBuilder.build, Config.master)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = registry.httpServer.stop()
    })

    val status = if (driver.run eq Status.DRIVER_STOPPED) 0 else 1
    System.exit(status)
  }

  private def initLogging() {
    BasicConfigurator.resetConfiguration()

    val root = Logger.getRootLogger
    root.setLevel(Level.INFO)

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.WARN)

    val logger = Logger.getLogger("KafkaMesosScheduler")
    logger.setLevel(if (Config.debug) Level.DEBUG else Level.INFO)

    val layout = new PatternLayout("%d %-5p %23c] %m%n")


    var appender: Appender = null
    if (Config.log == null) appender = new ConsoleAppender(layout)
    else appender = new DailyRollingFileAppender(layout, Config.log.getPath, "'.'yyyy-MM-dd")
    
    root.addAppender(appender)
  }
}
