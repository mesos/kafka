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
package ly.stealth.mesos.kafka.scheduler

import ly.stealth.mesos.kafka.scheduler.mesos._
import ly.stealth.mesos.kafka.{Broker, ClockComponent}
import net.elodina.mesos.util.Repr
import org.apache.log4j.Logger
import org.apache.mesos.Protos._
import scala.collection.JavaConversions._

trait BrokerLifecyleManagerComponent {
  val brokerLifecycleManager: BrokerLifecycleManager
  trait BrokerLifecycleManager {
    def onBrokerStatus(broker: Broker, status: TaskStatus): Unit
    def activateBroker(broker: Broker): Unit
    def stopBroker(broker: Broker, force: Boolean = false): Unit

    def tryLaunchBrokers(offers: Seq[Offer]): Boolean
  }
}

trait BrokerLifecycleManagerComponentImpl extends BrokerLifecyleManagerComponent{
  this: ClusterComponent
    with OfferManagerComponent
    with ClockComponent
    with BrokerTaskManagerComponent
    with TaskReconcilerComponent =>

  val brokerLifecycleManager = new BrokerLifecycleManagerImpl

  class BrokerLifecycleManagerImpl extends BrokerLifecycleManager {

    private[this] val logger = Logger.getLogger(classOf[BrokerLifecycleManager])

    def activateBroker(broker: Broker): Unit = {
      logger.info(s"Activating broker ${broker.id}")
      broker.active = true
      offerManager.pauseOrResumeOffers(forceRevive = true)
    }

    def stopBroker(broker: Broker, force: Boolean = false): Unit = {
      logger.info(s"Stopping broker ${broker.id}")

      broker.active = false
      broker.failover.resetFailures()
      if (broker.task != null && broker.task.id != null)
        broker.task.state = Broker.State.STOPPING
        if (force) {
          brokerTaskManager.forceStopBroker(broker)
        } else {
          brokerTaskManager.killBroker(broker)
        }
    }

    def tryLaunchBrokers(offers: Seq[Offer]): Boolean = {
      val brokers = cluster.getBrokers
      val results = offers.map(o => offerManager.tryAcceptOffer(o, brokers))

      // Only care about accepts, offers that didnt match have been declined already
      results.foreach({
        case Left(accept) => brokerTaskManager.launchBroker(accept)
        case _ =>
      })

      if (logger.isDebugEnabled)
        logger.debug("\n" + results.map({
          case Left(r) => s"[ACCEPT ]: ${ Repr.offer(r.offer) } => ${ r.broker }"
          case Right(r) => "[DECLINE]: " + r.map(d => s"\t${ Repr.offer(d.offer) } for ${d.duration}s because ${d.reason} ").mkString("\n")
        }).mkString("\n"))

      results.exists({
        case Left(_) => true
        case _ => false
      })
    }

    def onBrokerStatus(broker: Broker, status: TaskStatus): Unit = {
      status.getState match {
        case TaskState.TASK_STAGING =>
        case TaskState.TASK_RUNNING =>
          onRunning(broker, status)
        case TaskState.TASK_LOST | TaskState.TASK_FINISHED |
             TaskState.TASK_FAILED | TaskState.TASK_KILLED |
             TaskState.TASK_ERROR =>
          onStopped(broker, status)
        case _ => logger.warn("Got unexpected task state: " + status.getState)
      }

      cluster.save()
      offerManager.pauseOrResumeOffers()
    }

    private[this] def onRunning(broker: Broker, status: TaskStatus): Unit = {
      if (broker.task != null && broker.task.stopping) {
        // This broker is supposed to be shutting down but instead we got a RUNNING message,
        // try to kill it again
        logger.warn(
          s"Got RUNNING message for broker in STOPPING [${broker.id}], attempting to stop it agian.")
        stopBroker(broker)
        return
      } else if (broker.task == null) {
        // We don't even know about this task, kill it.
        logger.warn(
          s"Got RUNNING status for broker without a task [${broker.id}], killing the task.")
        brokerTaskManager.killTask(status.getTaskId)
        return
      }

      val wasReconciling = broker.task.reconciling
      broker.task.state = Broker.State.RUNNING
      if (wasReconciling) {
        logger.info(s"Finished reconciling of broker ${ broker.id }, task ${ broker.task.id }")
        taskReconciler.onBrokerReconciled(broker)
      }

      if (status.hasData && status.getData.size() > 0)
        broker.task.endpoint = new Broker.Endpoint(status.getData.toStringUtf8)
      broker.registerStart(broker.task.hostname)
    }

    private[this] def onStopped(broker: Broker, status: TaskStatus): Unit = {
      val now = clock.now()
      val failed = broker.active && (broker.task != null && !broker.task.stopping)
      broker.registerStop(now, failed)
      broker.task = null

      if (failed) {
        var msg = s"Broker ${ broker.id } failed ${ broker.failover.failures }"
        if (broker.failover.maxTries != null) msg += "/" + broker.failover.maxTries

        if (!broker.failover.isMaxTriesExceeded) {
          msg += ", waiting " + broker.failover.currentDelay
          msg += ", next start ~ " + Repr.dateTime(broker.failover.delayExpires)
        } else {
          broker.active = false
          msg += ", failure limit exceeded"
          msg += ", deactivating broker"
        }

        logger.info(msg)
      }

      broker.metrics = null
      broker.needsRestart = false
    }
  }
}
