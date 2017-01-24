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

trait BrokerLifecycleManagerComponent {
  val brokerLifecycleManager: BrokerLifecycleManager
  trait BrokerLifecycleManager {
    def tryTransition(broker: Broker, to: BrokerState): Boolean
    def tryTransition(status: TaskStatus): Unit
  }
}

abstract class BrokerState {}

object BrokerState {
  case class Active() extends BrokerState
  case class Inactive(force: Boolean = false) extends BrokerState
  case class Starting(accept: OfferResult.Accept) extends BrokerState
}

trait BrokerLifecycleManagerComponentImpl extends BrokerLifecycleManagerComponent{
  this: ClusterComponent
    with OfferManagerComponent
    with ClockComponent
    with BrokerTaskManagerComponent
    with TaskReconcilerComponent =>

  val brokerLifecycleManager = new BrokerLifecycleManagerImpl

  class BrokerLifecycleManagerImpl extends BrokerLifecycleManager {
    object InternalBrokerState {
      case class Stopping(force: Boolean = false) extends BrokerState
    }

    private[this] val logger = Logger.getLogger("BrokerLifecycleManager")

    private[this] val validStateTransitions: PartialFunction[(Option[Broker], BrokerState), Unit] = {
      case (StoppedBroker(b), _: BrokerState.Active) => activateBroker(b)
      case (StoppedBroker(b), s: BrokerState.Starting) if b.active => launchBroker(b, s.accept)

      case (Some(b), _: BrokerState.Inactive) => deactivateBroker(b)
      case (RunningBroker(b), s: InternalBrokerState.Stopping) => stopBroker(b, s.force)
    }

    // Request an explicit transition to a state.
    def tryTransition(broker: Broker, to: BrokerState): Boolean = {
      val tup = (Some(broker), to)
      if (validStateTransitions.isDefinedAt(tup)) {
        validStateTransitions(tup)
        true
      } else {
        false
      }
    }

    // Attempt to transition a broker based on a status update.
    def tryTransition(status: TaskStatus): Unit = {
      val maybeBroker = cluster.getBrokerByTaskId(status.getTaskId.getValue)
      (maybeBroker, status) match {
        // Reconciliation
        case Reconciling(broker, TaskRunning(_)) => onReconciled(broker)
        case Reconciling(broker, TaskExited(_)) => onStopped(broker, status, failed = true)
        case (ReconcilingBroker(broker), TaskExited(_)) => onStopped(broker, status, failed = true)

        // Expected start up
        case (PendingBroker(_), TaskStaging(_)) => // noop
        case (PendingBroker(broker), TaskStarting(_)) => onStarting(broker, status)
        case (StartingBroker(broker), TaskRunning(_)) => onRunning(broker, status)

        // Abnormal (duplicate) but benign transitions
        case (StartingBroker(_), TaskStarting(_)) => // noop
        case (RunningBroker(_), TaskRunning(_)) => // noop

        // Expected shutdown
        case (StoppingBroker(_), TaskKilling(_)) => // noop
        case (StoppingBroker(broker), TaskExited(_)) => onStopped(broker, status, failed = false)

        // Unexpected shutdown
        case (Some(broker), TaskKilling(_)) => onStopped(broker, status, failed = true)
        case (Some(broker), TaskExited(_)) => onStopped(broker, status, failed = true)

        // RUNNING status for brokers that shouldn't get them
        case (StoppingBroker(broker), TaskStarting(_)) => onUnexpectedRunning(broker, status)
        case (StoppingBroker(broker), TaskRunning(_)) => onUnexpectedRunning(broker, status)

        // Unexpected tasks
        case (None, TaskRunning(_)) => tryResurrectBroker(status)
        case (None, TaskExited(_)) => // noop

        // Unknown
        case (Some(broker), _) => logger.warn(s"Got unknown state ${status.getState} for broker ${broker.id}")
        case _ => logger.warn(s"Got unknown state ${status.getState} for task id ${status.getTaskId}")
      }

      cluster.save()
      offerManager.pauseOrResumeOffers()
    }

    private def activateBroker(broker: Broker): Unit = {
      logger.info(s"Activating broker ${broker.id}")
      broker.active = true
      offerManager.pauseOrResumeOffers(forceRevive = true)
    }

    private def deactivateBroker(broker: Broker, force: Boolean = false): Unit = {
      logger.info(s"Deactivating broker ${broker.id}")
      broker.active = false
      broker.failover.resetFailures()
      tryTransition(broker, InternalBrokerState.Stopping(force))
    }

    private def stopBroker(broker: Broker, force: Boolean = false): Unit = {
      logger.info(s"Stopping broker ${broker.id}, force = $force")
      broker.task.state = Broker.State.STOPPING
      if (force) {
        brokerTaskManager.forceStopBroker(broker)
      } else {
        brokerTaskManager.killBroker(broker)
      }
    }

    private def launchBroker(broker: Broker, accept: OfferResult.Accept) = {
      val task = brokerTaskManager.launchBroker(accept)
      broker.task = task
    }
    private def tryResurrectBroker(status: TaskStatus) = {
      val taskId = status.getTaskId
      val brokerId = Broker.idFromTaskId(taskId.getValue)
      val maybeBroker = Option(cluster.getBroker(brokerId))
      maybeBroker match {
        case RunningBroker(broker) =>
          logger.warn(s"Killing orphaned task $taskId because a new task for " +
            s"broker ${broker.id} is already running (${broker.task.id})")
          brokerTaskManager.killTask(taskId)
        case StoppedBroker(broker) if broker.active => tryReattachTask(broker, status)
        case StoppedBroker(broker) if !broker.active => brokerTaskManager.killTask(taskId)
        case None => brokerTaskManager.killTask(taskId)
        case _ => // other states we dont care about.
      }
    }
    private def tryReattachTask(broker: Broker, status: TaskStatus) = {
      val kill =
        if (broker.lastTask != null && broker.lastTask.id == status.getTaskId.getValue) {
          logger.warn(s"Reattaching task ${status.getTaskId}")
          broker.task = broker.lastTask
          false
        } else if (broker.lastTask != null) {
          logger.warn(s"Not able to reattach task ${status.getTaskId} because it didn't match the " +
            s"last task launched for broker ${broker.id}, ${broker.lastTask.id}")
          true
        } else {
          logger.warn(s"Not able to reattach task ${status.getTaskId}")
          true
        }
      if (kill)
        brokerTaskManager.killTask(status.getTaskId)
    }

    private[this] def onUnexpectedRunning(broker: Broker, status: TaskStatus) = {
      // This broker is supposed to be shutting down but instead we got a RUNNING message,
      // try to kill it again
      logger.warn(
        s"Got RUNNING message for broker in STOPPING [${broker.id}], attempting to stop it agian.")
      brokerTaskManager.killTask(status.getTaskId)
    }

    private[this] def onReconciled(broker: Broker) = {
      broker.task.state = Broker.State.RUNNING
      logger.info(s"Finished reconciling of broker ${ broker.id }, task ${ broker.task.id }")
      taskReconciler.onBrokerReconciled(broker)
    }

    private[this] def onStarting(broker: Broker, status: TaskStatus): Unit = {
      broker.task.state = Broker.State.STARTING
    }

    private[this] def onRunning(broker: Broker, status: TaskStatus): Unit = {
      broker.task.state = Broker.State.RUNNING
      if (status.hasData && status.getData.size() > 0)
        broker.task.endpoint = new Broker.Endpoint(status.getData.toStringUtf8)
      broker.registerStart(broker.task.hostname)
    }

    private[this] def onStopped(broker: Broker, status: TaskStatus, failed: Boolean): Unit = {
      val now = clock.now()
      broker.registerStop(now, failed)
      if (broker.task != null)
        broker.lastTask = broker.task
      broker.task = null
      broker.metrics = null
      broker.needsRestart = false

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
    }
  }
}
