package ly.stealth.mesos.kafka

import ly.stealth.mesos.kafka.mesos._
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
    with BrokerTaskManagerComponent =>

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
          onBrokerStarted(broker, status)
        case TaskState.TASK_LOST | TaskState.TASK_FINISHED |
             TaskState.TASK_FAILED | TaskState.TASK_KILLED |
             TaskState.TASK_ERROR =>
          onBrokerStopped(broker, status)
        case _ => logger.warn("Got unexpected task state: " + status.getState)
      }

      cluster.save()
      offerManager.pauseOrResumeOffers()
    }

    private[this] def onBrokerStarted(broker: Broker, status: TaskStatus): Unit = {
      if (broker == null || broker.task == null || broker.task.id != status.getTaskId.getValue) {
        logger.info(
          s"Got ${ status.getState } for unknown/stopped broker, killing task ${status.getTaskId}")
        brokerTaskManager.killTask(status.getTaskId)
        return
      }

      if (broker.task.reconciling)
        logger.info(s"Finished reconciling of broker ${ broker.id }, task ${ broker.task.id }")

      broker.task.state = Broker.State.RUNNING
      if (status.hasData && status.getData.size() > 0)
        broker.task.endpoint = new Broker.Endpoint(status.getData.toStringUtf8)
      broker.registerStart(broker.task.hostname)
    }

    private[this] def onBrokerStopped(broker: Broker, status: TaskStatus): Unit = {
      if (broker == null) {
        logger.info(s"Got ${ status.getState } for unknown broker, ignoring it")
        return
      }

      val now = clock.now()
      broker.task = null
      val failed = broker.active &&
        status.getState != TaskState.TASK_FINISHED &&
        status.getState != TaskState.TASK_KILLED
      broker.registerStop(now, failed)

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
