package ly.stealth.mesos.kafka.scheduler.mesos

import ly.stealth.mesos.kafka.Broker
import ly.stealth.mesos.kafka.scheduler.mesos.OfferResult.Accept
import net.elodina.mesos.util.Repr
import org.apache.log4j.Logger
import org.apache.mesos.Protos.{ExecutorID, SlaveID, TaskID}
import scala.collection.JavaConverters._

trait BrokerTaskManagerComponent {

  val brokerTaskManager: BrokerTaskManager

  trait BrokerTaskManager {
    def forceStopBroker(broker: Broker): Unit
    def killBroker(broker: Broker): Unit
    def killTask(taskId: TaskID): Unit
    def launchBroker(offerResult: OfferResult.Accept): Unit
  }
}

trait BrokerTaskManagerComponentImpl extends BrokerTaskManagerComponent {
  this: SchedulerDriverComponent
    with MesosTaskFactoryComponent =>

  val brokerTaskManager: BrokerTaskManager = new BrokerTaskManagerImpl

  class BrokerTaskManagerImpl extends BrokerTaskManager {
    private[this] val logger = Logger.getLogger(classOf[BrokerTaskManager])

    def forceStopBroker(broker: Broker): Unit = {
      if (driver != null && broker.task != null) {
        logger.info(s"Stopping broker ${ broker.id } forcibly: sending 'stop' message")
        Driver.call(_.sendFrameworkMessage(
          ExecutorID.newBuilder().setValue(broker.task.executorId).build(),
          SlaveID.newBuilder().setValue(broker.task.slaveId).build(),
          "stop".getBytes
        ))
      }
    }

    def killBroker(broker: Broker): Unit =
      if (broker.task != null && broker.task.id != null)
        Driver.call(_.killTask(TaskID.newBuilder.setValue(broker.task.id).build))

    def killTask(taskId: TaskID): Unit = Driver.call(_.killTask(taskId))

    def launchBroker(offerResult: Accept): Unit = {
      val broker = offerResult.broker
      val offer = offerResult.offer

      broker.needsRestart = false
      val reservation = broker.getReservation(offer)
      val task_ = taskFactory.newTask(broker, offer, reservation)
      val id = task_.getTaskId.getValue

      val attributes = offer.getAttributesList
        .asScala
        .filter(_.hasText)
        .map(a => a.getName -> a.getText.getValue)
        .toMap

      Driver.call(_.launchTasks(Seq(offer.getId).asJava, Seq(task_).asJava))
      broker.task = Broker.Task(
        id,
        task_.getSlaveId.getValue,
        task_.getExecutor.getExecutorId.getValue,
        offer.getHostname,
        attributes)
      logger
        .info(s"Starting broker ${ broker.id }: launching task $id by offer ${
          offer
            .getHostname + Repr.id(offer.getId.getValue)
        }\n ${ Repr.task(task_) }")
    }
  }

}

