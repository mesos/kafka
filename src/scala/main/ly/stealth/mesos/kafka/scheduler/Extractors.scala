package ly.stealth.mesos.kafka.scheduler

import ly.stealth.mesos.kafka.Broker
import org.apache.mesos.Protos.{TaskState, TaskStatus}


abstract class TaskStatusExtractor(states: Set[TaskState]) {
  def this(state: TaskState) = this(Set(state))

  def unapply(status: TaskStatus) =
    if (states.contains(status.getState)) Some(status) else None
}

object TaskStaging extends TaskStatusExtractor(TaskState.TASK_STAGING) {}
object TaskStarting extends TaskStatusExtractor(TaskState.TASK_STARTING) {}
object TaskRunning extends TaskStatusExtractor(TaskState.TASK_RUNNING) {}
object TaskKilling extends TaskStatusExtractor(TaskState.TASK_KILLING) {}
object TaskLost extends TaskStatusExtractor(TaskState.TASK_LOST) {}
object TaskExited extends TaskStatusExtractor(Set(
  TaskState.TASK_ERROR, TaskState.TASK_FAILED, TaskState.TASK_FINISHED,
  TaskState.TASK_KILLED, TaskState.TASK_LOST
)) {}

object Reconciling {
  def unapply(tup: (Option[Broker], TaskStatus)) = tup match {
    case (ReconcilingBroker(broker), status)
      if status.getReason == TaskStatus.Reason.REASON_RECONCILIATION =>
      Some((broker, status))
    case _ => None
  }
}


abstract class BrokerTaskExtractor(test: (Broker.Task => Boolean)) {
  def unapply(maybeBroker: Option[Broker]) = maybeBroker match {
    case b@Some(broker) if broker.task != null && test(broker.task) => b
    case _ => None
  }
}

object PendingBroker extends BrokerTaskExtractor(_.pending) {}
object StartingBroker extends BrokerTaskExtractor(_.starting) {}
object RunningBroker extends BrokerTaskExtractor(_.running) {}
object StoppingBroker extends BrokerTaskExtractor(_.stopping) {}
object ReconcilingBroker extends BrokerTaskExtractor(_.reconciling) {}
object StoppedBroker {
  def unapply(maybeBroker: Option[Broker]) = maybeBroker match {
    case b@Some(broker) if broker.task == null => b
    case _ => None
  }
}
