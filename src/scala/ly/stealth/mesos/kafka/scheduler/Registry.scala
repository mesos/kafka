package ly.stealth.mesos.kafka.scheduler

import ly.stealth.mesos.kafka.scheduler.http.api._
import ly.stealth.mesos.kafka.scheduler.http.{HttpServerComponent, HttpServerComponentImpl}
import ly.stealth.mesos.kafka.scheduler.mesos._
import ly.stealth.mesos.kafka.{ClockComponent, Cluster, WallClockComponent}

trait HttpApiComponent
  extends BrokerApiComponent
    with TopicApiComponent
    with PartitionApiComponent
    with QuotaApiComponent

trait Registry
  extends ClusterComponent
  with TaskReconcilerComponent
  with SchedulerComponent
  with OfferManagerComponent
  with HttpServerComponent
  with KafkaDistributionComponent
  with MesosTaskFactoryComponent
  with BrokerLogManagerComponent
  with SchedulerDriverComponent
  with BrokerLifecyleManagerComponent
  with ClockComponent
  with BrokerTaskManagerComponent
  with HttpApiComponent
  with EventLoopComponent
{

}

class ProductionRegistry
  extends Registry
    with ClusterComponent
    with OfferManagerComponentImpl
    with TaskReconcilerComponentImpl
    with HttpServerComponentImpl
    with SchedulerComponentImpl
    with KafkaDistributionComponentImpl
    with MesosTaskFactoryComponentImpl
    with BrokerLogManagerComponentImpl
    with BrokerLifecycleManagerComponentImpl
    with WallClockComponent
    with BrokerTaskManagerComponentImpl
    with BrokerApiComponentImpl
    with TopicApiComponentImpl
    with PartitionApiComponentImpl
    with QuotaApiComponentImpl
    with DefaultEventLoopComponent
{
  val cluster = Cluster.load()
}