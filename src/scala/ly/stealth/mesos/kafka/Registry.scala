package ly.stealth.mesos.kafka

import ly.stealth.mesos.kafka.http.api._
import ly.stealth.mesos.kafka.http.{HttpServerComponent, HttpServerComponentImpl}
import ly.stealth.mesos.kafka.mesos._

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