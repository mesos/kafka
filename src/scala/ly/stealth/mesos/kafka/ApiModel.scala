package ly.stealth.mesos.kafka

import ly.stealth.mesos.kafka.Broker.Metrics
import scala.collection.{Map, Seq}

case class BrokerStatusResponse(brokers: Seq[Broker])
case class BrokerStartResponse(brokers: Seq[Broker], status: String, message: Option[String] = None)
case class BrokerRemoveResponse(ids: Seq[String])

case class FrameworkMessage(metrics: Option[Metrics] = None, log: Option[LogResponse] = None)
case class ListTopicsResponse(topics: Seq[Topic])

case class HttpLogResponse(status: String, content: String)
case class RebalanceStartResponse(status: String, state: String, error: Option[String])
case class HttpErrorResponse(code: Int, error: String)


case class Partition(
  id: Int = 0,
  replicas: Seq[Int] = null,
  isr: Seq[Int] = null,
  leader: Int = 0,
  expectedLeader: Int = 0)

case class Topic(
  name: String = null,
  partitions: Map[Int, Seq[Int]] = Map(),
  options: Map[String, String] = Map()
) {
  def partitionsState: String = {
    var s: String = ""
    for ((partition, brokers) <- partitions.toSeq.sortBy(_._1)) {
      if (!s.isEmpty) s += ", "
      s += partition + ":[" + brokers.mkString(",") + "]"
    }
    s
  }
}