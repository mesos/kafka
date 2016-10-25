package ly.stealth.mesos.kafka

import ly.stealth.mesos.kafka.Broker.Metrics

case class BrokerStatusResponse(brokers: Seq[Broker])
case class BrokerStartResponse(brokers: Seq[Broker], status: String, message: Option[String] = None)
case class BrokerRemoveResponse(ids: Seq[String])

case class FrameworkMessage(metrics: Option[Metrics] = None, log: Option[LogResponse] = None)
case class ListTopicsResponse(topics: Seq[Topics.Topic])

case class HttpLogResponse(status: String, content: String)
case class RebalanceStartResponse(status: String, state: String, error: Option[String])
case class HttpErrorResponse(code: Int, error: String)
