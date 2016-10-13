package ly.stealth.mesos.kafka

import java.util.Properties

case class Quota(producerByteRate: Option[Integer], consumerByteRate: Option[Integer])

object Quotas {
  val PRODUCER_BYTE_RATE = "producer_byte_rate"
  val CONSUMER_BYTE_RATE = "consumer_byte_rate"
}

class Quotas {

  private[this] def propToInt(properties: Properties, key: String): Option[Integer] = {
    val propVal = properties.get(key)
    if (propVal == null) {
      None
    } else {
      Some(propVal.asInstanceOf[String].toInt)
    }
  }

  def getClientQuotas(): Map[String, Quota] = {
    val quotas = ZkUtilsWrapper().fetchAllEntityConfigs("clients")
    quotas.mapValues(p => Quota(propToInt(p, Quotas.PRODUCER_BYTE_RATE), propToInt(p, Quotas.CONSUMER_BYTE_RATE))).toMap
  }

  def getClientConfig(clientId: String) =
    ZkUtilsWrapper().fetchEntityConfig("clients", clientId)

  def setClientConfig(clientId: String, configs: Properties) =
    ZkUtilsWrapper().changeClientIdConfig(clientId, configs)
}
