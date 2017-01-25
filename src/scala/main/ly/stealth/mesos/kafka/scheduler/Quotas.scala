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
    val quotas = AdminUtilsWrapper().fetchAllEntityConfigs("clients")
    quotas.mapValues(p => Quota(propToInt(p, Quotas.PRODUCER_BYTE_RATE), propToInt(p, Quotas.CONSUMER_BYTE_RATE))).toMap
  }

  def getClientConfig(clientId: String) =
    AdminUtilsWrapper().fetchEntityConfig("clients", clientId)

  def setClientConfig(clientId: String, configs: Properties) =
    AdminUtilsWrapper().changeClientIdConfig(clientId, configs)
}
