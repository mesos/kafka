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
package ly.stealth.mesos.kafka.scheduler.http.api

import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.{GET, POST, Path, Produces}
import ly.stealth.mesos.kafka.scheduler.Quotas
import ly.stealth.mesos.kafka.scheduler.http.BothParam
import ly.stealth.mesos.kafka.scheduler.mesos.ClusterComponent

trait QuotaApiComponent {
  val quotaApi: QuotaApi
  trait QuotaApi {}
}

trait QuotaApiComponentImpl extends QuotaApiComponent {
  this: ClusterComponent =>

  val quotaApi = new QuotaApiImpl

  @Path("quota")
  class QuotaApiImpl extends QuotaApi {

    @Path("list")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def list(): Response = {
      val quotas = cluster.quotas.getClientQuotas()
      val response = quotas.mapValues(
        v => Map(
          "producer_byte_rate" -> v.producerByteRate,
          "consumer_byte_rate" -> v.consumerByteRate
        ).filter({
          case (_, Some(_)) => true
          case _ => false
        }).mapValues(v => v.get)
      )
      Response.ok(response).build()
    }

    @Path("list")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    def listGet() = list()

    @Path("set")
    @POST
    def set(
      @BothParam("entityType") entityType: String,
      @BothParam("entity") entityName: String,
      @BothParam("producerByteRate") producerQuota: String,
      @BothParam("consumerByteRate") consumerQuota: String
    ): Response = {
      if (entityType == null || entityType != "clients") {
        Status.BadRequest("invalid entity type")
      }
      else if (entityName == null) {
        Status.BadRequest("invalid entity")
      }
      else {
        if (producerQuota == null && consumerQuota == null) {
          Status.BadRequest("invalid quotas")
        }
        else {
          val existingConfig = cluster.quotas.getClientConfig(entityName)
          if (producerQuota != null) {
            existingConfig.setProperty(Quotas.PRODUCER_BYTE_RATE, producerQuota.toInt.toString)
          }
          if (consumerQuota != null) {
            existingConfig.setProperty(Quotas.CONSUMER_BYTE_RATE, consumerQuota.toInt.toString)
          }
          cluster.quotas.setClientConfig(entityName, existingConfig)
          Response.ok().build()
        }
      }
    }
  }
}