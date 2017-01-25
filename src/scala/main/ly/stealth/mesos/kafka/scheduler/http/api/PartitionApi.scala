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

import javax.ws.rs.{GET, POST, Path, Produces}
import javax.ws.rs.core.{MediaType, Response}
import ly.stealth.mesos.kafka.scheduler.Expr
import ly.stealth.mesos.kafka.scheduler.http.BothParam
import ly.stealth.mesos.kafka.scheduler.mesos.ClusterComponent

trait PartitionApiComponent {
  val partitionApi: PartitionApi
  trait PartitionApi {}
}

trait PartitionApiComponentImpl extends PartitionApiComponent {
  this: ClusterComponent =>

  val partitionApi = new PartitionApiImpl

  @Path("partition")
  @Produces(Array(MediaType.APPLICATION_JSON))
  class PartitionApiImpl extends PartitionApi {

    @Path("list")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def list(@BothParam("topic") topicExpr: String): Response = {
      if (topicExpr == null)
        return Status.BadRequest("invalid topic expression")

      val topics = Expr.expandTopics(topicExpr)
      val topicsAndPartitions = cluster.topics.getPartitions(topics)
      Response.ok(topicsAndPartitions).build()
    }

    @Path("list")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    def listGet(@BothParam("topic") topicExpr: String) = list(topicExpr)

    @Path("add")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def add(@BothParam("topic") topic: String): Response = {
      cluster.rebalancer
      Response.ok().build()
    }
  }
}
