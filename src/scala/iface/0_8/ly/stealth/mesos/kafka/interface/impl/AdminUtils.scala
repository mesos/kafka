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
package ly.stealth.mesos.kafka.interface.impl

import java.util.Properties
import kafka.admin.{AdminUtils => KafkaAdminUtils}
import kafka.utils.ZKStringSerializer
import ly.stealth.mesos.kafka.interface.{AdminUtilsProxy, FeatureSupport}
import org.I0Itec.zkclient.ZkClient
import scala.collection.Map

class AdminUtils(zkUrl: String) extends AdminUtilsProxy {
  private val DEFAULT_TIMEOUT_MS = 30000
  private val zkClient = new ZkClient(zkUrl, DEFAULT_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, ZKStringSerializer)

  override def fetchAllTopicConfigs(): Map[String, Properties] = KafkaAdminUtils.fetchAllTopicConfigs(zkClient)

  override def createOrUpdateTopicPartitionAssignmentPathInZK(
    topic: String,
    partitionReplicaAssignment: Map[Int, Seq[Int]],
    config: Properties,
    update: Boolean
  ): Unit = KafkaAdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment, config, update)

  override def changeTopicConfig(
    topic: String,
    configs: Properties
  ): Unit = KafkaAdminUtils.changeTopicConfig(zkClient, topic, configs)

  override def deleteTopic(topicToDelete: String): Unit =
    KafkaAdminUtils.deleteTopic(zkClient, topicToDelete)

  override def fetchEntityConfig(
    entityType: String,
    entity: String
  ): Properties = {
    if (entityType == "topics")
      KafkaAdminUtils.fetchTopicConfig(zkClient, entity)
    else
      new Properties()
  }

  override def changeClientIdConfig(
    clientId: String,
    configs: Properties
  ): Unit = {}

  override def fetchAllEntityConfigs(entityType: String): Map[String, Properties] = {
    if (entityType == "topics")
      KafkaAdminUtils.fetchAllTopicConfigs(zkClient)
    else
      Map()
  }

  override def assignReplicasToBrokers(
    ids: Seq[Int],
    nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int,
    startPartitionId: Int
  ): Map[Int, Seq[Int]] = KafkaAdminUtils.assignReplicasToBrokers(ids, nPartitions, replicationFactor, fixedStartIndex, startPartitionId)

  override val features: FeatureSupport = FeatureSupport(quotas = false, genericEntityConfigs = false)
}
