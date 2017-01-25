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

import kafka.utils.{ZkUtils => KafkaZkUtils, ZKStringSerializer}
import kafka.common.TopicAndPartition
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import ly.stealth.mesos.kafka.interface.ZkUtilsProxy
import org.I0Itec.zkclient.ZkClient
import scala.collection.{Map, Set, mutable}

class ZkUtils(zkUrl: String) extends ZkUtilsProxy {
  private val DEFAULT_TIMEOUT_MS = 30000
  private val zkClient = new ZkClient(zkUrl, DEFAULT_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, ZKStringSerializer)

  override def getAllTopics(): Seq[String] = KafkaZkUtils.getAllTopics(zkClient)

  override def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]]
  = KafkaZkUtils.getReplicaAssignmentForTopics(zkClient, topics)

  override def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext]
  = KafkaZkUtils.getPartitionsBeingReassigned(zkClient)

  override def getReplicasForPartition(
    topic: String,
    partition: Int
  ): Seq[Int] = KafkaZkUtils.getReplicasForPartition(zkClient, topic, partition)

  override def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit
  = KafkaZkUtils.updatePartitionReassignmentData(zkClient, partitionsToBeReassigned)

  override def createPersistentPath(
    path: String,
    data: String
  ): Unit = KafkaZkUtils.createPersistentPath(zkClient, path, data)

  override def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, Map[Int, Seq[Int]]]
  = KafkaZkUtils.getPartitionAssignmentForTopics(zkClient, topics)

  override def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
  = KafkaZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, topicAndPartitions)

  override def getSortedBrokerList(): Seq[Int]
  = KafkaZkUtils.getSortedBrokerList(zkClient)
}
