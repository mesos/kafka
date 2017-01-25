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
package ly.stealth.mesos.kafka.interface

import ly.stealth.mesos.kafka.interface.impl.ZkUtils
import kafka.common.TopicAndPartition
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import scala.collection.{Map, Set, mutable}

trait ZkUtilsProxy {
  def getAllTopics(): Seq[String]
  def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]]
  def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext]
  def getReplicasForPartition(topic: String, partition: Int): Seq[Int]
  def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit
  def createPersistentPath(path: String, data: String = ""): Unit
  def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]]
  def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
  def getSortedBrokerList(): Seq[Int]
}

object ZkUtilsProxy {
  def apply(zkUrl: String) = new ZkUtils(zkUrl)
}