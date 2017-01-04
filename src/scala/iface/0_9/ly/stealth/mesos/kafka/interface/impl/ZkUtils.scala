package ly.stealth.mesos.kafka.interface.impl

import kafka.utils.{ZkUtils => KafkaZkUtils}
import kafka.common.TopicAndPartition
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import ly.stealth.mesos.kafka.interface.ZkUtilsProxy
import scala.collection.{Map, Set, mutable}

class ZkUtils(zkUrl: String) extends ZkUtilsProxy {
  private val zkUtils = KafkaZkUtils(zkUrl, 30000, 30000, isZkSecurityEnabled = false)

  override def getAllTopics(): Seq[String] = zkUtils.getAllTopics()

  override def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]]
  = zkUtils.getReplicaAssignmentForTopics(topics)

  override def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext]
  = zkUtils.getPartitionsBeingReassigned()

  override def getReplicasForPartition(
    topic: String,
    partition: Int
  ): Seq[Int] = zkUtils.getReplicasForPartition(topic, partition)

  override def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit
  = zkUtils.updatePartitionReassignmentData(partitionsToBeReassigned)

  override def createPersistentPath(
    path: String,
    data: String
  ): Unit = zkUtils.createPersistentPath(path, data)

  override def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, Map[Int, Seq[Int]]]
  = zkUtils.getPartitionAssignmentForTopics(topics)

  override def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
  = zkUtils.getPartitionLeaderAndIsrForTopics(null, topicAndPartitions)

  override def getSortedBrokerList(): Seq[Int] = zkUtils.getSortedBrokerList()
}
