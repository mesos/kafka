package ly.stealth.mesos.kafka.interface.impl

import kafka.utils.{ZkUtils => KafkaZkUtils, ZKStringSerializer}
import kafka.common.TopicAndPartition
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import ly.stealth.mesos.kafka.interface.ZkUtilsProxy
import org.I0Itec.zkclient.ZkClient
import scala.collection.{Map, Set, mutable}

class ZkUtils(zkUrl: String) extends ZkUtilsProxy {
  private val zkClient = new ZkClient(zkUrl, 30000, 30000, ZKStringSerializer)

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
