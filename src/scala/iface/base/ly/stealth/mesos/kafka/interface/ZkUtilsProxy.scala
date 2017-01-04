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