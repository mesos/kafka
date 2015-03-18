package ly.stealth.mesos.kafka

import java.util

import scala.Some
import scala.collection.JavaConversions._
import scala.collection.{mutable, Seq, Map}

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, ZKStringSerializer}

object Rebalancer {
  private val zk = new ZkClient(Config.kafkaZkConnect, 30000, 30000, ZKStringSerializer)
  @volatile private var assignment: Map[TopicAndPartition, Seq[Int]] = null
  @volatile private var reassignment: Map[TopicAndPartition, Seq[Int]] = null

  def running: Boolean = zk.exists(ZkUtils.ReassignPartitionsPath)

  def start(_ids: util.List[String], _topics: util.List[String]): Boolean = {
    val ids: util.List[Int] = _ids.map(Integer.parseInt)
    val topics: Seq[String] = if (_topics == null) ZkUtils.getAllTopics(zk) else _topics

    val assignment: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zk, topics)
    val reassignment = getReassignments(ids, assignment)

    if (!reassignPartitions(reassignment)) return false
    this.reassignment = reassignment
    this.assignment = assignment
    true
  }

  def state: String = {
    if (assignment == null) return ""
    
    var s = ""
    val reassigning: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getPartitionsBeingReassigned(zk).mapValues(_.newReplicas)
    
    val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    for (topic <- byTopic.keys.to[List].sorted) {
      s += topic + "\n"

      val partitions: Map[TopicAndPartition, Seq[Int]] = byTopic.getOrElse(topic, null)
      for (topicAndPartition <- partitions.keys.to[List].sortBy(_.partition)) {
        val brokers = partitions.getOrElse(topicAndPartition, null)
        s += "  " + topicAndPartition.partition + ": " + brokers.mkString(",")
        s += " -> "
        s += reassignment.getOrElse(topicAndPartition, null).mkString(",")
        s += " - " + getReassignmentState(topicAndPartition, reassigning)
        s += "\n"
      }
    }

    s
  }

  private def getReassignments(brokerIds: Seq[Int], assignment: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    var reassignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    byTopic.foreach { entry =>
      val assignedReplicas: Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerIds, entry._2.size, entry._2.head._2.size)
      reassignment ++= assignedReplicas.map(replicaEntry => TopicAndPartition(entry._1, replicaEntry._1) -> replicaEntry._2)
    }

    reassignment.toMap
  }

  private def getReassignmentState(topicAndPartition: TopicAndPartition, reassigning: Map[TopicAndPartition, Seq[Int]]): String = {
    reassigning.get(topicAndPartition) match {
      case Some(partition) => "running"
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedBrokers = reassignment(topicAndPartition)
        val brokers = ZkUtils.getReplicasForPartition(zk, topicAndPartition.topic, topicAndPartition.partition)

        if(brokers.sorted == assignedBrokers.sorted) "done"
        else "error"
    }
  }

  private def reassignPartitions(partitions: Map[TopicAndPartition, Seq[Int]]): Boolean = {
    try {
      val json = ZkUtils.getPartitionReassignmentZkData(partitions)
      ZkUtils.createPersistentPath(zk, ZkUtils.ReassignPartitionsPath, json)
      true
    } catch {
      case ze: ZkNodeExistsException => false
    }
  }
}
