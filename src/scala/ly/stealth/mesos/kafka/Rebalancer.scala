package ly.stealth.mesos.kafka

import java.util
import kafka.admin._
import scala.Some

import scala.collection.JavaConversions._
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, ZKStringSerializer}
import scala.collection.{mutable, Seq, Map}
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.exception.ZkNodeExistsException

object Rebalancer {
  private val zk = new ZkClient(Config.kafkaZkConnect, 30000, 30000, ZKStringSerializer)
  @volatile private var assignment: Map[TopicAndPartition, Seq[Int]] = null
  @volatile private var reassignment: Map[TopicAndPartition, Seq[Int]] = null

  def running: Boolean = zk.exists(ZkUtils.ReassignPartitionsPath)

  def start(_ids: util.List[String], _topics: util.List[String]): Boolean = {
    val ids: util.List[Int] = _ids.map(Integer.parseInt)
    val topics: Seq[String] = if (_topics == null) ZkUtils.getAllTopics(zk) else _topics

    val assignment: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zk, topics)
    val reassignment = getReassignment(ids, assignment)

    if (!reassignPartitions(reassignment)) return false
    this.reassignment = reassignment
    this.assignment = assignment
    true
  }

  private def status: String = {
    val statuses: Map[TopicAndPartition, ReassignmentStatus] = checkIfReassignmentSucceeded(reassignment)
    statuses.foreach { partition =>
      partition._2 match {
        case ReassignmentCompleted => // todo remove deps from ReassignmentCompleted
          println("Reassignment of partition %s completed successfully".format(partition._1))
        case ReassignmentFailed =>
          println("Reassignment of partition %s failed".format(partition._1))
        case ReassignmentInProgress =>
          println("Reassignment of partition %s is still in progress".format(partition._1))
      }
    }

    "" // todo
  }

  private def getReassignment(brokerIds: Seq[Int], assignment: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    var reassignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val groupedByTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    groupedByTopic.foreach { topicInfo =>
      val assignedReplicas: Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerIds, topicInfo._2.size, topicInfo._2.head._2.size)
      reassignment ++= assignedReplicas.map(replicaInfo => (TopicAndPartition(topicInfo._1, replicaInfo._1) -> replicaInfo._2))
    }

    reassignment.toMap
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

  private def checkIfReassignmentSucceeded(reassignment: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getPartitionsBeingReassigned(zk).mapValues(_.newReplicas)

    reassignment.map { entry =>
      (entry._1, checkIfPartitionReassignmentSucceeded(entry._1, entry._2, reassignment, partitionsBeingReassigned))
    }
  }

  private def checkIfPartitionReassignmentSucceeded(topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = ZkUtils.getReplicasForPartition(zk, topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas.sorted == newReplicas.sorted)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }
}
