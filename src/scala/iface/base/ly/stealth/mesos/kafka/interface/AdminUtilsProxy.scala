package ly.stealth.mesos.kafka.interface

import ly.stealth.mesos.kafka.interface.impl.AdminUtils
import java.util.Properties
import scala.collection.Map

trait AdminUtilsProxy {
  def fetchAllTopicConfigs(): Map[String, Properties]

  def createOrUpdateTopicPartitionAssignmentPathInZK(topic: String,
    partitionReplicaAssignment: Map[Int, Seq[Int]],
    config: Properties = new Properties,
    update: Boolean = false)

  def changeTopicConfig(topic: String, configs: Properties)

  def fetchEntityConfig(entityType: String, entity: String): Properties

  def changeClientIdConfig(clientId: String, configs: Properties)

  def fetchAllEntityConfigs(entityType: String): Map[String, Properties]

  def assignReplicasToBrokers(ids: Seq[Int], nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int = -1,
    startPartitionId: Int = -1): Map[Int, Seq[Int]]
}

object AdminUtilsProxy {
  def apply(zkUrl: String) = new AdminUtils(zkUrl)
}
