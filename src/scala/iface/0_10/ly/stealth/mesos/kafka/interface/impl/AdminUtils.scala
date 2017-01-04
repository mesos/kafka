package ly.stealth.mesos.kafka.interface.impl

import kafka.utils.{ZkUtils => KafkaZkUtils}
import kafka.admin.{BrokerMetadata, AdminUtils => KafkaAdminUtils}
import java.util.Properties
import ly.stealth.mesos.kafka.interface.AdminUtilsProxy
import scala.collection.Map


class AdminUtils(zkUrl: String) extends AdminUtilsProxy {
  private val zkUtils = KafkaZkUtils(zkUrl, 30000, 30000, isZkSecurityEnabled = false)
  override def fetchAllTopicConfigs(): Map[String, Properties] = KafkaAdminUtils.fetchAllTopicConfigs(zkUtils)

  override def createOrUpdateTopicPartitionAssignmentPathInZK(
    topic: String,
    partitionReplicaAssignment: Map[Int, Seq[Int]],
    config: Properties,
    update: Boolean
  ): Unit = KafkaAdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, partitionReplicaAssignment, config, update)

  override def changeTopicConfig(
    topic: String,
    configs: Properties
  ): Unit = KafkaAdminUtils.changeTopicConfig(zkUtils, topic, configs)

  override def fetchEntityConfig(
    entityType: String,
    entity: String
  ): Properties = KafkaAdminUtils.fetchEntityConfig(zkUtils, entityType, entity)

  override def changeClientIdConfig(
    clientId: String,
    configs: Properties
  ): Unit = KafkaAdminUtils.changeClientIdConfig(zkUtils, clientId, configs)

  override def fetchAllEntityConfigs(entityType: String): Map[String, Properties]
  = KafkaAdminUtils.fetchAllEntityConfigs(zkUtils, entityType)

  override def assignReplicasToBrokers(
    ids: Seq[Int],
    nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int,
    startPartitionId: Int
  ): Map[Int, Seq[Int]] = {
    val md = ids.map(BrokerMetadata(_, None))
    KafkaAdminUtils.assignReplicasToBrokers(md, nPartitions, replicationFactor, fixedStartIndex, startPartitionId)
  }
}
