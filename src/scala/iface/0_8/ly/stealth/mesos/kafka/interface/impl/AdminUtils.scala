package ly.stealth.mesos.kafka.interface.impl

import java.util.Properties
import kafka.admin.{AdminUtils => KafkaAdminUtils}
import kafka.utils.ZKStringSerializer
import ly.stealth.mesos.kafka.interface.AdminUtilsProxy
import org.I0Itec.zkclient.ZkClient
import scala.collection.Map

class AdminUtils(zkUrl: String) extends AdminUtilsProxy {
  private val zkClient = new ZkClient(zkUrl, 30000, 30000, ZKStringSerializer)

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
}
