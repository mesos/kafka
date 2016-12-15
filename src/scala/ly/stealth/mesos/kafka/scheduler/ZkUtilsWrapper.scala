package ly.stealth.mesos.kafka.scheduler

import java.lang.reflect.Method
import java.util.Properties
import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import kafka.utils.ZkUtils
import ly.stealth.mesos.kafka.Config
import net.elodina.mesos.util.Version
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import scala.collection.{Map, Set, mutable}

object ZKStringSerializer extends ZkSerializer {
  @throws(classOf[ZkMarshallingError])
  override def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  override def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

abstract class ZkUtilsWrapper(protected val cls: Class[_]) {
  protected def getMethod(name: String, paramTypes: Class[_]*) = {
    cls.getMethod(name, modifyClassArgs(paramTypes):_*)
  }

  protected def callMethod[T](m: Method, args: Any*): T = {
    m.invoke(getInstance, modifyArgs(args).map(_.asInstanceOf[AnyRef]) :_*).asInstanceOf[T]
  }

  protected def callUnitMethod(m: Method, args: Any*): Unit = {
    m.invoke(getInstance, modifyArgs(args).map(_.asInstanceOf[AnyRef]) :_*)
  }

  protected def callAdminMethod[T](m: Method, args: Any*): T = {
    m.invoke(AdminUtils, (Seq(getAdminZkParam) ++ args).map(_.asInstanceOf[AnyRef]) :_*).asInstanceOf[T]
  }

  protected def callUnitAdminMethod(m: Method, args: Any*): Unit = {
    m.invoke(AdminUtils, (Seq(getAdminZkParam) ++ args).map(_.asInstanceOf[AnyRef]) :_*)
  }

  protected def getInstance : Object
  protected def getAdminZkParam: AnyRef
  protected def getAdminZkClass: Class[_]
  protected def modifyArgs(args: Seq[Any]): Seq[Any] = args
  protected def modifyClassArgs(classArgs: Seq[Class[_]]): Seq[Class[_]] = classArgs

  private val getAllTopicsMethod = getMethod("getAllTopics")
  private val getReplicaAssignmentForTopicsMethod = getMethod("getReplicaAssignmentForTopics", classOf[Seq[String]])
  private val getPartitionsBeingReassignedMethod = getMethod("getPartitionsBeingReassigned")
  private val getReplicasForPartitionMethod = getMethod("getReplicasForPartition", classOf[String], classOf[Int])
  private val getPartitionAssignmentForTopicsMethod = getMethod("getPartitionAssignmentForTopics", classOf[Seq[String]])
  protected val getPartitionLeaderAndIsrForTopicsMethod = cls.getMethod("getPartitionLeaderAndIsrForTopics", classOf[ZkClient], classOf[Set[TopicAndPartition]])
  private val getSortedBrokerListMethod = getMethod("getSortedBrokerList")

  private val adminClass = AdminUtils.getClass
  private val fetchAllTopicConfigsMethod = adminClass.getMethod("fetchAllTopicConfigs", getAdminZkClass)
  private val createOrUpdateTopicPartitionAssignmentPathInZKMethod = adminClass.getMethod(
    "createOrUpdateTopicPartitionAssignmentPathInZK", getAdminZkClass, classOf[String], classOf[Map[Int, Seq[Int]]], classOf[Properties], classOf[Boolean])
  private val changeTopicConfigMethod = adminClass.getMethod("changeTopicConfig", getAdminZkClass, classOf[String], classOf[Properties])
  private val fetchEntityConfigMethod = adminClass.getMethod("fetchEntityConfig", getAdminZkClass, classOf[String], classOf[String])
  private val changeClientIdConfigMethod = adminClass.getMethod("changeClientIdConfig", getAdminZkClass, classOf[String], classOf[Properties])
  private val fetchAllEntityConfigsMethod = adminClass.getMethod("fetchAllEntityConfigs", getAdminZkClass, classOf[String])

  // ZkUtils methods
  def getAllTopics(): Seq[String] = callMethod(getAllTopicsMethod)
  def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]]
    = callMethod(getReplicaAssignmentForTopicsMethod, topics)
  def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext]
    = callMethod(getPartitionsBeingReassignedMethod)
  def getReplicasForPartition(topic: String, partition: Int): Seq[Int]
    = callMethod(getReplicasForPartitionMethod, topic, partition)
  def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit
  def createPersistentPath(path: String, data: String = ""): Unit
  def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]]
    = callMethod(getPartitionAssignmentForTopicsMethod, topics)
  def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
  def getSortedBrokerList(): Seq[Int]
    = callMethod(getSortedBrokerListMethod)

  // AdminUtils methods
  def fetchAllTopicConfigs(): Map[String, Properties]
    = callAdminMethod(fetchAllTopicConfigsMethod)

  def createOrUpdateTopicPartitionAssignmentPathInZK(topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false)
    = callUnitAdminMethod(createOrUpdateTopicPartitionAssignmentPathInZKMethod, topic, partitionReplicaAssignment, config, update)

  def changeTopicConfig(topic: String, configs: Properties)
    = callUnitAdminMethod(changeTopicConfigMethod, topic, configs)

  def fetchEntityConfig(entityType: String, entity: String): Properties
    = callAdminMethod(fetchEntityConfigMethod, entityType, entity)

  def changeClientIdConfig(clientId: String, configs: Properties)
    = callUnitAdminMethod(changeClientIdConfigMethod, clientId, configs)

  def fetchAllEntityConfigs(entityType: String): Map[String, Properties]
    = callAdminMethod(fetchAllEntityConfigsMethod, entityType)

  def assignReplicasToBrokers(ids: Seq[Int], nPartitions: Int,
    replicationFactor: Int,
    fixedStartIndex: Int = -1,
    startPartitionId: Int = -1): Map[Int, Seq[Int]]
}

object ZkUtilsWrapper {
  private trait HasOldPartitionReassignment {
    self: ZkUtilsWrapper =>

    private val getPartitionReassignmentZkDataMethod = cls.getMethod("getPartitionReassignmentZkData", classOf[Map[TopicAndPartition, Seq[Int]]])
    def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String
      = getPartitionReassignmentZkDataMethod.invoke(getInstance, partitionsToBeReassigned).asInstanceOf[String]

    def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit = {
      val json = getPartitionReassignmentZkData(partitionsToBeReassigned)
      createPersistentPath(ZkUtils.ReassignPartitionsPath, json)
    }
  }

  private trait HasUpdatePartitionReassignment {
    self: ZkUtilsWrapper =>

    private val updatePartitionReassignmentDataMethod = cls.getMethod("updatePartitionReassignmentData", classOf[Map[TopicAndPartition, Seq[Int]]])

    def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): Unit = {
      updatePartitionReassignmentDataMethod.invoke(getInstance, partitionsToBeReassigned)
    }
  }

  private trait HasRackAwareRebalanceMethods {
    self: ZkUtilsWrapper =>

    private val brokerMetadataObject = Class.forName("kafka.admin.BrokerMetadata$")
    private val brokerMetadataInstance = brokerMetadataObject.getField("MODULE$").get(null)
    private val brokerMetadataApply = brokerMetadataObject.getMethod("apply", classOf[Int], classOf[Option[_]])
    private val assignReplicasToBrokersMethod = adminClass.getMethod("assignReplicasToBrokers", classOf[Seq[_]], classOf[Int], classOf[Int], classOf[Int], classOf[Int])

    override def assignReplicasToBrokers(
      ids: Seq[Int],
      nPartitions: Int,
      replicationFactor: Int,
      fixedStartIndex: Int,
      startPartitionId: Int
    ): Map[Int, Seq[Int]] = {
      val brokers = ids.map(id => brokerMetadataApply.invoke(brokerMetadataInstance, id: Integer, None))
      assignReplicasToBrokersMethod.invoke(AdminUtils, brokers, nPartitions: Integer, replicationFactor: Integer, fixedStartIndex: Integer, startPartitionId: Integer).asInstanceOf[Map[Int, Seq[Int]]]
    }
  }

  private trait HasLegacyRebalanceMethods {
    self: ZkUtilsWrapper =>

    private val assignReplicasToBrokersMethod = adminClass.getMethod("assignReplicasToBrokers", classOf[Seq[_]], classOf[Int], classOf[Int], classOf[Int], classOf[Int])
    override def assignReplicasToBrokers(
      ids: Seq[Int],
      nPartitions: Int,
      replicationFactor: Int,
      fixedStartIndex: Int,
      startPartitionId: Int
    ): Map[Int, Seq[Int]] = {
      assignReplicasToBrokersMethod.invoke(AdminUtils, ids, nPartitions: Integer, replicationFactor: Integer, fixedStartIndex: Integer, startPartitionId: Integer).asInstanceOf[Map[Int, Seq[Int]]]
    }
  }

  private trait HasZkUtils {
    protected val zkUtils: Object
  }

  private trait Kafka9PlusZkUtils {
    self: ZkUtilsWrapper with HasZkUtils =>

    override def getInstance: AnyRef = zkUtils
    override def getAdminZkClass: Class[_] = zkUtils.getClass
    override def getAdminZkParam: AnyRef = zkUtils

    override def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
    = getPartitionLeaderAndIsrForTopicsMethod.invoke(getInstance, null, topicAndPartitions).asInstanceOf[mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]]

    private val createPersistentPathMethod = getMethod("createPersistentPath", classOf[String], classOf[String], classOf[java.util.List[ACL]])
    def createPersistentPath(path: String, data: String = "")
    = callUnitMethod(createPersistentPathMethod, path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE)
  }

  private class Kafka8ZkUtilsWrapper(zkClient: ZkClient)
      extends ZkUtilsWrapper(ZkUtils.getClass)
        with HasOldPartitionReassignment
        with HasLegacyRebalanceMethods {
    override def modifyClassArgs(classArgs: Seq[Class[_]]): Seq[Class[_]] = Seq(classOf[ZkClient]) ++ classArgs
    override def modifyArgs(args: Seq[Any]): Seq[Any] = Seq(zkClient) ++ args
    override def getInstance = ZkUtils
    override def getAdminZkClass: Class[_] = classOf[ZkClient]
    override def getAdminZkParam: AnyRef = zkClient

    override def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]
      = getPartitionLeaderAndIsrForTopicsMethod.invoke(getInstance, zkClient, topicAndPartitions).asInstanceOf[mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch]]


    private val createPersistentPathMethod = getMethod("createPersistentPath", classOf[String], classOf[String])
    def createPersistentPath(path: String, data: String = "")
      = callUnitMethod(createPersistentPathMethod, path, data)
  }

  private class Kafka9ZkUtilsWrapper(protected val zkUtils: Object)
      extends ZkUtilsWrapper(zkUtils.getClass)
        with HasOldPartitionReassignment
        with HasLegacyRebalanceMethods
        with HasZkUtils
        with Kafka9PlusZkUtils {
  }

  private class Kafka10ZkUtilsWrapper(protected val zkUtils: Object)
    extends ZkUtilsWrapper(zkUtils.getClass)
      with HasUpdatePartitionReassignment
      with HasRackAwareRebalanceMethods
      with HasZkUtils
      with Kafka9PlusZkUtils {

  }

  private def getInstance() = {
    val kafkaVersion = new Version(AppInfoParser.getVersion)

    val isKafka10Plus = kafkaVersion.compareTo(new Version("0.10.0.0")) >= 0
    val isKafka9Plus = kafkaVersion.compareTo(new Version("0.9.0.0")) >= 0

    if (isKafka9Plus) {
      val applyMethod = ZkUtils.getClass.getMethod("apply", classOf[String], Integer.TYPE, Integer.TYPE, java.lang.Boolean.TYPE)
      val zkUtilsObj = applyMethod.invoke(ZkUtils, Config.zk, 30000: java.lang.Integer, 30000: java.lang.Integer, false: java.lang.Boolean)

      if (isKafka10Plus) {
        new Kafka10ZkUtilsWrapper(zkUtilsObj)
      } else {
        new Kafka9ZkUtilsWrapper(zkUtilsObj)
      }
    } else {
      new Kafka8ZkUtilsWrapper(new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer))
    }
  }

  private var instance: Option[ZkUtilsWrapper] = None

  def apply(): ZkUtilsWrapper = instance match {
    case Some(i) => i
    case None =>
      instance = Some(getInstance())
      instance.get
  }
  def reset() = instance = None
}
