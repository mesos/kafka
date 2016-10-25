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

package ly.stealth.mesos.kafka

import java.util
import java.util.Properties
import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.LeaderIsrAndControllerEpoch
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq, mutable}
import ly.stealth.mesos.kafka.Topics.Topic
import kafka.log.LogConfig
import org.apache.kafka.common.KafkaException

class Topics {
  def getTopic(name: String): Topics.Topic = {
    if (name == null) return null
    val topics: util.List[Topic] = getTopics.filter(_.name == name)
    if (topics.nonEmpty) topics(0) else null
  }

  def getTopics: util.List[Topics.Topic] = {
    val names = ZkUtilsWrapper().getAllTopics()

    val assignments: mutable.Map[String, Map[Int, Seq[Int]]] = ZkUtilsWrapper().getPartitionAssignmentForTopics(names)
    val configs = ZkUtilsWrapper().fetchAllTopicConfigs()

    val topics = new util.ArrayList[Topics.Topic]
    for (name <- names.sorted)
      topics.add(Topics.Topic(
        name,
        assignments.getOrElse(name, null),
        propertiesAsScalaMap(configs.getOrElse(name, null))
      ))

    topics
  }

  private val NoLeader = LeaderIsrAndControllerEpoch(LeaderAndIsr(LeaderAndIsr.NoLeader, -1, List(), -1), -1)

  def getPartitions(topics: util.List[String]): Map[String, Set[Topics.Partition]] = {
    // returns topic name -> (partition -> brokers)
    val assignments = ZkUtilsWrapper().getPartitionAssignmentForTopics(topics)
    val topicAndPartitions = assignments.flatMap {
      case (topic, partitions) => partitions.map {
        case (partition, _)  => TopicAndPartition(topic, partition)
      }
    }.toSet
    val leaderAndisr = ZkUtilsWrapper().getPartitionLeaderAndIsrForTopics(topicAndPartitions)

    topicAndPartitions.map(tap => {
      val replicas = assignments(tap.topic).getOrElse(tap.partition, Seq())
      val partitionLeader = leaderAndisr.getOrElse(tap, NoLeader)
      tap.topic -> new Topics.Partition(
        tap.partition,
        replicas,
        partitionLeader.leaderAndIsr.isr,
        partitionLeader.leaderAndIsr.leader,
        replicas.headOption.getOrElse(-1)
      )
    }).groupBy(_._1).mapValues(v => v.map(_._2))
  }

  def fairAssignment(partitions: Int = 1, replicas: Int = 1, brokers: util.List[Int] = null, fixedStartIndex: Int = -1, startPartitionId: Int = -1): util.Map[Int, util.List[Int]] = {
    var brokers_ = brokers

    if (brokers_ == null) {
      brokers_ = ZkUtilsWrapper().getSortedBrokerList()
    }

    ZkUtilsWrapper().assignReplicasToBrokers(brokers_, partitions, replicas, fixedStartIndex, startPartitionId).mapValues(new util.ArrayList[Int](_))
  }

  def addTopic(name: String, assignment: util.Map[Int, util.List[Int]] = null, options: util.Map[String, String] = null): Topic = {
    var assignment_ = assignment
    if (assignment_ == null) assignment_ = fairAssignment(1, 1, null)

    val config: Properties = new Properties()
    if (options != null)
      for ((k, v) <- options) config.setProperty(k, v)

    ZkUtilsWrapper().createOrUpdateTopicPartitionAssignmentPathInZK(name, assignment_.mapValues(_.toList), config)
    getTopic(name)
  }

  def updateTopic(topic: Topic, options: util.Map[String, String]): Unit = {
    val config: Properties = new Properties()
    for ((k, v) <- options) config.setProperty(k, v)

    ZkUtilsWrapper().changeTopicConfig(topic.name, config)
  }

  def validateOptions(options: util.Map[String, String]): String = {
    val config: Properties = new Properties()
    for ((k, v) <- options) config.setProperty(k, v)

    try { LogConfig.validate(config) }
    catch {
      case e: IllegalArgumentException => return e.getMessage
      case e: KafkaException => return e.getMessage
    }
    null
  }
}

object Topics {
  class Exception(message: String) extends java.lang.Exception(message)

  case class Partition(
      id: Int = 0,
      replicas: Seq[Int] = null,
      isr: Seq[Int] = null,
      leader: Int = 0,
      expectedLeader: Int = 0)

  case class Topic(
    name: String = null,
    partitions: Map[Int, Seq[Int]] = Map(),
    options: Map[String, String] = Map()
  ) {
    def partitionsState: String = {
      var s: String = ""
      for ((partition, brokers) <- partitions.toSeq.sortBy(_._1)) {
        if (!s.isEmpty) s += ", "
        s += partition + ":[" + brokers.mkString(",") + "]"
      }
      s
    }
  }
}
