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

package ly.stealth.mesos.kafka.scheduler

import java.util
import java.util.Properties
import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.log.LogConfig
import ly.stealth.mesos.kafka.{Partition, Topic}
import org.apache.kafka.common.KafkaException
import scala.collection.JavaConversions._
import scala.collection.{Map, Seq, mutable}

class Topics {
  def getTopic(name: String): Topic = {
    if (name == null) return null
    val topics: util.List[Topic] = getTopics.filter(_.name == name)
    if (topics.nonEmpty) topics(0) else null
  }

  def getTopics: Seq[Topic] = {
    val names = ZkUtilsWrapper().getAllTopics()

    val assignments: mutable.Map[String, Map[Int, Seq[Int]]] = ZkUtilsWrapper().getPartitionAssignmentForTopics(names)
    val configs = AdminUtilsWrapper().fetchAllTopicConfigs()

    names.sorted.map(t => Topic(
      t,
      assignments.getOrElse(t, null),
      propertiesAsScalaMap(configs.getOrElse(t, null)))
    )
  }

  private val NoLeader = LeaderIsrAndControllerEpoch(LeaderAndIsr(LeaderAndIsr.NoLeader, -1, List(), -1), -1)

  def getPartitions(topics: Seq[String]): Map[String, Set[Partition]] = {
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
      tap.topic -> Partition(
        tap.partition,
        replicas,
        partitionLeader.leaderAndIsr.isr,
        partitionLeader.leaderAndIsr.leader,
        replicas.headOption.getOrElse(-1)
      )
    }).groupBy(_._1).mapValues(v => v.map(_._2))
  }

  def fairAssignment(partitions: Int = 1, replicas: Int = 1, brokers: Seq[Int] = null, fixedStartIndex: Int = -1, startPartitionId: Int = -1): Map[Int, Seq[Int]] = {
    var brokers_ = brokers

    if (brokers_ == null) {
      brokers_ = ZkUtilsWrapper().getSortedBrokerList()
    }

    AdminUtilsWrapper().assignReplicasToBrokers(brokers_, partitions, replicas, fixedStartIndex, startPartitionId)
  }

  def addTopic(name: String, assignment: Map[Int, Seq[Int]] = null, options: Map[String, String] = null): Topic = {
    val config = new Properties()
    if (options != null)
      for ((k, v) <- options) config.setProperty(k, v)

    AdminUtilsWrapper().createOrUpdateTopicPartitionAssignmentPathInZK(
      name,
      Option(assignment).getOrElse(fairAssignment(1, 1, null)),
      config)
    getTopic(name)
  }

  def updateTopic(topic: Topic, options: util.Map[String, String]): Topic = {
    val config: Properties = new Properties()
    for ((k, v) <- options) config.setProperty(k, v)

    AdminUtilsWrapper().changeTopicConfig(topic.name, config)
    topic
  }

  def validateOptions(options: Map[String, String]): String = {
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
