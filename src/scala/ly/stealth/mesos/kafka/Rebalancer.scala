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

import scala.Some
import scala.collection.JavaConversions._
import scala.collection.{mutable, Seq, Map}

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.{ZkUtils, ZKStringSerializer}
import ly.stealth.mesos.kafka.Util.Period
import org.apache.log4j.Logger

class Rebalancer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  @volatile private var assignment: Map[TopicAndPartition, Seq[Int]] = null
  @volatile private var reassignment: Map[TopicAndPartition, Seq[Int]] = null

  private def newZkClient: ZkClient = new ZkClient(Config.kafkaZkConnect, 30000, 30000, ZKStringSerializer)

  def running: Boolean = {
    val zkClient = newZkClient
    try { zkClient.exists(ZkUtils.ReassignPartitionsPath) }
    finally { zkClient.close() }
  }

  def start(_ids: util.List[String], topics: util.Map[String, Integer]): Unit = {
    if (topics.isEmpty) throw new Rebalancer.Exception("no topics")
    for ((topic, rf) <- topics) if (rf == null) throw new Rebalancer.Exception(s"no rf for topic $topic")

    logger.info(s"Starting rebalance for topics ${Util.formatMap(topics, valueSep = ':')} on brokers ${_ids.mkString(",")}")
    val zkClient = newZkClient
    try {
      val ids: util.List[Int] = _ids.map(Integer.parseInt)
      val assignment: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zkClient, topics.keys.toSeq)
      val reassignment: Map[TopicAndPartition, Seq[Int]] = getReassignments(ids, topics, assignment)

      reassignPartitions(zkClient, reassignment)
      this.reassignment = reassignment
      this.assignment = assignment
    } finally {
      zkClient.close()
    }
  }

  def state: String = {
    if (assignment == null) return ""
    var s = ""

    val zkClient = newZkClient
    try {
      val reassigning: Map[TopicAndPartition, Seq[Int]] = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas)

      val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
      for (topic <- byTopic.keys.to[List].sorted) {
        s += topic + "\n"

        val partitions: Map[TopicAndPartition, Seq[Int]] = byTopic.getOrElse(topic, null)
        for (topicAndPartition <- partitions.keys.to[List].sortBy(_.partition)) {
          val brokers = partitions.getOrElse(topicAndPartition, null)
          s += "  " + topicAndPartition.partition + ": " + brokers.mkString(",")
          s += " -> "
          s += reassignment.getOrElse(topicAndPartition, null).mkString(",")
          s += " - " + getReassignmentState(zkClient, topicAndPartition, reassigning)
          s += "\n"
        }
      }
    } finally {
      zkClient.close()
    }

    s
  }
  
  def waitFor(running: Boolean, timeout: Period): Boolean = {
    def matches: Boolean = this.running == running

    var t = timeout.ms
    while (t > 0 && !matches) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    matches
  }

  def expandTopics(expr: String): util.Map[String, Integer] = {
    val zkClient = newZkClient
    try {
      val assignment: mutable.Map[TopicAndPartition, Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zkClient, ZkUtils.getAllTopics(zkClient))
      val replicas: util.Map[String, Int] = assignment.map(e => (e._1.topic, e._2.size))

      val topics: util.Map[String, Integer] = new util.LinkedHashMap()
      for ((topic, rf) <- Util.parseMap(expr, ',', ':', nullValues = true))
        topics.put(topic, if (rf != null) Integer.parseInt(rf) else null)

      // expand wildcard
      if (topics.containsKey("*")) {
        for (topic <- replicas.keys.toList.sorted)
          if (!topics.containsKey(topic)) topics.put(topic, null)

        val wildcardRf: Integer = topics.remove("*")
        if (wildcardRf != null)
          for ((topic, rf) <- topics)
            if (rf == null) topics.put(topic, wildcardRf)
      }

      // remove not existent topics
      topics.keySet().retainAll(replicas.keySet())

      // set default rf if needed
      for ((topic, rf) <- topics)
        if (rf == null) topics.put(topic, replicas.get(topic))

      topics
    } finally {
      zkClient.close()
    }
  }

  private def getReassignments(brokerIds: Seq[Int], topics: util.Map[String, Integer], assignment: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    var reassignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    byTopic.foreach { entry =>
      val topic = entry._1
      val rf: Int = topics.get(topic)
      val partitions: Int = entry._2.size
    
      val assignedReplicas: Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerIds, partitions, rf)
      reassignment ++= assignedReplicas.map(replicaEntry => TopicAndPartition(topic, replicaEntry._1) -> replicaEntry._2)
    }

    reassignment.toMap
  }

  private def getReassignmentState(zkClient: ZkClient, topicAndPartition: TopicAndPartition, reassigning: Map[TopicAndPartition, Seq[Int]]): String = {
    reassigning.get(topicAndPartition) match {
      case Some(partition) => "running"
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedBrokers = reassignment(topicAndPartition)
        val brokers = ZkUtils.getReplicasForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition)

        if(brokers.sorted == assignedBrokers.sorted) "done"
        else "error"
    }
  }

  private def reassignPartitions(zkClient: ZkClient, partitions: Map[TopicAndPartition, Seq[Int]]): Unit = {
    try {
      val json = ZkUtils.getPartitionReassignmentZkData(partitions)
      ZkUtils.createPersistentPath(zkClient, ZkUtils.ReassignPartitionsPath, json)
    } catch {
      case ze: ZkNodeExistsException => throw new Rebalancer.Exception("rebalance is in progress")
    }
  }
}

object Rebalancer {
  class Exception(message: String) extends java.lang.Exception(message)
}
