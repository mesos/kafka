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

import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Random

import scala.collection.JavaConversions._
import scala.collection.{Map, Seq, mutable}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.admin._
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import net.elodina.mesos.util.Period
import org.apache.log4j.Logger

class Rebalancer {
  private val logger: Logger = Logger.getLogger(this.getClass)

  @volatile private var assignment: Map[TopicAndPartition, Seq[Int]] = null
  @volatile private var reassignment: Map[TopicAndPartition, Seq[Int]] = null

  private def newZkClient: ZkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)

  def running: Boolean = {
    val zkClient = newZkClient
    try { zkClient.exists(ZkUtils.ReassignPartitionsPath) }
    finally { zkClient.close() }
  }

  def start(topics: util.List[String], brokers: util.List[String], replicas: Int = -1, fixedStartIndex: Int = -1, startPartitionId: Int = -1, realign: Boolean = false): Unit = {
    if (topics.isEmpty) throw new Rebalancer.Exception("no topics")

    logger.info(s"Starting rebalance for topics ${topics.mkString(",")} on brokers ${brokers.mkString(",")} with ${if (replicas == -1) "<default>" else replicas} replicas and realign=$realign")

    var assignment: Map[TopicAndPartition, Seq[Int]] = ZkUtilsWrapper().getReplicaAssignmentForTopics(topics)
    var reassignment: Map[TopicAndPartition, Seq[Int]] = null
    // Realignment mode
    if (realign) {
      // Get our reassignment, disturbing as few partitions as possible
      reassignment = getRealignments(brokers.map(Integer.parseInt), topics, assignment, replicas, fixedStartIndex)
      // Some partitions do not require any changes in membership. Remove them completely so the
      // controller never even looks at them.
      val noOpPartitions = assignment.keySet.diff(reassignment.keySet)
      assignment --= noOpPartitions
    }
    // Regular old scramble mode
    else {
      reassignment = getReassignments(brokers.map(Integer.parseInt), topics, assignment, replicas, fixedStartIndex, startPartitionId)
    }

    // Don't do anything if all assignments remain the same
    var identicalMaps = true
    assignment.keySet.foreach { topic =>
      val oldBrokers = assignment.get(topic).toSet
      val newBrokers = reassignment.get(topic).toSet
      if ((oldBrokers.diff(newBrokers).size > 0) || (newBrokers.diff(oldBrokers).size > 0)) {
        identicalMaps = false
      }
    }
    if (identicalMaps) {
      logger.info("rebalance deemed unnecessary, no changes made")
      return
    }

    reassignPartitions(reassignment)
    this.reassignment = reassignment
    this.assignment = assignment
  }

  def state: String = {
    if (assignment == null) return ""
    var s = ""
    val reassigning: Map[TopicAndPartition, Seq[Int]] = ZkUtilsWrapper().getPartitionsBeingReassigned().mapValues(_.newReplicas)

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

  private def getReassignments(brokerIds: Seq[Int], topics: util.List[String], assignment: Map[TopicAndPartition, Seq[Int]], replicas: Int = -1, fixedStartIndex: Int = -1, startPartitionId: Int = -1): Map[TopicAndPartition, Seq[Int]] = {
    var reassignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    val byTopic: Map[String, Map[TopicAndPartition, Seq[Int]]] = assignment.groupBy(tp => tp._1.topic)
    byTopic.foreach { entry =>
      val topic = entry._1
      val rf: Int = if (replicas != -1) replicas else entry._2.valuesIterator.next().size
      val partitions: Int = entry._2.size
    
      val assignedReplicas: Map[Int, Seq[Int]] = AdminUtils.assignReplicasToBrokers(brokerIds, partitions, rf, fixedStartIndex, startPartitionId)
      reassignment ++= assignedReplicas.map(replicaEntry => TopicAndPartition(topic, replicaEntry._1) -> replicaEntry._2)
    }

    reassignment.toMap
  }

  private def getRealignments(brokerIds: Seq[Int], topics: util.List[String], assignment: Map[TopicAndPartition, Seq[Int]], replicas: Int = -1, fixedStartIndex: Int = -1): Map[TopicAndPartition, Seq[Int]] = {
    var realignment : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

    // Start at a random index unless told otherwise
    val rand = new Random
    var replacementReplicaIndex = if (fixedStartIndex != -1) fixedStartIndex else rand.nextInt(brokerIds.size)

    // Iterate over the current assignment. For each entry, determine if we need to make broker
    // replacements.
    assignment.foreach { case (topic, members)  =>
      val rf: Int = if (replicas != -1) replicas else members.size

      // If we don't have enough members for this topic, or some of the members are not in the
      // brokerIds list (which dictates the brokers we're allowed to use), make replacements.
      // Otherwise, we leave the partition out of consideration completely - no changes needed,
      // so let's not bother the controller.
      if((rf != members.size) || ((brokerIds intersect members).size != members.size)) {
        // Keep any members that do comply with the broker ID restrictions, up to rf.
        var newMembers = brokerIds.intersect(members)
        if(newMembers.size > rf) {
          newMembers = newMembers.slice(0, rf)
        }
        // Keep hunting for new brokers until we meet the replication factor requirements or we've
        // attempted all the brokers available.
        var replacementReplicasAttempted = 0
        while((newMembers.size < rf) && (replacementReplicasAttempted <= brokerIds.size)){
          val nextBrokerId = brokerIds.get(replacementReplicaIndex)
          // Make sure we don't double-assign a single broker. That would be a valid operation as
          // far as Kafka is concerned, but insane as far as any reasonable cluster owner would be
          // concerned.
          if(!newMembers.contains(nextBrokerId)) {
            newMembers :+= nextBrokerId
          }
          replacementReplicasAttempted += 1
          replacementReplicaIndex = (replacementReplicaIndex + replacementReplicasAttempted) % brokerIds.size
        }
        // If we didn't get enough members, we weren't passed an adequate number of acceptable
        // broker IDs. For example, if partition A has members [1, 2, 3], RF=3 and we are allowed
        // to use broker IDs [1, 2], these inputs are invalid.
        if (newMembers.size < rf)
          throw new Rebalancer.Exception("not enough candidate brokers to satisfy request")
        realignment += (topic -> newMembers)
      }
    }

    realignment.toMap
  }

  private def getReassignmentState(topicAndPartition: TopicAndPartition, reassigning: Map[TopicAndPartition, Seq[Int]]): String = {
    reassigning.get(topicAndPartition) match {
      case Some(partition) => "running"
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedBrokers = reassignment(topicAndPartition)
        val brokers = ZkUtilsWrapper().getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)

        if(brokers.sorted == assignedBrokers.sorted) "done"
        else "error"
    }
  }

  private def reassignPartitions(partitions: Map[TopicAndPartition, Seq[Int]]): Unit = {
    try {
      val json = ZkUtilsWrapper().getPartitionReassignmentZkData(partitions)
      ZkUtilsWrapper().createPersistentPath(ZkUtils.ReassignPartitionsPath, json)
    } catch {
      case ti: InvocationTargetException if ti.getCause.isInstanceOf[ZkNodeExistsException] =>
        throw new Rebalancer.Exception("rebalance is in progress")
    }
  }
}

object Rebalancer {
  class Exception(message: String) extends java.lang.Exception(message)
}
