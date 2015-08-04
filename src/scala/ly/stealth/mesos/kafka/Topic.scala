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

import kafka.admin.TopicCommand.TopicCommandOptions

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

class Topic {
  private val logger: Logger = Logger.getLogger(this.getClass)

  private def zkClient: ZkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)

  def getTopicLists(): List[String] = {
    val optsList = Array[String]()
    val topics = ZkUtils.getAllTopics(zkClient)
    logger.info("All Topics : " + topics.toString())
    topics.toList
  }

  def getTopic(nameExp: String): List[String] = {
    val optsList = Array[String]()
    val topics = ZkUtils.getAllTopics(zkClient)
    topics.filter(x => x.matches(nameExp))
    topics.toList
  }

  def createTopic(topic: String, partitions: String = "1", replicationFactor: String = "1", topicConfig: String): Unit = {

    val errors = new util.ArrayList[String]()

    var options: util.Map[String, String] = null

    try {
      options = Util.parseMap(topicConfig, nullValues = false)
      val config: List[String] = options.map { case (k,v) => (String.format("%s=%s",k,v)) }(collection.breakOut)
    }
    catch {
      case e: IllegalArgumentException => errors.add("Invalid options: " + e.getMessage)
    }

    //TODO get the configs working add --config key/value
    val cmd = Array("--zookeeper",Config.zk, "--create","--topic",topic,"--partitions", partitions,
                    "--replication-factor",replicationFactor)
    TopicCommand.createTopic(zkClient, new TopicCommandOptions(cmd))
  }


  def alterTopic(topic: String, partitions: String = "1", replicationFactor: String = "1", topicConfig: String): Unit = {
    val cmd = Array("--zookeeper",Config.zk, "--create","--topic",topic,"--partitions", partitions,
      "--replication-factor",replicationFactor)

    //TODO get the configs working add --config key/value
    TopicCommand.alterTopic(zkClient, new TopicCommandOptions(cmd))
  }
}

object Topic {
  class Exception(message: String) extends java.lang.Exception(message)
}