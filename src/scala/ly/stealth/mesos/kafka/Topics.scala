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

import scala.collection.JavaConversions._
import scala.collection.{mutable, Seq, Map}

import org.I0Itec.zkclient.ZkClient

import kafka.admin._
import kafka.utils.{ZkUtils, ZKStringSerializer}
import scala.util.parsing.json.JSONObject
import ly.stealth.mesos.kafka.Topics.Topic
import kafka.log.LogConfig

class Topics {
  private def newZkClient: ZkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)

  def getTopic(name: String): Topics.Topic = {
    val topics: util.List[Topic] = getTopics(name)
    if (topics.length > 0) topics(0) else null
  }

  def getTopics(name: String = null): util.List[Topics.Topic] = {
    val zkClient = newZkClient

    try {
      var names = ZkUtils.getAllTopics(zkClient)
      if (name != null) names = names.filter(_.matches(name))

      val assignments: mutable.Map[String, Map[Int, Seq[Int]]] = ZkUtils.getPartitionAssignmentForTopics(zkClient, names)
      val configs = AdminUtils.fetchAllTopicConfigs(zkClient)

      val topics = new util.ArrayList[Topics.Topic]
      for (name <- names.sorted)
        topics.add(new Topics.Topic(
          name,
          assignments.getOrElse(name, null).mapValues(brokers => new util.ArrayList[Int](brokers)),
          new util.TreeMap[String, String](propertiesAsScalaMap(configs.getOrElse(name, null)))
        ))

      topics
    } finally {
      zkClient.close()
    }
  }

  def addTopic(name: String, partitions: Int = 1, replicas: Int = 1, options: util.Map[String, String]): Topic = {
    val zkClient = newZkClient
    try {
      val config: Properties = new Properties()
      for ((k, v) <- options) config.setProperty(k, v)
      AdminUtils.createTopic(zkClient, name, partitions, replicas, config)
      getTopic(name)
    } finally {
      zkClient.close()
    }
  }

  def updateTopic(topic: Topic, options: util.Map[String, String]): Unit = {
    val zkClient = newZkClient
    try {
      val config: Properties = new Properties()
      for ((k, v) <- options) config.setProperty(k, v)
      AdminUtils.changeTopicConfig(zkClient, topic.name, config)
    } finally {
      zkClient.close()
    }
  }

  def validateOptions(options: util.Map[String, String]): String = {
    val config: Properties = new Properties()
    for ((k, v) <- options) config.setProperty(k, v)

    try { LogConfig.validate(config) }
    catch { case e: IllegalArgumentException => return e.getMessage }
    null
  }
}

object Topics {
  class Exception(message: String) extends java.lang.Exception(message)
  
  class Topic(
    _name: String,
    _partitions: util.Map[Int, util.List[Int]],
    _options: util.Map[String, String]
  ) {
    val name: String = _name
    val partitions: util.Map[Int, util.List[Int]] = _partitions
    val options: util.Map[String, String] = _options

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()
      obj("name") = name

      val partitionsObj = new collection.mutable.LinkedHashMap[String, Any]()
      for ((partition, brokers) <- partitions)
        partitionsObj.put("" + partition, brokers.mkString(", "))
      obj("partitions") = new JSONObject(partitionsObj.toMap)

      obj.put("options", new JSONObject(options.toMap))
      new JSONObject(obj.toMap)
    }
  }
}