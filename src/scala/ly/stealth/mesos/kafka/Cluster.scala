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
import scala.util.parsing.json.{JSON, JSONArray, JSONObject}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.util.Collections
import java.io.{IOException, FileWriter, File}

class Cluster {
  private val brokers: util.List[Broker] = new util.concurrent.CopyOnWriteArrayList[Broker]()

  def getBrokers:util.List[Broker] = Collections.unmodifiableList(brokers)

  def getBroker(id: String): Broker = {
    for (broker <- brokers)
      if (broker.id == id) return broker
    null
  }

  def addBroker(broker: Broker): Unit = brokers.add(broker)

  def removeBroker(broker: Broker): Unit = brokers.remove(broker)

  def fromJson(root: Map[String, Object]): Unit = {
    if (root.contains("brokers"))
      for (brokerNode <- root("brokers").asInstanceOf[List[Map[String, Object]]]) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)
        brokers.add(broker)
      }
  }

  def toJson: JSONObject = {
    val obj = new mutable.LinkedHashMap[String, Object]()

    if (!brokers.isEmpty) {
      val brokerNodes = new ListBuffer[JSONObject]()
      for (broker <- brokers)
        brokerNodes.add(broker.toJson)
      obj("brokers") = new JSONArray(brokerNodes.toList)
    }

    new JSONObject(obj.toMap)
  }

  def load(clearTasks: Boolean) {
    if (!Cluster.stateFile.exists()) return

    val json: String = scala.io.Source.fromFile(Cluster.stateFile).mkString
    val node: Map[String, Object] = JSON.parseFull(json).getOrElse(null).asInstanceOf[Map[String, Object]]
    if (node == null) throw new IOException("Failed to parse json")

    brokers.clear()
    fromJson(node)

    if (clearTasks)
      for (broker <- brokers) broker.task = null
  }

  def expandIds(expr: String): util.List[String] = {
    val ids = new util.ArrayList[String]()

    for (_part <- expr.split(",")) {
      val part = _part.trim()

      if (part.equals("*"))
        for (broker <- brokers) ids.add(broker.id)
      else if (part.contains("..")) {
        val idx = part.indexOf("..")

        var start: Integer = null
        var end: Integer = null
        try {
          start = Integer.parseInt(part.substring(0, idx))
          end = Integer.parseInt(part.substring(idx + 2, part.length))
        } catch {
          case e: NumberFormatException => throw new IllegalArgumentException("Invalid expr " + expr)
        }

        for (id <- start.toInt until end + 1)
          ids.add("" + id)
      } else {
        var id: Integer = null
        try { id = Integer.parseInt(part) }
        catch { case e: NumberFormatException => throw new IllegalArgumentException("Invalid expr " + expr) }
        ids.add("" + id)
      }
    }

    ids
  }

  def save() {
    val json = "" + toJson

    val writer  = new FileWriter(Cluster.stateFile)
    try { writer.write(json) }
    finally { writer.close() }
  }
}

object Cluster {
  val stateFile: File = new File("scheduler.json")
}