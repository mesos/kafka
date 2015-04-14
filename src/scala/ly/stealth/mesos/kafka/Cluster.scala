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
import org.apache.mesos.Protos.FrameworkID

import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.util.Collections
import java.io.{FileWriter, File}
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.exception.ZkNodeExistsException

class Cluster {
  private val brokers: util.List[Broker] = new util.concurrent.CopyOnWriteArrayList[Broker]()
  private[kafka] var rebalancer: Rebalancer = new Rebalancer()
  private[kafka] var frameworkId: Option[FrameworkID] = None

  def getBrokers:util.List[Broker] = Collections.unmodifiableList(brokers)

  def getBroker(id: String): Broker = {
    for (broker <- brokers)
      if (broker.id == id) return broker
    null
  }

  def addBroker(broker: Broker): Broker = {
    brokers.add(broker)
    broker
  }

  def removeBroker(broker: Broker): Unit = brokers.remove(broker)

  def clear(): Unit = brokers.clear()
  def load() = Cluster.storage.load(this)
  def save() = Cluster.storage.save(this)

  def expandIds(expr: String): util.List[String] = {
    val ids = new util.TreeSet[String]()

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

    new util.ArrayList(ids)
  }

  def fromJson(root: Map[String, Object]): Unit = {
    if (root.contains("brokers")) {
      for (brokerNode <- root("brokers").asInstanceOf[List[Map[String, Object]]]) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)
        brokers.add(broker)
      }
    }

    if (root.contains("frameworkId")) {
      frameworkId = Some(FrameworkID.newBuilder().setValue(root("frameworkId").asInstanceOf[String]).build())
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

    frameworkId.map { id =>
      obj("frameworkId") = id.getValue
    }

    new JSONObject(obj.toMap)
  }
}

object Cluster {
  var storage: Storage = newStorage(Config.clusterStorage)

  def newStorage(s: String): Storage = {
    if (s.startsWith("file:")) return new FsStorage(new File(s.substring("file:".length)))
    else if (s.startsWith("zk:")) return new ZkStorage(s.substring("zk:".length))
    throw new IllegalStateException("Unsupported storage " + s)
  }

  abstract class Storage {
    def load(cluster: Cluster): Unit = {
      val json: String = loadJson
      if (json == null) return
      
      val node: Map[String, Object] = Util.parseJson(json)
      cluster.brokers.clear()
      cluster.fromJson(node)
    }
    
    def save(cluster: Cluster): Unit = {
      saveJson("" + cluster.toJson)
    }
    
    protected def loadJson: String
    protected def saveJson(json: String): Unit
  }

  class FsStorage(val file: File) extends Storage {
    protected def loadJson: String = {
      if (!file.exists) null else scala.io.Source.fromFile(file).mkString
    }

    protected def saveJson(json: String): Unit = {
      val writer  = new FileWriter(file)
      try { writer.write(json) }
      finally { writer.close() }
    }
  }

  object FsStorage {
    val DEFAULT_FILE: File = new File("kafka-mesos.json")
  }

  class ZkStorage(val path: String) extends Storage {
    def zkClient: ZkClient = new ZkClient(Config.kafkaZkConnect, 30000, 30000, ZKStringSerializer)

    protected def loadJson: String = {
      val zkClient = this.zkClient
      try { zkClient.readData(path, true).asInstanceOf[String] }
      finally { zkClient.close() }
    }

    protected def saveJson(json: String): Unit = {
      val zkClient = this.zkClient
      try { zkClient.createPersistent(path, json) }
      catch { case e: ZkNodeExistsException => zkClient.writeData(path, json) }
      finally { zkClient.close() }
    }
  }
}