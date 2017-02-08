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
import scala.collection.JavaConversions._
import java.util.Collections
import java.io.{File, FileWriter}
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.{Quotas, Rebalancer, Topics, ZKStringSerializer}
import ly.stealth.mesos.kafka.scheduler.mesos.KafkaMesosScheduler
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

class Cluster {
  val version: String = SchedulerVersion.value
  private val brokers: util.List[Broker] = new util.concurrent.CopyOnWriteArrayList[Broker]()
  private[kafka] var rebalancer: Rebalancer = new Rebalancer()
  private[kafka] var topics: Topics = new Topics()
  private[kafka] var quotas: Quotas = new Quotas()
  private[kafka] var frameworkId: String = null

  def getBrokers:util.List[Broker] = Collections.unmodifiableList(brokers)

  def getBroker(id: Int): Broker = {
    for (broker <- brokers)
      if (broker.id == id) return broker
    null
  }

  def getBrokerByTaskId(taskId: String): Option[Broker] = {
    for (broker <- brokers)
      if (broker.task != null && broker.task.id == taskId)
        return Some(broker)
    None
  }

  def addBroker(broker: Broker): Broker = {
    brokers.add(broker)
    broker
  }

  def removeBroker(broker: Broker): Unit = brokers.remove(broker)

  def clear(): Unit = brokers.clear()
  def save() = Cluster.storage.save(this)
}

object Cluster {
  var storage: Storage = newStorage(Config.storage)
  def load() = storage.load()

  def newStorage(s: String): Storage = {
    if (s.startsWith("file:")) return new FsStorage(new File(s.substring("file:".length)))
    else if (s.startsWith("zk:")) return new ZkStorage(s.substring("zk:".length))
    throw new IllegalStateException("Unsupported storage " + s)
  }

  abstract class Storage {
    def load(): Cluster = {
      val json: String = loadJson
      if (json == null) return new Cluster()
      
      val cluster = JsonUtil.fromJson[Cluster](json)
      save(cluster)

      cluster
    }
    
    def save(cluster: Cluster): Unit = {
      saveJson(JsonUtil.toJson(cluster))
    }
    
    protected def loadJson: String
    protected def saveJson(json: String): Unit
  }

  class FsStorage(val file: File) extends Storage {
    protected def loadJson: String = {
      if (!file.exists) return null
      val source = scala.io.Source.fromFile(file)
      try source.mkString finally source.close()
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
    createChrootIfRequired()
    val zkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)

    private def createChrootIfRequired(): Unit = {
      val slashIdx: Int = Config.zk.indexOf('/')
      if (slashIdx == -1) return

      val chroot = Config.zk.substring(slashIdx)
      val zkConnect = Config.zk.substring(0, slashIdx)

      val client = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      try { client.createPersistent(chroot, true) }
      finally { client.close() }
    }

    protected def loadJson: String = {
      zkClient.readData(path, true).asInstanceOf[String]
    }

    protected def saveJson(json: String): Unit = {
      if (zkClient.exists(path)) {
        zkClient.writeData(path, json)
      }
      else {
        try { zkClient.createPersistent(path, json) }
        catch { case e: ZkNodeExistsException => zkClient.writeData(path, json) }
      }
    }
  }
}
