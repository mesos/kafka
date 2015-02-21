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
import scala.util.parsing.json.JSONObject
import scala.collection
import org.apache.mesos.Protos.Offer

class Broker(_id: String = "0") {
  var id: String = _id
  @volatile var started: Boolean = false

  var host: String = null
  var cpus: Double = 1
  var mem: Long = 128

  var attributes: String = null
  var options: String = null

  def attributeMap: util.Map[String, String] = Broker.parseMap(attributes, ";", ":")
  def optionMap: util.Map[String, String] = Broker.parseMap(options, ";", "=")

  @volatile var task: Broker.Task = null

  def copy(): Broker = {
    val broker: Broker = new Broker()
    broker.id = id
    broker.started = started

    broker.host = host
    broker.cpus = cpus
    broker.mem = mem

    broker.attributes = attributes
    broker.options = options
    broker
  }

  def fromJson(node: Map[String, Object]): Unit = {
    id = node("id").asInstanceOf[String]
    started = node("started").asInstanceOf[Boolean]

    if (node.contains("host")) host = node("host").asInstanceOf[String]
    cpus = node("cpus").asInstanceOf[Number].doubleValue()
    mem = node("mem").asInstanceOf[Number].longValue()

    if (node.contains("attributes")) attributes = node("attributes").asInstanceOf[String]
    if (node.contains("options")) options = node("options").asInstanceOf[String]

    if (node.contains("task")) {
      task = new Broker.Task()
      task.fromJson(node("task").asInstanceOf[Map[String, Object]])
    }
  }

  def toJson: JSONObject = {
    val obj = new collection.mutable.LinkedHashMap[String, Any]()
    obj("id") = id
    obj("started") = started

    if (host != null) obj("host") = host
    obj("cpus") = cpus
    obj("mem") = mem

    if (attributes != null) obj("attributes") = attributes
    if (options != null) obj("options") = options

    if (task != null) obj("task") = task.toJson

    new JSONObject(obj.toMap)
  }

  def canAccept(offer: Offer): Boolean = {
    if (host != null && offer.getHostname != host) return false

    for (resource <- offer.getResourcesList) {
      resource.getName match {
        case "cpus" => if (resource.getScalar.getValue < cpus) return false
        case "mem" => if (resource.getScalar.getValue < mem) return false
        case _ => // ignore
      }
    }

    val offerAttributes = new util.HashMap[String, String]()
    for (attribute <- offer.getAttributesList) {
      var value: String = null
      if (attribute.hasText) value = attribute.getText.getValue
      if (attribute.hasScalar) value = "" + attribute.getScalar.getValue

      if (value != null) offerAttributes.put(attribute.getName, value)
    }

    for ((name, value) <- attributeMap) {
      if (!offerAttributes.containsKey(name)) return false
      if (offerAttributes.get(name) != value) return false
    }

    true
  }

  def waitForState(running: Boolean, timeout: java.lang.Long): Boolean = {
    def stateMatches: Boolean = if (running) task != null && task.running else task == null

    var t = timeout
    while (t > 0 && !stateMatches) {
      t -= 200
      if (t > 0) Thread.sleep(200)
    }

    stateMatches
  }
}

object Broker {
  def parseMap(s: String, entrySep: String, valueSep: String): util.LinkedHashMap[String, String] = {
    val result = new util.LinkedHashMap[String, String]()
    if (s == null) return result

    for (entry <- s.split(entrySep))
      if (entry.trim() != "") {
        val pair = entry.split(valueSep)
        if (pair.length == 2) result.put(pair(0).trim(), pair(1).trim())
        else throw new IllegalArgumentException(s)
      }

    result
  }

  class Task(_id: String = null, _host: String = null, _port: Int = -1) {
    var id: String = _id
    @volatile var running: Boolean = false
    var host: String = _host
    var port: Int = _port

    def endpoint: String = host + ":" + port

    def fromJson(node: Map[String, Object]): Unit = {
      id = node("id").asInstanceOf[String]
      running = node("running").asInstanceOf[Boolean]
      host = node("host").asInstanceOf[String]
      port = node("port").asInstanceOf[Number].intValue()
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("id") = id
      obj("running") = running
      obj("host") = host
      obj("port") = port

      new JSONObject(obj.toMap)
    }
  }
}