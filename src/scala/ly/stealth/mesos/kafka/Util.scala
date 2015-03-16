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

import java.util.regex.Pattern
import java.util
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON
import java.io.{IOException, OutputStream, InputStream}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.mesos.Protos._
import org.apache.mesos.Protos

object Util {
  def parseMap(s: String, entrySep: String = ",", valueSep: String = "="): util.LinkedHashMap[String, String] = {
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

  def parseJson(json: String): Map[String, Object] = {
    val node: Map[String, Object] = JSON.parseFull(json).getOrElse(null).asInstanceOf[Map[String, Object]]
    if (node == null) throw new IllegalArgumentException("Failed to parse json: " + json)
    node
  }

  def copyAndClose(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](16 * 1024)
    var actuallyRead = 0

    try {
      while (actuallyRead != -1) {
        actuallyRead = in.read(buffer)
        if (actuallyRead != -1) out.write(buffer, 0, actuallyRead)
      }
    } finally {
      try { in.close() }
      catch { case ignore: IOException => }

      try { out.close() }
      catch { case ignore: IOException => }
    }
  }

  class Period(s: String) {
    private var _value: Long = 0
    private var _unit: String = null
    private var _ms: Long = 0

    parse()
    private def parse() {
      if (s.isEmpty) throw new IllegalArgumentException(s)

      var unitIdx = s.length - 1
      if (s.endsWith("ms")) unitIdx -= 1

      try { _value = java.lang.Long.valueOf(s.substring(0, unitIdx)) }
      catch { case e: IllegalArgumentException => throw new IllegalArgumentException(s) }
      _unit = s.substring(unitIdx)

      _ms = value
      if (_unit == "ms") _ms *= 1
      else if (_unit == "s") _ms *= 1000
      else if (_unit == "m") _ms *= 60 * 1000
      else if (_unit == "h") _ms *= 60 * 60 * 1000
      else if (_unit == "d") _ms *= 24 * 60 * 60 * 1000
      else throw new IllegalArgumentException(s)
    }

    def value: Long = _value
    def unit: String = _unit
    def ms: Long = _ms

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Period]) return false
      obj.asInstanceOf[Period]._ms == _ms
    }

    override def hashCode: Int = _ms.asInstanceOf[Int]
    override def toString: String = _value + _unit
  }


  class Wildcard(s: String) {
    private val _value: String = s
    private var _pattern: Pattern = null
    compilePattern()

    private def compilePattern() {
      var regex: String = "^"
      var token: String = ""

      for (c <- _value.toCharArray) {
        if (c == '*' || c == '?') {
          regex += Pattern.quote(token)
          token = ""
          regex += (if (c == '*') ".*" else ".")
        } else
          token += c
      }

      if (token != "") regex += Pattern.quote(token)
      regex += "$"

      _pattern = Pattern.compile(regex)
    }

    def matches(value: String): Boolean = _pattern.matcher(value).find()
    def value: String = _value

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Wildcard]) return false
      obj.asInstanceOf[Wildcard]._value == _value
    }

    override def hashCode: Int = _value.hashCode
    override def toString: String = _value
  }

  object Str {
    def dateTime(date: Date): String = {
      new SimpleDateFormat("yyyy-MM-dd hh:mm:ssX").format(date)
    }

    def framework(framework: FrameworkInfo): String = {
      var s = ""

      s += id(framework.getId.getValue)
      s += " name: " + framework.getName
      s += " host: " + framework.getHostname
      s += " failover_timeout: " + framework.getFailoverTimeout

      s
    }

    def master(master: MasterInfo): String = {
      var s = ""

      s += id(master.getId)
      s += " pid:" + master.getPid
      s += " host:" + master.getHostname

      s
    }

    def slave(slave: SlaveInfo): String = {
      var s = ""

      s += id(slave.getId.getValue)
      s += " host:" + slave.getHostname
      s += " port:" + slave.getPort
      s += " " + resources(slave.getResourcesList)

      s
    }

    def offer(offer: Offer): String = {
      var s = ""

      s += offer.getHostname + id(offer.getId.getValue)
      s += " " + resources(offer.getResourcesList)
      s += " " + attributes(offer.getAttributesList)

      s
    }

    def offers(offers: Iterable[Offer]): String = {
      var s = ""

      for (offer <- offers)
        s += (if (s.isEmpty) "" else "\n") + Str.offer(offer)

      s
    }

    def task(task: TaskInfo): String = {
      var s = ""

      s += task.getTaskId.getValue
      s += " slave:" + id(task.getSlaveId.getValue)

      s += " " + resources(task.getResourcesList)
      s += " data:" + new String(task.getData.toByteArray)

      s
    }

    def resources(resources: util.List[Protos.Resource]): String = {
      var s = ""

      val order: util.List[String] = "cpus mem disk ports".split(" ").toList
      for (resource <- resources.sortBy(r => order.indexOf(r.getName))) {
        if (!s.isEmpty) s += " "
        s += resource.getName + ":"

        if (resource.hasScalar)
          s += "%.2f".format(resource.getScalar.getValue)

        if (resource.hasRanges)
          for (range <- resource.getRanges.getRangeList)
            s += "[" + range.getBegin + ".." + range.getEnd + "]"
      }

      s
    }

    def attributes(attributes: util.List[Protos.Attribute]): String = {
      var s = ""

      for (attr <- attributes) {
        if (!s.isEmpty) s += ";"
        s += attr.getName + ":"

        if (attr.hasText) s += attr.getText.getValue
        if (attr.hasScalar) s +=  "%.2f".format(attr.getScalar.getValue)
      }

      s
    }

    def taskStatus(status: TaskStatus): String = {
      var s = ""
      s += status.getTaskId.getValue
      s += " " + status.getState.name()

      s += " slave:" + id(status.getSlaveId.getValue)

      if (status.getState != TaskState.TASK_RUNNING)
        s += " reason:" + status.getReason.name()

      if (status.getMessage != null && status.getMessage != "")
        s += " message:" + status.getMessage

      s
    }

    def id(id: String): String = "#" + suffix(id, 5)

    def suffix(s: String, maxLen: Int): String = {
      if (s.length <= maxLen) return s
      s.substring(s.length - maxLen)
    }
  }
}
