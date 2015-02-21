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

import org.apache.mesos.Protos._
import java.util
import org.apache.mesos.Protos
import scala.collection.JavaConversions._

object MesosStr {
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
      s += (if (s.isEmpty) "" else "\n") + MesosStr.offer(offer)

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
