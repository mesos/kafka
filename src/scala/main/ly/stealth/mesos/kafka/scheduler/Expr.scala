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
package ly.stealth.mesos.kafka.scheduler

import java.util
import ly.stealth.mesos.kafka.Broker.Task
import ly.stealth.mesos.kafka.{Broker, Cluster}
import net.elodina.mesos.util.Strings
import scala.collection.JavaConversions._

object Expr {
  def expandBrokers(cluster: Cluster, _expr: String, sortByAttrs: Boolean = false): Seq[String] = {
    var expr: String = _expr
    var attributes: util.Map[String, String] = null
    
    if (expr.endsWith("]")) {
      val filterIdx = expr.lastIndexOf("[")
      if (filterIdx == -1) throw new IllegalArgumentException("Invalid expr " + expr)
      
      attributes = Strings.parseMap(expr.substring(filterIdx + 1, expr.length - 1), true)
      expr = expr.substring(0, filterIdx)
    }

    var ids: util.List[String] = new util.ArrayList[String]()

    for (_part <- expr.split(",")) {
      val part = _part.trim()

      if (part.equals("*"))
        for (broker <- cluster.getBrokers) ids.add(broker.id)
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

    ids = new util.ArrayList[String](ids.distinct.sorted.toList)

    if (attributes != null) 
      filterAndSortBrokersByAttrs(cluster, ids, attributes, sortByAttrs)

    ids
  }
  
  private def filterAndSortBrokersByAttrs(cluster: Cluster, ids: util.Collection[String], attributes: util.Map[String, String], sortByAttrs: Boolean): Unit = {
    def brokerAttr(broker: Broker, name: String): String = {
      if (broker == null || broker.task == null) return null

      val task: Task = broker.task
      if (name != "hostname") task.attributes.get(name).orNull else task.hostname
    }

    def brokerMatches(broker: Broker): Boolean = {
      if (broker == null) return false

      for (e <- attributes.entrySet()) {
        val expected = e.getValue
        val actual = brokerAttr(broker, e.getKey)
        if (actual == null) return false

        if (expected != null) {
          if (expected.endsWith("*") && !actual.startsWith(expected.substring(0, expected.length - 1)))
            return false

          if (!expected.endsWith("*") && actual != expected)
            return false
        }
      }

      true
    }

     def filterBrokers(): Unit = {
      val iterator = ids.iterator()
      while (iterator.hasNext) {
        val id = iterator.next()
        val broker = cluster.getBroker(id)

        if (!brokerMatches(broker))
          iterator.remove()
      }
    }

    def sortBrokers(): Unit = {
      class Value(broker: Broker) extends Comparable[Value] {
        def compareTo(v: Value): Int = toString.compareTo(v.toString)

        override def hashCode(): Int = toString.hashCode

        override def equals(obj: scala.Any): Boolean = obj.isInstanceOf[Value] && toString == obj.toString

        override def toString: String = {
          val values = new util.LinkedHashMap[String, String]()

          for (k <- attributes.keySet()) {
            val value: String = brokerAttr(broker, k)
            values.put(k, value)
          }

          Strings.formatMap(values)
        }
      }

      val values = new util.HashMap[Value, util.List[String]]()
      for (id <- ids) {
        val broker: Broker = cluster.getBroker(id)

        if (broker != null) {
          val value = new Value(broker)
          if (!values.containsKey(value)) values.put(value, new util.ArrayList[String]())
          values.get(value).add(id)
        }
      }

      val t = new util.ArrayList[String]()
      while (!values.isEmpty) {
        for (value <- new util.ArrayList[Value](values.keySet()).sorted) {
          val ids = values.get(value)

          val id: String = ids.remove(0)
          t.add(id)

          if (ids.isEmpty) values.remove(value)
        }
      }

      ids.clear()
      ids.addAll(t)
    }
    
    filterBrokers()
    if (sortByAttrs) sortBrokers()
  }

  def expandTopics(expr: String): Seq[String] = {
    val allTopics = ZkUtilsWrapper().getAllTopics()

    expr.split(",").map(_.trim).filter(!_.isEmpty).flatMap(part =>
      if (!part.endsWith("*"))
        Seq(part)
      else
        allTopics.filter(topic => topic.startsWith(part.substring(0, part.length - 1)))
    )
  }
}
