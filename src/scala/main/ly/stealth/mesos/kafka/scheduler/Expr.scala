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

import ly.stealth.mesos.kafka.{Broker, Cluster}
import net.elodina.mesos.util.Strings
import scala.collection.JavaConversions._

object Expr {
  object any {
    def unapply(arg: String): Option[String] = if (arg.equals("*")) Some(arg) else None
  }
  object wildcard {
    def unapply(arg: String): Option[String] =
      if (arg.endsWith("*"))
        Some(arg.substring(0, arg.length - 1))
      else None
  }
  object range {
    def unapply(part: String): Option[(Int, Int)] =
      part.indexOf("..") match {
        case v if v == -1 => None
        case v if v == 0 => throw new IllegalArgumentException("Invalid expr " + part)
        case idx => Some(
          try {
            (part.substring(0, idx).toInt, part.substring(idx + 2, part.length).toInt)
          } catch {
            case _: NumberFormatException => throw new IllegalArgumentException("Invalid expr " + part)
          })
      }
  }
  object parsable {
    def unapply(arg: String): Option[Int] = try {
      Some(arg.toInt)
    } catch {
      case _: NumberFormatException => None
    }
  }

  private def parseAttrs(expr: String): (String, Option[Map[String, Option[String]]]) = {
    if (expr.endsWith("]")) {
      val idx = expr.lastIndexOf("[")
      if (idx == -1)
        throw new IllegalArgumentException("Invalid expr " + expr)

      (expr.substring(0, idx),
        Some(Strings.parseMap(expr.substring(idx + 1, expr.length - 1), true)
          .toMap.mapValues(Option.apply)))
    } else {
      (expr, None)
    }
  }

  def expandBrokers(cluster: Cluster, _expr: String, sortByAttrs: Boolean = false): Seq[Int] = {
    val (expr, attributes) = parseAttrs(_expr)
    val ids = expr.split(",").map(_.trim).flatMap {
      case wildcard(i) if i.isEmpty => cluster.getBrokers.map(_.id)
      case range((start, end)) => start until end + 1
      case parsable(i) => Seq(i)
      case _ => throw new IllegalArgumentException("Invalid expr " + expr)
    }.distinct

    attributes
      .map(attrs => filterAndSortBrokersByAttrs(cluster, ids, attrs, sortByAttrs))
      .getOrElse(ids.sorted)
  }

  private def brokerAttr(broker: Broker, name: String): Option[String] = {
    Option(broker)
      .flatMap(b => Option(b.task))
      .flatMap { t =>
        name match {
          case "hostname" => Some(t.hostname)
          case attr => t.attributes.get(attr)
        }
      }
  }

  private def filterAndSortBrokersByAttrs(
    cluster: Cluster,
    ids: Iterable[Int],
    attributes: Map[String, Option[String]],
    sortByAttrs: Boolean
  ): Seq[Int] = {
    val filtered = ids.map(id => cluster.getBroker(id)).filter(_ != null).filter { broker =>
      attributes.forall { case (key, expected) =>
        (brokerAttr(broker, key), expected) match {
          case (Some(a), Some(e)) if e.endsWith("*") => a.startsWith(e.substring(0, e.length - 1))
          case (Some(a), Some(e)) => a == e
          case (Some(_), None) => true
          case _ => false
        }
      }
    }
    if (sortByAttrs)
      sortBrokers(cluster, attributes.keys, filtered)
    else
      filtered.map(_.id).toSeq
  }

  // Group passed in brokers by attribute, and then round robin across the groups.
  // All brokers in the broker list must have all attributes in the passed attribute list.
  private def sortBrokers(
    cluster: Cluster,
    attributes: Iterable[String],
    brokers: Iterable[Broker]
  ): Seq[Int] = {
    brokers
      .groupBy(b => attributes
        .map(a => a -> brokerAttr(b, a).get)) // Group by unique attribute (k,v) tuples
      .toSeq
      .sortBy(_._1) // Sort them (this will be by the key and value combo)
      .flatMap(_._2.zipWithIndex) // Add an index to each element, and merge all lists together
      .sortBy(_._2) // Sort by the index added from above
      .map(_._1.id) // Pick the id off of the broker
  }

  def expandTopics(expr: String): Seq[String] = {
    val allTopics = ZkUtilsWrapper().getAllTopics()

    expr.split(",").map(_.trim).filter(!_.isEmpty).flatMap {
      case wildcard(part) => allTopics.filter(topic => topic.startsWith(part))
      case p => Seq(p)
    }
  }
}
