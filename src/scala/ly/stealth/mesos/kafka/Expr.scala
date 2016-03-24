package ly.stealth.mesos.kafka

import java.util
import scala.collection.JavaConversions._
import java.io.PrintStream
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import ly.stealth.mesos.kafka.Broker.Task
import java.lang.Comparable
import net.elodina.mesos.util.Strings

object Expr {
  def expandBrokers(cluster: Cluster, _expr: String, sortByAttrs: Boolean = false): util.List[String] = {
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
      if (name != "hostname") task.attributes.get(name) else task.hostname
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

  def printBrokerExprExamples(out: PrintStream): Unit = {
    out.println("broker-expr examples:")
    out.println("  0      - broker 0")
    out.println("  0,1    - brokers 0,1")
    out.println("  0..2   - brokers 0,1,2")
    out.println("  0,1..2 - brokers 0,1,2")
    out.println("  *      - any broker")
    out.println("attribute filtering:")
    out.println("  *[rack=r1]           - any broker having rack=r1")
    out.println("  *[hostname=slave*]   - any broker on host with name starting with 'slave'")
    out.println("  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1")
  }

  def expandTopics(expr: String): util.List[String] = {
    val topics = new util.TreeSet[String]()

    val zkClient = newZkClient
    var allTopics: util.List[String] = null
    try { allTopics = ZkUtils.getAllTopics(zkClient) }
    finally { zkClient.close() }

    for (part <- expr.split(",").map(_.trim).filter(!_.isEmpty)) {
      if (!part.endsWith("*")) topics.add(part)
      else
        for (topic <- allTopics)
          if (topic.startsWith(part.substring(0, part.length - 1)))
            topics.add(topic)
    }

    topics.toList
  }

  def printTopicExprExamples(out: PrintStream): Unit = {
    out.println("topic-expr examples:")
    out.println("  t0        - topic t0")
    out.println("  t0,t1     - topics t0, t1")
    out.println("  *         - any topic")
    out.println("  t*        - topics starting with 't'")
  }

  private def newZkClient: ZkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)
}
