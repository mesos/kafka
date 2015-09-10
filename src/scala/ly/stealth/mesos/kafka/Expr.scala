package ly.stealth.mesos.kafka

import java.util
import scala.collection.JavaConversions._
import java.io.PrintStream
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import ly.stealth.mesos.kafka.Broker.Task

object Expr {
  def expandBrokers(cluster: Cluster, _expr: String): util.List[String] = {
    var expr: String = _expr
    var attributes: util.Map[String, String] = null
    
    if (expr.endsWith("]")) {
      val filterIdx = expr.lastIndexOf("[")
      if (filterIdx == -1) throw new IllegalArgumentException("Invalid expr " + expr)
      
      attributes = Util.parseMap(expr.substring(filterIdx + 1, expr.length - 1))
      expr = expr.substring(0, filterIdx)
    }
    
    val ids = new util.TreeSet[String]()

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
    
    if (attributes != null) {
      val iterator = ids.iterator()
      while (iterator.hasNext) {
        val id = iterator.next()
        val broker = cluster.getBroker(id)
        
        if (!brokerMatchesAttributes(broker, attributes))
          iterator.remove()
      }
    }

    new util.ArrayList(ids)
  }
  
  private def brokerMatchesAttributes(broker: Broker, attributes: util.Map[String, String]): Boolean = {
    if (broker == null || broker.task == null) return false
    val task: Task = broker.task

    for (e <- attributes.entrySet()) {
      val expected = e.getValue
      val actual = if (e.getKey != "hostname") task.attributes.get(e.getKey) else task.hostname
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

  def printBrokerExprExamples(out: PrintStream): Unit = {
    out.println("broker-expr examples:")
    out.println("  0        - broker 0")
    out.println("  0,1      - brokers 0,1")
    out.println("  0..2     - brokers 0,1,2")
    out.println("  0,1..2   - brokers 0,1,2")
    out.println("  *        - any broker")
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
