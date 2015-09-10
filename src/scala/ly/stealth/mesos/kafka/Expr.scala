package ly.stealth.mesos.kafka

import java.util
import scala.collection.JavaConversions._
import java.io.PrintStream
import scala.collection.{Seq, mutable}
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient

object Expr {
  def expandBrokers(cluster: Cluster, expr: String): util.List[String] = {
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

    new util.ArrayList(ids)
  }

  def printBrokerExprExamples(out: PrintStream): Unit = {
    out.println("broker-expr examples:")
    out.println("  0      - broker 0")
    out.println("  0,1    - brokers 0,1")
    out.println("  0..2   - brokers 0,1,2")
    out.println("  0,1..2 - brokers 0,1,2")
    out.println("  *      - any broker")
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
