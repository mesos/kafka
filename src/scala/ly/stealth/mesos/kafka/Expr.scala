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

  def expandTopics(expr: String): util.Map[String, Integer] = {
    val zkClient = newZkClient
    try {
      val assignment: mutable.Map[TopicAndPartition, Seq[Int]] = ZkUtils.getReplicaAssignmentForTopics(zkClient, ZkUtils.getAllTopics(zkClient))
      val replicas: util.Map[String, Int] = assignment.map(e => (e._1.topic, e._2.size))

      val topics: util.Map[String, Integer] = new util.LinkedHashMap()
      for ((topic, rf) <- Util.parseMap(expr, ',', ':', nullValues = true))
        topics.put(topic, if (rf != null) Integer.parseInt(rf) else null)

      // expand wildcard
      if (topics.containsKey("*")) {
        for (topic <- replicas.keys.toList.sorted)
          if (!topics.containsKey(topic)) topics.put(topic, null)

        val wildcardRf: Integer = topics.remove("*")
        if (wildcardRf != null)
          for ((topic, rf) <- topics)
            if (rf == null) topics.put(topic, wildcardRf)
      }

      // remove not existent topics
      topics.keySet().retainAll(replicas.keySet())

      // set default rf if needed
      for ((topic, rf) <- topics)
        if (rf == null) topics.put(topic, replicas.get(topic))

      topics
    } finally {
      zkClient.close()
    }
  }

  def printTopicExprExamples(out: PrintStream): Unit = {
    out.println("topic-expr examples:")
    out.println("  t0        - topic t0 with default RF (replication-factor)")
    out.println("  t0,t1     - topics t0, t1 with default RF")
    out.println("  t0:3      - topic t0 with RF=3")
    out.println("  t0,t1:2   - topic t0 with default RF, topic t1 with RF=2")
    out.println("  *         - all topics with default RF")
    out.println("  *:2       - all topics with RF=2")
    out.println("  t0:1,*:2  - all topics with RF=2 except topic t0 with RF=1")
  }


  private def newZkClient: ZkClient = new ZkClient(Config.zk, 30000, 30000, ZKStringSerializer)
}
