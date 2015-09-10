package ly.stealth.mesos.kafka

import org.junit.{After, Before, Test}
import org.junit.Assert._
import scala.collection.JavaConversions._
import java.util

class ExprTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    startZkServer()
  }

  @After
  override def after {
    super.after
    stopZkServer()
  }

  @Test
  def expandBrokers {
    val cluster = Scheduler.cluster

    for (i <- 0 until 5)
      cluster.addBroker(new Broker("" + i))

    try {
      assertEquals(util.Arrays.asList(), Expr.expandBrokers(cluster, ""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(util.Arrays.asList("0"), Expr.expandBrokers(cluster, "0"))
    assertEquals(util.Arrays.asList("0", "2", "4"), Expr.expandBrokers(cluster, "0,2,4"))

    assertEquals(util.Arrays.asList("1", "2", "3"), Expr.expandBrokers(cluster, "1..3"))
    assertEquals(util.Arrays.asList("0", "1", "3", "4"), Expr.expandBrokers(cluster, "0..1,3..4"))

    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), Expr.expandBrokers(cluster, "*"))

    // duplicates
    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), Expr.expandBrokers(cluster, "0..3,2..4"))

    // sorting
    assertEquals(util.Arrays.asList("2", "3", "4"), Expr.expandBrokers(cluster, "4,3,2"))
  }

  @Test
  def expandTopics {
    val cluster = Scheduler.cluster
    val topics: Topics = cluster.topics

    topics.addTopic("t0")
    topics.addTopic("t1")
    topics.addTopic("x")

    assertEquals(util.Arrays.asList(), Expr.expandTopics(""))
    assertEquals(util.Arrays.asList("t5", "t6"), Expr.expandTopics("t5,t6"))
    assertEquals(util.Arrays.asList("t0"), Expr.expandTopics("t0"))
    assertEquals(util.Arrays.asList("t0", "t1"), Expr.expandTopics("t0, t1"))
    assertEquals(util.Arrays.asList("t0", "t1", "x"), Expr.expandTopics("*"))
    assertEquals(util.Arrays.asList("t0", "t1"), Expr.expandTopics("t*"))
  }
}
