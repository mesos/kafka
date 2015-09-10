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

    topics.addTopic("t1", Map(1 -> util.Arrays.asList(0)))
    topics.addTopic("t2", Map(1 -> util.Arrays.asList(0, 1)))
    topics.addTopic("t3", Map(1 -> util.Arrays.asList(0, 1, 2)))

    // topic lists
    assertEquals("t1=1,t2=2,t3=3", Util.formatMap(Expr.expandTopics("t1,t2,t3")))
    assertEquals("t1=1,t3=3", Util.formatMap(Expr.expandTopics("t1,t3")))
    assertEquals("t1=1,t2=2,t3=3", Util.formatMap(Expr.expandTopics("t0,t1,t2,t3,t4")))

    // topic lists with rf
    assertEquals("t1=1,t3=1", Util.formatMap(Expr.expandTopics("t1,t3:1")))

    // wildcard
    assertEquals("t1=1,t2=2,t3=3", Util.formatMap(Expr.expandTopics("*")))
    assertEquals("t1=1,t2=2,t3=3", Util.formatMap(Expr.expandTopics("t1,t2,*")))

    // wildcard with rf
    assertEquals("t1=3,t2=3,t3=3", Util.formatMap(Expr.expandTopics("*:3")))
    assertEquals("t1=3,t2=3,t3=3", Util.formatMap(Expr.expandTopics("t1,*:3")))
    assertEquals("t1=1,t2=3,t3=3", Util.formatMap(Expr.expandTopics("t1:1,*:3")))
  }
}
