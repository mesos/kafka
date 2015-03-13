package ly.stealth.mesos.kafka

import org.junit.{Before, Test}
import java.util
import org.junit.Assert._

class ClusterTest extends MesosTest {
  var cluster: Cluster = new Cluster()

  @Before
  override def before {
    super.before
    cluster.clear()
  }

  @Test
  def addBroker_removeBroker_getBrokers {
    assertTrue(cluster.getBrokers.isEmpty)

    val broker0 = cluster.addBroker(new Broker("0"))
    val broker1 = cluster.addBroker(new Broker("1"))
    assertEquals(util.Arrays.asList(broker0, broker1), cluster.getBrokers)

    cluster.removeBroker(broker0)
    assertEquals(util.Arrays.asList(broker1), cluster.getBrokers)

    cluster.removeBroker(broker1)
    assertTrue(cluster.getBrokers.isEmpty)
  }

  @Test
  def getBroker {
    assertNull(cluster.getBroker("0"))

    val broker0 = cluster.addBroker(new Broker("0"))
    assertSame(broker0, cluster.getBroker("0"))
  }

  @Test
  def save_load {
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.save()

    val read = new Cluster()
    read.load(clearTasks = false)
    assertEquals(2, read.getBrokers.size())
  }

  @Test
  def toJson_fromJson {
    val broker0 = cluster.addBroker(new Broker("0"))
    broker0.task = new Broker.Task("1", "host", 9092)
    broker0.task.running = true
    cluster.addBroker(new Broker("1"))

    val read = new Cluster()
    read.fromJson(Util.parseJson("" + cluster.toJson))

    assertEquals(2, read.getBrokers.size())
    BrokerTest.assertBrokerEquals(broker0, read.getBroker("0"))
  }

  @Test
  def expandIds {
    for (i <- 0 until 5)
      cluster.addBroker(new Broker("" + i))

    try {
      assertEquals(util.Arrays.asList(), cluster.expandIds(""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(util.Arrays.asList("0"), cluster.expandIds("0"))
    assertEquals(util.Arrays.asList("0", "2", "4"), cluster.expandIds("0,2,4"))

    assertEquals(util.Arrays.asList("1", "2", "3"), cluster.expandIds("1..3"))
    assertEquals(util.Arrays.asList("0", "1", "3", "4"), cluster.expandIds("0..1,3..4"))

    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), cluster.expandIds("*"))

    // duplicates
    assertEquals(util.Arrays.asList("0", "1", "2", "3", "4"), cluster.expandIds("0..3,2..4"))

    // sorting
    assertEquals(util.Arrays.asList("2", "3", "4"), cluster.expandIds("4,3,2"))
  }
}
