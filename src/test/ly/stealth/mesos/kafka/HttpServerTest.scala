package ly.stealth.mesos.kafka

import org.junit.{Test, After, Before}
import org.junit.Assert._
import CLI.sendRequest
import ly.stealth.mesos.kafka.Util.{Period, parseMap}

class HttpServerTest extends MesosTestCase {
  @Before
  override def before {
    super.before
    Config.schedulerUrl = "http://localhost:7000"
    HttpServer.start()
  }
  
  @After
  override def after {
    HttpServer.stop()
    super.after
  }
  
  @Test
  def brokers_add {
    val json = sendRequest("/brokers/add", parseMap("id=0,cpus=0.1,mem=128"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(1, brokerNodes.size)
    val responseBroker = new Broker()
    responseBroker.fromJson(brokerNodes(0))

    assertEquals(1, Scheduler.cluster.getBrokers.size())
    val broker = Scheduler.cluster.getBrokers.get(0)
    assertEquals("0", broker.id)
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)

    BrokerTest.assertBrokerEquals(broker, responseBroker)
  }

  @Test
  def brokers_add_range {
    val json = sendRequest("/brokers/add", parseMap("id=0..4"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(5, brokerNodes.size)
    assertEquals(5, Scheduler.cluster.getBrokers.size)
  }

  @Test
  def brokers_update {
    sendRequest("/brokers/add", parseMap("id=0"))
    val json = sendRequest("/brokers/update", parseMap("id=0,cpus=1,heap=128,failoverDelay=5s"))
    val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]

    assertEquals(1, brokerNodes.size)
    val responseBroker = new Broker()
    responseBroker.fromJson(brokerNodes(0))

    val broker = Scheduler.cluster.getBroker("0")
    assertEquals(1, broker.cpus, 0.001)
    assertEquals(128, broker.heap)
    assertEquals(new Period("5s"), broker.failover.delay)

    BrokerTest.assertBrokerEquals(broker, responseBroker)
  }
}
