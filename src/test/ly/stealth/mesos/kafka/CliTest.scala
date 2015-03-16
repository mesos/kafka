package ly.stealth.mesos.kafka

import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import scala.collection.JavaConversions._
import java.io.{ByteArrayOutputStream, PrintStream}
import Util.Period

class CliTest extends MesosTestCase {
  var out: ByteArrayOutputStream = null
  var err: ByteArrayOutputStream = null

  @Before
  override def before {
    super.before

    Config.schedulerUrl = "http://localhost:7000"
    HttpServer.start(resolveDeps = false)

    out = new ByteArrayOutputStream()
    err = new ByteArrayOutputStream()
    Cli.out = new PrintStream(out, true)
    Cli.err = new PrintStream(err, true)
  }

  @After
  override def after {
    Cli.out = System.out
    Cli.err = System.err

    HttpServer.stop()
    super.after
  }

  @Test
  def status {
    Scheduler.cluster.addBroker(new Broker("0"))
    Scheduler.cluster.addBroker(new Broker("1"))
    Scheduler.cluster.addBroker(new Broker("2"))

    exec("status")
    assertTrue("" + out, out.toString.contains("status received"))
    assertTrue("" + out, out.toString.contains("id: 0"))
    assertTrue("" + out, out.toString.contains("id: 1"))
    assertTrue("" + out, out.toString.contains("id: 2"))
  }

  @Test
  def add {
    exec("add 0 --cpus=0.1 --mem=128")
    assertTrue("" + out, out.toString.contains("Broker added"))
    assertTrue("" + out, out.toString.contains("id: 0"))
    assertTrue("" + out, out.toString.contains("cpus:0.10, mem:128"))

    assertEquals(1, Scheduler.cluster.getBrokers.size())
    val broker = Scheduler.cluster.getBroker("0")
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)
  }

  @Test
  def update {
    val broker = Scheduler.cluster.addBroker(new Broker("0"))

    exec("update 0 --failoverDelay=10s --failoverMaxDelay=20s --options=log.dirs=/tmp/kafka-logs")
    assertTrue("" + out, out.toString.contains("Broker updated"))
    assertTrue("" + out, out.toString.contains("delay:10s, maxDelay:20s"))
    assertTrue("" + out, out.toString.contains("options: log.dirs=/tmp/kafka-logs"))

    assertEquals(new Period("10s"), broker.failover.delay)
    assertEquals(new Period("20s"), broker.failover.maxDelay)
    assertEquals("log.dirs=/tmp/kafka-logs", broker.options)
  }

  @Test
  def remove {
    Scheduler.cluster.addBroker(new Broker("0"))
    exec("remove 0")

    assertTrue("" + out, out.toString.contains("Broker 0 removed"))
    assertNull(Scheduler.cluster.getBroker("0"))
  }

  def exec(cmd: String) {
    val args = new util.ArrayList[String]()
    args.addAll(cmd.split(" ").toList)
    Cli.exec(args.toArray(new Array[String](args.length)))
  }
}
