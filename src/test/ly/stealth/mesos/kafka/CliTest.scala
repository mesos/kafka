package ly.stealth.mesos.kafka

import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import scala.collection.JavaConversions._
import java.io.{ByteArrayOutputStream, PrintStream}

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

  def exec(cmd: String) {
    val args = new util.ArrayList[String]()
    args.addAll(cmd.split(" ").toList)
    Cli.exec(args.toArray(new Array[String](args.length)))
  }
}
