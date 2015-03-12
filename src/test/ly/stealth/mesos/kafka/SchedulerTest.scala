package ly.stealth.mesos.kafka

import java.util
import scala.collection.JavaConversions._
import org.junit.{After, Before, Test}
import org.junit.Assert._
import ly.stealth.mesos.kafka.Mesos._
import org.apache.mesos.Protos.Resource
import java.util.Properties
import java.io.StringReader
import org.apache.log4j.BasicConfigurator

class SchedulerTest {
  var driver: TestSchedulerDriver = null

  @Before
  def before {
    BasicConfigurator.configure()
    Scheduler.getCluster.clear()

    driver = schedulerDriver
    Scheduler.registered(driver, frameworkId(), master())
  }

  @After
  def after {
    Scheduler.disconnected(driver)
    BasicConfigurator.resetConfiguration()
  }

  @Test
  def newExecutor {
    val broker = new Broker("1")
    broker.heap = 512

    val executor = Scheduler.newExecutor(broker)
    val command = executor.getCommand
    assertTrue(command.getUrisCount > 0)

    val cmd: String = command.getValue
    assertTrue(cmd, cmd.contains("-Xmx" + broker.heap + "m"))
    assertTrue(cmd, cmd.contains(Executor.getClass.getName.replace("$", "")))
  }

  @Test
  def newTask {
    val broker = new Broker("1")
    broker.options = "a=1"
    broker.cpus = 0.5
    broker.mem = 256

    val offer = Mesos.offer(
      slaveId = "slave",
      ports = Pair(1000, 1000)
    )

    val task = Scheduler.newTask(broker, offer)
    assertEquals("slave", task.getSlaveId.getValue)
    assertNotNull(task.getExecutor)

    // resources
    val resources = new util.HashMap[String, Resource]()
    for (resource <- task.getResourcesList) resources.put(resource.getName, resource)

    val cpuResource = resources.get("cpus")
    assertNotNull(cpuResource)
    assertEquals(broker.cpus, cpuResource.getScalar.getValue, 0.001)

    val memResource = resources.get("mem")
    assertNotNull(memResource)
    assertEquals(broker.mem, memResource.getScalar.getValue.toInt)

    val portsResource = resources.get("ports")
    assertNotNull(portsResource)
    assertEquals(1, portsResource.getRanges.getRangeCount)

    val range = portsResource.getRanges.getRangeList.get(0)
    assertEquals(1000, range.getBegin)
    assertEquals(1000, range.getEnd)

    // options
    val options = new Properties()
    options.load(new StringReader(task.getData.toStringUtf8))
    assertEquals(broker.id, options.getProperty("broker.id"))
    assertEquals("" + 1000, options.getProperty("port"))

    assertEquals("kafka-logs", options.getProperty("log.dirs"))
    assertEquals("1", options.getProperty("a"))
  }

  @Test
  def syncBrokers {
    val broker = Scheduler.getCluster.addBroker(new Broker())
    val offer = Mesos.offer(cpus = broker.cpus, mem = broker.mem, ports = Pair(100, 100))

    // broker !active
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(0, driver.launchedTasks.size())

    // broker active
    broker.active = true
    Scheduler.syncBrokers(util.Arrays.asList(offer))
    assertEquals(1, driver.launchedTasks.size())
    assertEquals(0, driver.killedTasks.size())

    // broker !active
    broker.active = false
    Scheduler.syncBrokers(util.Arrays.asList())
    assertEquals(1, driver.launchedTasks.size())
    assertEquals(1, driver.killedTasks.size())
  }

  @Test
  def findBrokerPort {
    assertEquals(100, Scheduler.findBrokerPort(offer(ports = Pair(100, 200))))
    assertEquals(100, Scheduler.findBrokerPort(offer(ports = Pair(100, 100))))

    // no ports
    try { Scheduler.findBrokerPort(offer()); fail() }
    catch { case e: IllegalArgumentException => }
  }
}
