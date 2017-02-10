/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.mesos.kafka

import java.io.{File, FileWriter}
import org.I0Itec.zkclient.{IDefaultNameSpace, ZkClient, ZkServer}
import org.apache.log4j.{Appender, BasicConfigurator, ConsoleAppender, Level, Logger, PatternLayout}
import ly.stealth.mesos.kafka.Cluster.FsStorage
import net.elodina.mesos.util.{IO, Net, Period, Version}
import org.junit.{After, Before, Ignore}
import scala.concurrent.duration.Duration
import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import java.util
import java.util.Date
import ly.stealth.mesos.kafka.executor.{BrokerServer, Executor, KafkaServer, LaunchConfig}
import ly.stealth.mesos.kafka.scheduler._
import net.elodina.mesos.test.TestSchedulerDriver
import org.apache.mesos.Protos.{Status, TaskState}
import org.junit.Assert._

@Ignore
class KafkaMesosTestCase extends net.elodina.mesos.test.MesosTestCase {
  var zkDir: File = null
  var zkServer: ZkServer = null

  trait MockKafkaDistribution extends KafkaDistributionComponent {
    def createTempFile(name: String, content: String): File = {
      val file = File.createTempFile(getClass.getSimpleName, name)
      IO.writeFile(file, content)

      file.deleteOnExit()
      file
    }

    override val kafkaDistribution: KafkaDistribution = new KafkaDistribution {
      override val distInfo: KafkaDistributionInfo = KafkaDistributionInfo(
        jar = createTempFile("executor.jar", "executor"),
        kafkaDist = createTempFile("kafka-0.9.3.0.tgz", "kafka"),
        kafkaVersion = new Version("0.9.3.0")
      )
    }
  }

  object MockWallClock extends Clock {
    private[this] var mockDate: Option[Date] = None

    def now(): Date = mockDate.getOrElse(new Date())
    def overrideNow(now: Option[Date]): Unit = mockDate = now
  }

  trait MockWallClockComponent extends ClockComponent {
    override val clock: Clock = MockWallClock
  }

  schedulerDriver = new TestSchedulerDriver() {
    override def suppressOffers() = Status.DRIVER_RUNNING
    override def reviveOffers(): Status = Status.DRIVER_RUNNING
  }

  var registry: Registry = _

  def started(broker: Broker) {
    registry.scheduler.resourceOffers(schedulerDriver, Seq(offer("slave" + broker.id, "cpus:2.0;mem:2048;ports:9042..65000")))
    broker.waitFor(Broker.State.PENDING, new Period("1s"), 1)
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_STARTING, "slave" + broker.id + ":9042"))
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_RUNNING, "slave" + broker.id + ":9042"))
    broker.waitFor(Broker.State.RUNNING, new Period("1s"), 1)
    assertEquals(Broker.State.RUNNING, broker.task.state)
    assertTrue(broker.active)
  }

  def stopped(broker: Broker): Unit = {
    assertTrue(broker.waitFor(Broker.State.STOPPING, new Period("1s"), 1))
    registry.scheduler.statusUpdate(schedulerDriver, taskStatus(broker.task.id, TaskState.TASK_FINISHED))
    assertTrue(broker.waitFor(null, new Period("1s"), 1))
    assertNull(broker.task)
  }

  @Before
  def before {
    BasicConfigurator.resetConfiguration()
    val layout = new PatternLayout("%d %-5p %23c] %m%n")
    val appender: Appender = new ConsoleAppender(layout)

    Logger.getRootLogger.addAppender(appender)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.FATAL)
    Logger.getLogger("org.I0Itec.zkclient").setLevel(Level.FATAL)

    val storageFile = File.createTempFile(getClass.getSimpleName, null)
    storageFile.delete()
    Cluster.storage = new FsStorage(storageFile)

    Config.api = "http://localhost:7000"
    Config.zk = "localhost"

    MockWallClock.overrideNow(None)
    registry = new ProductionRegistry() with MockKafkaDistribution with MockWallClockComponent
    registry.cluster.clear()
    registry.cluster.rebalancer = new TestRebalancer()

    registry.scheduler.registered(schedulerDriver, frameworkId(), master())
    Executor.server = new TestBrokerServer()
  }

  @After
  def after {
    val storage = Cluster.storage.asInstanceOf[FsStorage]
    storage.file.delete()
    Cluster.storage = new FsStorage(FsStorage.DEFAULT_FILE)

    Executor.server.stop()
    Executor.server = new KafkaServer()
    BasicConfigurator.resetConfiguration()
    ZkUtilsWrapper.reset()
    AdminUtilsWrapper.reset()
  }

  def startZkServer(): ZkClient = {
    val port = Net.findAvailPort
    Config.zk = s"localhost:$port"

    zkDir = File.createTempFile(getClass.getName, null)
    zkDir.delete()

    val defaultNamespace = new IDefaultNameSpace { def createDefaultNameSpace(zkClient: ZkClient): Unit = {} }
    zkServer = new ZkServer("" + zkDir, "" + zkDir, defaultNamespace, port)
    zkServer.start()

    val zkClient: ZkClient = zkServer.getZkClient
    zkClient.createPersistent("/brokers/ids/0", true)
    zkClient.createPersistent("/config/changes", true)
    zkClient
  }

  def stopZkServer() {
    if (zkDir == null) return

    zkServer.shutdown()
    def delete(dir: File) {
      val children: Array[File] = dir.listFiles()
      if (children != null) children.foreach(delete)
      dir.delete()
    }
    delete(zkDir)

    zkDir = null
    zkServer = null
  }

  def startHttpServer() {
    registry.httpServer.initLogging()
    Config.api = "http://localhost:0"
    registry.httpServer.start()
  }

  def stopHttpServer() {
    registry.httpServer.stop()
  }

  def delay(duration: String = "100ms")(f: => Unit) = new Thread {
    override def run(): Unit = {
      Thread.sleep(Duration(duration).toMillis)
      f
    }
  }.start()
}

class TestBrokerServer extends BrokerServer {
  var failOnStart: Boolean = false
  private val started: AtomicBoolean = new AtomicBoolean(false)

  def isStarted: Boolean = started.get()

  def start(config: LaunchConfig, send: Broker.Metrics => Unit): Broker.Endpoint = {
    if (failOnStart) throw new RuntimeException("failOnStart")
    started.set(true)
    new Broker.Endpoint("localhost", 9092)
  }

  def stop(): Unit = {
    started.set(false)
    started.synchronized { started.notify() }
  }

  def waitFor(): Unit = {
    started.synchronized {
      while (started.get)
        started.wait()
    }
  }

  def getClassLoader: ClassLoader = getClass.getClassLoader
}

class TestRebalancer extends Rebalancer {
  var _running: Boolean = false
  var _failOnStart: Boolean = false

  override def running: Boolean = _running

  override def start(topics: Seq[String], brokers: Seq[Int], replicas: Int = -1, fixedStartIndex: Int = -1, startPartitionId: Int = -1, realignment: Boolean = false): Unit = {
    if (_failOnStart) throw new Rebalancer.Exception("failOnStart")
    _running = true
  }

  override def state: String = if (running) "running" else ""
}
