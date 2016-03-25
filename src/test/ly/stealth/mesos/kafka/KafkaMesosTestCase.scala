package ly.stealth.mesos.kafka

import java.io.{FileWriter, File}
import org.I0Itec.zkclient.{ZkClient, IDefaultNameSpace, ZkServer}
import org.apache.log4j.BasicConfigurator
import ly.stealth.mesos.kafka.Cluster.FsStorage
import net.elodina.mesos.util.{IO, Net, Version}
import org.junit.{Ignore, Before, After}
import scala.concurrent.duration.Duration

@Ignore
class KafkaMesosTestCase extends net.elodina.mesos.test.MesosTestCase {
  var zkDir: File = null
  var zkServer: ZkServer = null

  @Before
  def before {
    BasicConfigurator.configure()

    val storageFile = File.createTempFile(getClass.getSimpleName, null)
    storageFile.delete()
    Cluster.storage = new FsStorage(storageFile)

    Config.api = "http://localhost:7000"
    Config.zk = "localhost"

    Scheduler.cluster.clear()
    Scheduler.cluster.rebalancer = new TestRebalancer()
    Scheduler.reconciles = 0
    Scheduler.reconcileTime = null

    Scheduler.registered(schedulerDriver, frameworkId(), master())
    Executor.server = new TestBrokerServer()

    def createTempFile(name: String, content: String): File = {
      val file = File.createTempFile(getClass.getSimpleName, name)
      IO.writeFile(file, content)

      file.deleteOnExit()
      file
    }

    HttpServer.jar = createTempFile("executor.jar", "executor")
    HttpServer.kafkaDist = createTempFile("kafka-0.9.3.0.tgz", "kafka")
    HttpServer.kafkaVersion = new Version("0.9.3.0")
  }

  @After
  def after {
    Scheduler.disconnected(schedulerDriver)

    Scheduler.cluster.rebalancer = new Rebalancer()

    val storage = Cluster.storage.asInstanceOf[FsStorage]
    storage.file.delete()
    Cluster.storage = new FsStorage(FsStorage.DEFAULT_FILE)

    Executor.server.stop()
    Executor.server = new KafkaServer()
    BasicConfigurator.resetConfiguration()
  }

  def startZkServer() {
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
    HttpServer.initLogging()
    Config.api = "http://localhost:0"
    HttpServer.start(resolveDeps = false)
  }

  def stopHttpServer() {
    HttpServer.stop()
  }

  def delay(duration: String = "100ms")(f: => Unit) = new Thread {
    override def run(): Unit = {
      Thread.sleep(Duration(duration).toMillis)
      f
    }
  }.start()
}
