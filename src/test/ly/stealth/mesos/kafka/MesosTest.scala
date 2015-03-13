package ly.stealth.mesos.kafka

import org.apache.mesos.Protos._
import java.util.UUID
import org.apache.mesos.Protos.Value.{Text, Scalar}
import scala.collection.JavaConversions._
import org.apache.mesos.SchedulerDriver
import java.util
import org.junit.{After, Before}
import org.apache.log4j.BasicConfigurator
import java.io.File

class MesosTest {
  var driver: TestSchedulerDriver = null

  @Before
  def before {
    BasicConfigurator.configure()

    Cluster.stateFile = File.createTempFile(getClass.getSimpleName, null)
    Cluster.stateFile.delete()
    Scheduler.cluster.clear()

    driver = schedulerDriver
    Scheduler.registered(driver, frameworkId(), master())
  }

  @After
  def after {
    Scheduler.disconnected(driver)

    Cluster.stateFile.delete()
    Cluster.stateFile = Cluster.DEFAULT_STATE_FILE

    BasicConfigurator.resetConfiguration()
  }

  val LOCALHOST_IP: Int = 2130706433
  
  def frameworkId(id: String = "" + UUID.randomUUID()): FrameworkID = FrameworkID.newBuilder().setValue(id).build()

  def master(
    id: String = "" + UUID.randomUUID(),
    ip: Int = LOCALHOST_IP,
    port: Int = 5050,
    host: String = "master"
  ): MasterInfo = {
    MasterInfo.newBuilder()
    .setId(id)
    .setIp(ip)
    .setPort(port)
    .setHostname(host)
    .build()
  }

  def offer(
    id: String = "" + UUID.randomUUID(),
    frameworkId: String = "" + UUID.randomUUID(),
    slaveId: String = "" + UUID.randomUUID(),
    host: String = "host",
    cpus: Double = 0,
    mem: Long = 0,
    ports: Pair[Int, Int] = null,
    attributes: String = null
  ): Offer = {
    val builder = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue(id))
      .setFrameworkId(FrameworkID.newBuilder().setValue(frameworkId))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))

    builder.setHostname(host)

    val cpusResource = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpus))
      .build
    builder.addResources(cpusResource)

    val memResource = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(0.0 + mem))
      .build
    builder.addResources(memResource)
    
    if (ports != null) {
      val portsRange = Value.Range.newBuilder().setBegin(ports._1).setEnd(ports._2)

      val portsResource = Resource.newBuilder()
      .setName("ports")
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(portsRange))
      .build

      builder.addResources(portsResource)
    }

    if (attributes != null) {
      val map = Util.parseMap(attributes, ";", ":")
      for ((k, v) <- map) {
        val attribute = Attribute.newBuilder()
          .setType(Value.Type.TEXT)
          .setName(k)
          .setText(Text.newBuilder().setValue(v))
          .build
        builder.addAttributes(attribute)
      }
    }

    builder.build()
  }

  def taskStatus(
    id: String = "" + UUID.randomUUID(),
    state: TaskState
  ): TaskStatus = {
    TaskStatus.newBuilder()
    .setTaskId(TaskID.newBuilder().setValue(id))
    .setState(state)
    .build()
  }
  
  def schedulerDriver: TestSchedulerDriver = new TestSchedulerDriver()

  class TestSchedulerDriver extends SchedulerDriver {
    var status: Status = Status.DRIVER_RUNNING

    var declinedOffers: util.List[String] = new util.ArrayList[String]()
    var acceptedOffers: util.List[String] = new util.ArrayList[String]()
    
    var launchedTasks: util.List[TaskInfo] = new util.ArrayList[TaskInfo]()
    var killedTasks: util.List[String] = new util.ArrayList[String]()
    
    def declineOffer(id: OfferID): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def declineOffer(id: OfferID, filters: Filters): Status = {
      declinedOffers.add(id.getValue)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo]): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerId: OfferID, tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo]): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def launchTasks(offerIds: util.Collection[OfferID], tasks: util.Collection[TaskInfo], filters: Filters): Status = {
      for (offerId <- offerIds) acceptedOffers.add(offerId.getValue)
      launchedTasks.addAll(tasks)
      status
    }

    def stop(): Status = throw new UnsupportedOperationException

    def stop(failover: Boolean): Status = throw new UnsupportedOperationException

    def killTask(id: TaskID): Status = {
      killedTasks.add(id.getValue)
      status
    }

    def requestResources(requests: util.Collection[Request]): Status = throw new UnsupportedOperationException

    def sendFrameworkMessage(executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Status = throw new UnsupportedOperationException

    def join(): Status = throw new UnsupportedOperationException

    def reconcileTasks(statuses: util.Collection[TaskStatus]): Status = throw new UnsupportedOperationException

    def reviveOffers(): Status = throw new UnsupportedOperationException

    def run(): Status = throw new UnsupportedOperationException

    def abort(): Status = throw new UnsupportedOperationException

    def start(): Status = throw new UnsupportedOperationException
  }
}
