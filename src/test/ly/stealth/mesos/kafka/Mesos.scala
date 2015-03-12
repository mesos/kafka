package ly.stealth.mesos.kafka

import org.apache.mesos.Protos._
import java.util.UUID
import org.apache.mesos.Protos.Value.{Text, Scalar}
import scala.collection.JavaConversions._

object Mesos {
  def offer(
     id: String = "" + UUID.randomUUID(),
     frameworkId: String = "" + UUID.randomUUID(),
     slaveId: String = "" + UUID.randomUUID(),
     host: String = "host",
     cpus: Double = 0,
     mem: Int = 0,
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
}
