package ly.stealth.mesos.kafka

import java.io.File
import net.elodina.mesos.util.Version

case class KafkaDistributionInfo(jar: File, kafkaVersion: Version, kafkaDist: File)

trait KafkaDistributionComponent {
  val kafkaDistribution: KafkaDistribution

  trait KafkaDistribution {
    val distInfo: KafkaDistributionInfo
  }
}

trait KafkaDistributionComponentImpl extends KafkaDistributionComponent {
  val kafkaDistribution: KafkaDistribution = new KafkaDistributionImpl

  class KafkaDistributionImpl extends KafkaDistribution{
    lazy val distInfo: KafkaDistributionInfo = {
      var jar: File = null
      var kafkaDist: File = null
      var kafkaVersion: Version = null

      val jarMask: String = "kafka-mesos.*\\.jar"
      val kafkaMask: String = "kafka.*\\.tgz"

      for (file <- new File(".").listFiles()) {
        if (file.getName.matches(jarMask)) jar = file
        if (file.getName.matches(kafkaMask)) kafkaDist = file
      }

      if (jar == null) throw new IllegalStateException(jarMask + " not found in current dir")
      if (kafkaDist == null) throw new IllegalStateException(kafkaMask + " not found in in current dir")

      // extract version: "kafka-dist-1.2.3.tgz" => "1.2.3"
      val distName: String = kafkaDist.getName
      val tgzIdx = distName.lastIndexOf(".tgz")
      val hIdx = distName.lastIndexOf("-")
      if (tgzIdx == -1 || hIdx == -1) throw new IllegalStateException("Can't extract version number from " + distName)
      kafkaVersion = new Version(distName.substring(hIdx + 1, tgzIdx))

      KafkaDistributionInfo(jar, kafkaVersion, kafkaDist)
    }
  }
}