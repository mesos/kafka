package ly.stealth.mesos.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._

class KafkaBrokerServerTest extends KafkaMesosTestCase {
  //@Test
  def launchKafkaBroker {

    Thread.currentThread().setContextClassLoader(KafkaServer.Distro.loader)

    val client = startZkServer()
    client.delete("/brokers/ids/0")

    val defaults: Map[String, String] = Map(
      "broker.id" -> "0",
      "port" -> ("" + 9092),
      "log.dirs" -> "kafka-logs",
      "log.retention.bytes" -> ("" + 10l * 1024 * 1024 * 1024),

      "zookeeper.connect" -> Config.zk,
      "host.name" -> "localhost",
      "listeners" -> s"PLAINTEXT://:9092",
      "log.dirs" -> "data/kafka-logs",
      "auto.create.topics.enable" -> "true"
    )
    val server = KafkaServer.Distro.newServer(defaults)
    server.getClass.getMethod("startup").invoke(server)

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](
      Map[String, Object](
        "bootstrap.servers" -> "localhost:9092"
      ),
      new ByteArraySerializer(),
      new ByteArraySerializer()
    )
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("test", new Array[Byte](100)))
    producer.flush()

    var gotMetrics: Broker.Metrics = null
    KafkaServer.Distro.startCollector(server, m => { gotMetrics = m })

    assertNotNull(gotMetrics)
  }
}
