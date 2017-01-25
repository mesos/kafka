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

import ly.stealth.mesos.kafka.executor.KafkaServer
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

    var gotMetrics: Broker.Metrics = null
    KafkaServer.Distro.startCollector(server, m => { gotMetrics = m })

    assertNotNull(gotMetrics)
  }
}
