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

import org.junit.Test
import org.junit.Assert._
import net.elodina.mesos.util.{Constraint, Period, Range}
import ly.stealth.mesos.kafka.Broker.{Endpoint, ExecutionOptions, Failover, Stickiness, Task}
import scala.io.Source

class JsonTest {

  def getResourceJson[T](file: String)(implicit m: Manifest[T]): T = {
    val fileData = this.getClass.getResource(file)
    val txt = Source.fromFile(fileData.getFile)
    json.JsonUtil.fromJson[T](txt.mkString)
  }

  @Test
  def broker_legacy(): Unit = {
    val broker = getResourceJson[Broker]("/broker.json")

    val b = new Broker(1)
    b.task = Task(
      id = "kafka-general-0-705d56e2-7d62-4d7e-b033-74ea5526ed82",
      hostname = "host1",
      executorId = "kafka-general-0-ff258207-18f3-4fd2-9028-5f4c4143f84d",
      attributes = Map(
        "ip" -> "10.253.166.214",
        "host" -> "host1",
        "ami" -> "ami-d0232eba",
        "cluster" -> "us-east-1",
        "dedicated" -> "kafka/general",
        "zone" -> "us-east-1d",
        "instance_type" -> "i2.2xlarge"
      ),
      slaveId = "1fbd3a0d-a685-47e6-8066-01be06d68fac-S821"
    )
    b.task.state = Broker.State.RUNNING
    b.task.endpoint = new Endpoint("host1:9092")
    b.syslog = false
    b.stickiness = new Stickiness(new Period("10m"))
    b.stickiness.registerStart("host1")
    b.log4jOptions = Map("k1" -> "v1", "k2" -> "v2")
    b.options = Map("a" -> "1", "b" -> "2")
    b.active = true
    b.port = new Range(9092)
    b.constraints = Map("dedicated" -> new Constraint("like:kafka/general"))
    b.mem = 56320
    b.executionOptions = ExecutionOptions(jvmOptions = "-server")
    b.cpus = 7
    b.heap = 5120
    b.failover = new Failover(new Period("1m"), new Period("10m"))

    BrokerTest.assertBrokerEquals(b, broker)
  }

  @Test
  def topic_legacy(): Unit = {
    val topic = getResourceJson[Topic]("/topic.json")
    val t = Topic(
      "__consumer_offsets",
      Map(
        45 -> Seq(5,3,4),
        34 -> Seq(2,7,0)
      ),
      Map(
        "cleanup.policy" -> "compact",
        "compression.type" -> "uncompressed",
        "segment.bytes" -> "104857600"
      )
    )
    assertEquals(t, topic)
  }
}
