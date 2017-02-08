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

import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZkUtils
import scala.collection.JavaConversions._
import java.util
import ly.stealth.mesos.kafka.scheduler.{Rebalancer, ZKStringSerializer, ZkUtilsWrapper}

class RebalancerTest extends KafkaMesosTestCase {
  var rebalancer: Rebalancer = _
  var zkClient: ZkClient = _

  @Before
  override def before {
    super.before
    rebalancer = new Rebalancer()

    val port = 56789
    Config.zk = s"localhost:$port"

    startZkServer()
    zkClient = zkServer.getZkClient
    zkClient.setZkSerializer(ZKStringSerializer)
  }

  @After
  override def after {
    super.after
    stopZkServer()
  }

  @Test
  def start {
    val cluster = registry.cluster
    cluster.addBroker(new Broker(0))
    cluster.addBroker(new Broker(1))

    cluster.topics.addTopic("topic", Map(0 -> Seq(0), 1 -> Seq(0)))
    assertFalse(rebalancer.running)
    rebalancer.start(Seq("topic"), Seq(0, 1))

    assertTrue(rebalancer.running)
    assertFalse(rebalancer.state.isEmpty)
  }

  // This test no longer applies in kafka 0.10.0
  //@Test
  def start_in_progress {
    registry.cluster.topics.addTopic("topic", Map(0 -> Seq(0), 1 -> Seq(0)))
    ZkUtilsWrapper().createPersistentPath(ZkUtils.ReassignPartitionsPath)

    registry.cluster.addBroker(new Broker(2))
    try { rebalancer.start(Seq("topic"), Seq(1, 2)); fail() }
    catch {
      case e: Rebalancer.Exception => assertTrue(e.getMessage, e.getMessage.contains("in progress"))
    }
  }
}
