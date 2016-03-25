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

import org.junit.{Before, Test}
import java.util
import org.junit.Assert._
import ly.stealth.mesos.kafka.Broker.State

class ClusterTest extends KafkaMesosTestCase {
  var cluster: Cluster = new Cluster()

  @Before
  override def before {
    super.before
    cluster.clear()
  }

  @Test
  def addBroker_removeBroker_getBrokers {
    assertTrue(cluster.getBrokers.isEmpty)

    val broker0 = cluster.addBroker(new Broker("0"))
    val broker1 = cluster.addBroker(new Broker("1"))
    assertEquals(util.Arrays.asList(broker0, broker1), cluster.getBrokers)

    cluster.removeBroker(broker0)
    assertEquals(util.Arrays.asList(broker1), cluster.getBrokers)

    cluster.removeBroker(broker1)
    assertTrue(cluster.getBrokers.isEmpty)
  }

  @Test
  def getBroker {
    assertNull(cluster.getBroker("0"))

    val broker0 = cluster.addBroker(new Broker("0"))
    assertSame(broker0, cluster.getBroker("0"))
  }

  @Test
  def save_load {
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))
    cluster.save()

    val read = new Cluster()
    read.load()
    assertEquals(2, read.getBrokers.size())
  }

  @Test
  def toJson_fromJson {
    val broker0 = cluster.addBroker(new Broker("0"))
    broker0.task = new Broker.Task("1", "slave", "executor", "host", _state = State.RUNNING)
    cluster.addBroker(new Broker("1"))
    cluster.frameworkId = "id"

    val read = new Cluster()
    read.fromJson(Util.parseJson("" + cluster.toJson))

    assertEquals(cluster.frameworkId, read.frameworkId)
    assertEquals(2, read.getBrokers.size())
    BrokerTest.assertBrokerEquals(broker0, read.getBroker("0"))
  }
}
