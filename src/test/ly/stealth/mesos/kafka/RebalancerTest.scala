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

import org.junit.{Test, After, Before}
import org.junit.Assert._
import java.io.File
import org.I0Itec.zkclient.{ZkServer, ZkClient, IDefaultNameSpace}
import kafka.utils.{ZKStringSerializer, ZkUtils}
import java.util

class RebalancerTest extends MesosTestCase {
  var rebalancer: Rebalancer = null

  var zkDir: File = null
  var zkServer: ZkServer = null
  var zkClient: ZkClient = null

  @Before
  override def before {
    super.before
    rebalancer = new Rebalancer()

    val port = 8001
    Config.kafkaZkConnect = s"localhost:$port"

    zkDir = File.createTempFile(getClass.getName, null)
    zkDir.delete()

    val defaultNamespace = new IDefaultNameSpace { def createDefaultNameSpace(zkClient: ZkClient): Unit = {} }
    zkServer = new ZkServer("" + zkDir, "" + zkDir, defaultNamespace, port)
    zkServer.start()

    zkClient = zkServer.getZkClient
    zkClient.setZkSerializer(ZKStringSerializer)
  }

  @After
  override def after {
    super.after

    Config.load()
    zkServer.shutdown()

    def delete(dir: File) {
      val children: Array[File] = dir.listFiles()
      if (children != null) children.foreach(delete)
      dir.delete()
    }
    delete(zkDir)
  }

  @Test
  def start {
    val cluster = Scheduler.cluster
    cluster.addBroker(new Broker("0"))
    cluster.addBroker(new Broker("1"))

    createTopic("topic", Map[Int, Seq[Int]](0 -> Seq(0), 1 -> Seq(0)))
    assertFalse(rebalancer.running)
    rebalancer.start(util.Arrays.asList("0", "1"), null)

    assertTrue(rebalancer.running)
    assertFalse(rebalancer.state.isEmpty)
  }

  @Test
  def start_no_topics {
    try { rebalancer.start(util.Arrays.asList("0", "1"), null); fail() }
    catch { case e: Rebalancer.Exception => assertTrue(e.getMessage, e.getMessage.contains("no topics")) }
  }

  @Test
  def start_in_progress {
    createTopic("topic", Map[Int, Seq[Int]](0 -> Seq(0), 1 -> Seq(0)))
    ZkUtils.createPersistentPath(zkClient, ZkUtils.ReassignPartitionsPath, "")

    try { rebalancer.start(util.Arrays.asList("0", "1"), null); fail() }
    catch { case e: Rebalancer.Exception => assertTrue(e.getMessage, e.getMessage.contains("in progress")) }
  }


  private def createTopic(name: String, assignment: Map[Int, Seq[Int]]) {
    val json: String = ZkUtils.replicaAssignmentZkData(assignment.map(e => "" + e._1 -> e._2))
    ZkUtils.createPersistentPath(zkClient, ZkUtils.getTopicPath(name), json)
  }
}
