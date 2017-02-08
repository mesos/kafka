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
import java.util
import ly.stealth.mesos.kafka.scheduler.{Expr, Topics}
import net.elodina.mesos.util.Strings.parseMap
import scala.collection.JavaConversions._


class ExprTest extends KafkaMesosTestCase {
  @Before
  override def before {
    super.before
    startZkServer()
  }

  @After
  override def after {
    super.after
    stopZkServer()
  }

  @Test
  def expandBrokers {
    val cluster = registry.cluster

    for (i <- 0 until 5)
      cluster.addBroker(new Broker(i))

    try {
      assertEquals(util.Arrays.asList(), Expr.expandBrokers(cluster, ""))
      fail()
    } catch { case e: IllegalArgumentException => }

    assertEquals(Seq(0), Expr.expandBrokers(cluster, "0"))
    assertEquals(Seq(0, 2, 4), Expr.expandBrokers(cluster, "0,2,4"))
    assertEquals(Seq(1, 2, 3), Expr.expandBrokers(cluster, "1..3"))
    assertEquals(Seq(0, 1, 3, 4), Expr.expandBrokers(cluster, "0..1,3..4"))
    assertEquals(Seq(0, 1, 2, 3, 4), Expr.expandBrokers(cluster, "*"))

    // duplicates
    assertEquals(Seq(0, 1, 2, 3, 4), Expr.expandBrokers(cluster, "0..3,2..4"))

    // sorting
    assertEquals(Seq(2, 3, 4), Expr.expandBrokers(cluster, "4,3,2"))

    // not-existent brokers
    assertEquals(Seq(5, 6, 7), Expr.expandBrokers(cluster, "5,6,7"))
  }

  @Test
  def expandBrokers_attributes {
    val cluster = registry.cluster
    val b0 = cluster.addBroker(new Broker(0))
    val b1 = cluster.addBroker(new Broker(1))
    val b2 = cluster.addBroker(new Broker(2))
    cluster.addBroker(new Broker(3))

    b0.task = Broker.Task(hostname = "master", attributes = parseMap("a=1").toMap)
    b1.task = Broker.Task(hostname = "slave0", attributes = parseMap("a=2,b=2").toMap)
    b2.task = Broker.Task(hostname = "slave1", attributes = parseMap("b=2").toMap)

    // exact match
    assertEquals(Seq(0, 1, 2, 3), Expr.expandBrokers(cluster, "*"))
    assertEquals(Seq(0), Expr.expandBrokers(cluster, "*[a=1]"))
    assertEquals(Seq(1, 2), Expr.expandBrokers(cluster, "*[b=2]"))

    // attribute present
    assertEquals(Seq(0, 1), Expr.expandBrokers(cluster, "*[a]"))
    assertEquals(Seq(1, 2), Expr.expandBrokers(cluster, "*[b]"))

    // hostname
    assertEquals(Seq(0), Expr.expandBrokers(cluster, "*[hostname=master]"))
    assertEquals(Seq(1, 2), Expr.expandBrokers(cluster, "*[hostname=slave*]"))

    // not existent broker
    assertEquals(Seq(), Expr.expandBrokers(cluster, "5[a]"))
    assertEquals(Seq(), Expr.expandBrokers(cluster, "5[]"))
  }

  @Test
  def expandBrokers_sortByAttrs {
    val cluster = registry.cluster
    val b0 = cluster.addBroker(new Broker(0))
    val b1 = cluster.addBroker(new Broker(1))
    val b2 = cluster.addBroker(new Broker(2))
    val b3 = cluster.addBroker(new Broker(3))
    val b4 = cluster.addBroker(new Broker(4))
    val b5 = cluster.addBroker(new Broker(5))
    val b6 = cluster.addBroker(new Broker(6))


    b0.task = Broker.Task(attributes = parseMap("r=2,a=1").toMap)
    b1.task = Broker.Task(attributes = parseMap("r=0,a=1").toMap)
    b2.task = Broker.Task(attributes = parseMap("r=1,a=1").toMap)
    b3.task = Broker.Task(attributes = parseMap("r=1,a=2").toMap)
    b4.task = Broker.Task(attributes = parseMap("r=0,a=2").toMap)
    b5.task = Broker.Task(attributes = parseMap("r=0,a=2").toMap)
    b6.task = Broker.Task(attributes = parseMap("a=2").toMap)

    assertEquals(Seq(0, 1, 2, 3, 4, 5, 6), Expr.expandBrokers(cluster, "*", sortByAttrs = true))
    assertEquals(Seq(1, 2, 0, 4, 3, 5), Expr.expandBrokers(cluster, "*[r]", sortByAttrs = true))
    assertEquals(Seq(1, 4, 2, 3, 0, 5), Expr.expandBrokers(cluster, "*[r,a]", sortByAttrs = true))

    assertEquals(Seq(1, 2, 0), Expr.expandBrokers(cluster, "*[r=*,a=1]", sortByAttrs = true))
    assertEquals(Seq(4, 3, 5), Expr.expandBrokers(cluster, "*[r,a=2]", sortByAttrs = true))
  }

  @Test
  def expandTopics {
    val cluster = registry.cluster
    val topics: Topics = cluster.topics

    topics.addTopic("t0")
    topics.addTopic("t1")
    topics.addTopic("x")

    assertEquals(Seq(), Expr.expandTopics(""))
    assertEquals(Seq("t5", "t6"), Expr.expandTopics("t5,t6"))
    assertEquals(Seq("t0"), Expr.expandTopics("t0"))
    assertEquals(Seq("t0", "t1"), Expr.expandTopics("t0, t1"))
    assertEquals(Set("t0", "t1", "x"), Expr.expandTopics("*").toSet)
    assertEquals(Set("t0", "t1"), Expr.expandTopics("t*").toSet)
  }
}
