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

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.Topics
import net.elodina.mesos.util.Strings.{formatMap, parseMap}

class TopicsTest extends KafkaMesosTestCase {
  var topics: Topics = null

  @Before
  override def before {
    super.before
    startZkServer()
    topics = registry.cluster.topics
  }

  @After
  override def after {
    super.after
    stopZkServer()
  }

  @Test
  def getTopic {
    assertNull(topics.getTopic("t"))

    topics.addTopic("t")
    assertNotNull(topics.getTopic("t"))
  }

  @Test
  def getTopics {
    assertEquals(0, topics.getTopics.size)

    topics.addTopic("t0")
    topics.addTopic("t1")
    assertEquals(2, topics.getTopics.size)
  }

  @Test
  def fairAssignment {
    val assignment = topics.fairAssignment(3, 2, Seq(0, 1, 2), 0, 0)
    assertEquals(3, assignment.size)
    assertEquals(Seq(0, 1), assignment(0))
    assertEquals(Seq(1, 2), assignment(1))
    assertEquals(Seq(2, 0), assignment(2))
  }

  @Test
  def addTopic {
    topics.addTopic("t0", topics.fairAssignment(2, 1), options = parseMap("flush.ms=1000"))
    topics.addTopic("t1")

    val _topics: util.List[Topic] = topics.getTopics
    assertEquals(2, _topics.size())

    val t0: Topic = _topics.get(0)
    assertEquals("t0", t0.name)
    assertEquals("flush.ms=1000", formatMap(t0.options))

    assertEquals(2, t0.partitions.size)
    assertEquals(Seq(0), t0.partitions(0))
    assertEquals(Seq(0), t0.partitions(1))
  }

  @Test
  def updateTopic {
    var t: Topic = topics.addTopic("t")
    topics.updateTopic(t, parseMap("flush.ms=1000"))

    t = topics.getTopic(t.name)
    assertEquals("flush.ms=1000", formatMap(t.options))
  }

  @Test
  def validateOptions {
    assertNull(topics.validateOptions(parseMap("flush.ms=1000")))
    assertNotNull(topics.validateOptions(parseMap("invalid=1000")))
  }

  // Topic
  @Test
  def Topic_toJson_fromJson() {
    val topic = Topic(
      "name",
      Map(
        0 -> Seq(0, 1),
        1 -> Seq(1, 2),
        2 -> Seq(0, 2)
      ),
      parseMap("a=1,b=2")
    )

    val read = JsonUtil.fromJson[Topic](JsonUtil.toJson(topic))
    assertEquals(topic.name, read.name)
    assertEquals(topic.partitions(0), read.partitions(0))
    assertEquals(topic.options, read.options)
  }
}
