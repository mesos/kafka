package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._
import ly.stealth.mesos.kafka.Topics.Topic
import java.util

class TopicsTest {
  // Topic
  @Test
  def toJson_fromJson() {
    val topic: Topic = new Topics.Topic("name")
    topic.partitions.put(0, util.Arrays.asList(0, 1))
    topic.partitions.put(1, util.Arrays.asList(1, 2))
    topic.partitions.put(2, util.Arrays.asList(0, 2))
    topic.options = Util.parseMap("a=1,b=2")

    val read: Topic = new Topic()
    read.fromJson(Util.parseJson("" + topic.toJson))

    assertEquals(topic.name, read.name)
    assertEquals(topic.partitions, read.partitions)
    assertEquals(topic.options, read.options)
  }
}
