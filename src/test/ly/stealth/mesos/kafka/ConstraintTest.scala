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

class ConstraintTest {
  @Test
  def parse {
    def c(s: String): Constraint.Condition = new Constraint(s).condition

    // like
    var like = c("like:1").asInstanceOf[Constraint.Like]
    assertEquals("1", like.regex)

    // unlike
    like = c("unlike:1").asInstanceOf[Constraint.Like]
    assertEquals("1", like.regex)

    // unique
    assertEquals(classOf[Constraint.Unique], c("unique").getClass)

    // cluster
    var cluster = c("cluster").asInstanceOf[Constraint.Cluster]
    assertNull(cluster.value)

    cluster = c("cluster:123").asInstanceOf[Constraint.Cluster]
    assertEquals("123", cluster.value)

    // groupBy
    var groupBy = c("groupBy").asInstanceOf[Constraint.GroupBy]
    assertEquals(1, groupBy.groups)

    groupBy = c("groupBy:2").asInstanceOf[Constraint.GroupBy]
    assertEquals(2, groupBy.groups)

    // unsupported
    try { c("unsupported"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unsupported condition")) }
  }

  @Test
  def matches {
    // smoke tests
    assertTrue(new Constraint("like:abc").matches("abc"))
    assertFalse(new Constraint("like:abc").matches("abc1"))

    assertTrue(new Constraint("like:a.*").matches("abc"))
    assertFalse(new Constraint("like:a.*").matches("bc"))

    assertTrue(new Constraint("unique").matches("a", Array()))
    assertFalse(new Constraint("unique").matches("a", Array("a")))

    assertTrue(new Constraint("cluster").matches("a", Array()))
    assertFalse(new Constraint("cluster").matches("b", Array("a")))

    assertTrue(new Constraint("groupBy").matches("a", Array("a")))
    assertFalse(new Constraint("groupBy").matches("a", Array("b")))
  }

  @Test
  def _toString {
    assertEquals("like:abc", "" + new Constraint("like:abc"))
    assertEquals("groupBy", "" + new Constraint("groupBy"))
    assertEquals("groupBy:2", "" + new Constraint("groupBy:2"))
  }

  @Test
  def Like_matches {
    var like = new Constraint.Like("1.*2")
    assertTrue(like.matches("12"))
    assertTrue(like.matches("1a2"))
    assertTrue(like.matches("1ab2"))

    assertFalse(like.matches("a1a2"))
    assertFalse(like.matches("1a2a"))

    like = new Constraint.Like("1", _negated = true)
    assertFalse(like.matches("1"))
    assertTrue(like.matches("2"))
  }

  @Test
  def Like_toString {
    assertEquals("like:1", "" + new Constraint.Like("1"))
    assertEquals("unlike:1", "" + new Constraint.Like("1", _negated = true))
  }

  @Test
  def Unique_matches {
    val unique = new Constraint.Unique()
    assertTrue(unique.matches("1", Array()))
    assertTrue(unique.matches("2", Array("1")))
    assertTrue(unique.matches("3", Array("1", "2")))

    assertFalse(unique.matches("1", Array("1", "2")))
    assertFalse(unique.matches("2", Array("1", "2")))
  }

  @Test
  def Cluster_matches {
    var cluster = new Constraint.Cluster()
    assertTrue(cluster.matches("1", Array()))
    assertTrue(cluster.matches("2", Array()))

    assertTrue(cluster.matches("1", Array("1")))
    assertTrue(cluster.matches("1", Array("1", "1")))
    assertFalse(cluster.matches("2", Array("1")))

    cluster = new Constraint.Cluster("1")
    assertTrue(cluster.matches("1", Array()))
    assertFalse(cluster.matches("2", Array()))

    assertTrue(cluster.matches("1", Array("1")))
    assertTrue(cluster.matches("1", Array("1", "1")))
    assertFalse(cluster.matches("2", Array("1")))
  }

  @Test
  def GroupBy_matches {
    var groupBy = new Constraint.GroupBy()
    assertTrue(groupBy.matches("1", Array()))
    assertTrue(groupBy.matches("1", Array("1")))
    assertTrue(groupBy.matches("1", Array("1", "1")))
    assertFalse(groupBy.matches("1", Array("2")))

    groupBy = new Constraint.GroupBy(2)
    assertTrue(groupBy.matches("1", Array()))
    assertFalse(groupBy.matches("1", Array("1")))
    assertFalse(groupBy.matches("1", Array("1", "1")))
    assertTrue(groupBy.matches("2", Array("1")))

    assertTrue(groupBy.matches("1", Array("1", "2")))
    assertTrue(groupBy.matches("2", Array("1", "2")))

    assertFalse(groupBy.matches("1", Array("1", "1", "2")))
    assertTrue(groupBy.matches("2", Array("1", "1", "2")))
  }
}
