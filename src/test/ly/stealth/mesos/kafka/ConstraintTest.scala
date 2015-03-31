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

    // same
    var same = c("same").asInstanceOf[Constraint.Same]
    assertEquals(1, same.variants)

    same = c("same:2").asInstanceOf[Constraint.Same]
    assertEquals(2, same.variants)

    // unique
    assertEquals(classOf[Constraint.Unique], c("unique").getClass)

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
    assertFalse(new Constraint("same").matches("a", Array("b")))
  }

  @Test
  def _toString {
    assertEquals("like:abc", "" + new Constraint("like:abc"))
    assertEquals("same", "" + new Constraint("same"))
    assertEquals("same:2", "" + new Constraint("same:2"))
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
  def Same_matches {
    var same = new Constraint.Same(1)
    assertTrue(same.matches("1", Array()))
    assertTrue(same.matches("1", Array("1")))
    assertTrue(same.matches("1", Array("1", "1")))
    assertFalse(same.matches("1", Array("2")))

    same = new Constraint.Same(2)
    assertTrue(same.matches("1", Array()))
    assertFalse(same.matches("1", Array("1")))
    assertFalse(same.matches("1", Array("1", "1")))
    assertTrue(same.matches("2", Array("1")))

    assertTrue(same.matches("1", Array("1", "2")))
    assertTrue(same.matches("2", Array("1", "2")))

    assertFalse(same.matches("1", Array("1", "1", "2")))
    assertTrue(same.matches("2", Array("1", "1", "2")))
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
}
