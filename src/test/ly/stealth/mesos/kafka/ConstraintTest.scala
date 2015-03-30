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

    // pattern
    assertEquals(classOf[Constraint.Pattern], c("").getClass)

    val pattern = c("abc").asInstanceOf[Constraint.Pattern]
    assertEquals("abc", pattern.value)

    // same
    var same = c("#same").asInstanceOf[Constraint.Same]
    assertEquals(1, same.variants)

    same = c("#same:2").asInstanceOf[Constraint.Same]
    assertEquals(2, same.variants)

    // unique
    assertEquals(classOf[Constraint.Unique], c("#unique").getClass)
    
    // regex
    val regex = c("#regex:.").asInstanceOf[Constraint.Regex]
    assertEquals(".", regex.value)

    // unsupported
    try { c("#unsupported"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unsupported condition")) }
  }

  @Test
  def matches {
    // smoke tests
    assertTrue(new Constraint("abc").matches("abc"))
    assertFalse(new Constraint("abc").matches("abc1"))

    assertTrue(new Constraint("a*").matches("abc"))
    assertFalse(new Constraint("a*").matches("bc"))

    assertTrue(new Constraint("#unique").matches("a", Array()))
    assertFalse(new Constraint("#same").matches("a", Array("b")))
  }

  @Test
  def _toString {
    assertEquals("", "" + new Constraint(""))
    assertEquals("abc", "" + new Constraint("abc"))
    assertEquals("#same", "" + new Constraint("#same"))
    assertEquals("#same:2", "" + new Constraint("#same:2"))
  }

  @Test
  def Pattern_negated {
    assertTrue(new Constraint.Pattern("!a").negated)
    assertFalse(new Constraint.Pattern("a").negated)
  }

  @Test
  def Pattern_regex {
    def r(s: String): String = {
      var p: String = new Constraint.Pattern("").regex(s)
      p = p.substring(1, p.length - 1) // remove ^$
      p = p.replace("\\Q", "(")        // simplify escaping
      p = p.replace("\\E", ")")        // simplify escaping
      p
    }

    // empty
    assertEquals("", r(""))

    // literal
    assertEquals("(1)", r("1"))
    assertEquals("(abc)", r("abc"))

    // ?
    assertEquals(".", r("?"))
    assertEquals("(1).", r("1?"))
    assertEquals(".(2)", r("?2"))
    assertEquals("(1).(2)", r("1?2"))

    // *
    assertEquals(".*", r("*"))
    assertEquals("(1).*", r("1*"))
    assertEquals(".*(2)", r("*2"))
    assertEquals("(1).*(2)", r("1*2"))

    // backslash
    assertEquals("(1)", r("\\1"))
    assertEquals("(*)", r("\\*"))
    assertEquals("(?)", r("\\?"))
    assertEquals("(\\)", r("\\\\"))
    assertEquals("(1*?2)", r("1\\*\\?2"))

    try { r("\\"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unterminated \\"))}

    // #,!
    try { r("#"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unescaped #"))}

    try { r("!"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unescaped !"))}

    // complex
    assertEquals("(1?).(2#).*(3\\)", r("1\\??2\\#*3\\\\"))
  }

  @Test
  def Pattern_matches {
    var pattern = new Constraint.Pattern("12")
    assertTrue(pattern.matches("12"))
    assertFalse(pattern.matches("13"))
    assertFalse(pattern.matches("a12b"))

    pattern = new Constraint.Pattern("1*2")
    assertTrue(pattern.matches("12"))
    assertTrue(pattern.matches("1ab2"))
    assertFalse(pattern.matches("a12"))
    assertFalse(pattern.matches("12b"))

    pattern = new Constraint.Pattern("1?2")
    assertTrue(pattern.matches("1a2"))
    assertFalse(pattern.matches("12"))
    assertFalse(pattern.matches("a1a2"))
    assertFalse(pattern.matches("1a2a"))
    assertFalse(pattern.matches("1ab2"))

    // negated
    pattern = new Constraint.Pattern("!a*")
    assertFalse(pattern.matches("ab"))
    assertTrue(pattern.matches("1b"))
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

  @Test
  def Regex_matches {
    val regex = new Constraint.Regex("1.2")
    assertTrue(regex.matches("1a2"))

    assertFalse(regex.matches("12"))
    assertFalse(regex.matches("a1a2"))
    assertFalse(regex.matches("1a2a"))
  }
}
