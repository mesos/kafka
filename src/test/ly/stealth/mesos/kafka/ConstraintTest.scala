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

    // same | unique
    assertEquals(classOf[Constraint.Same], c("#same").getClass)
    assertEquals(classOf[Constraint.Unique], c("#unique").getClass)

    // regex
    val regex = c("#regex:.").asInstanceOf[Constraint.Regex]
    assertEquals(".", regex.value)

    // group
    var group = c("#group").asInstanceOf[Constraint.Group]
    assertEquals(1, group.groups)

    group = c("#group:2").asInstanceOf[Constraint.Group]
    assertEquals(2, group.groups)

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
    val same = new Constraint.Same()
    assertTrue(same.matches("1", Array()))
    assertTrue(same.matches("1", Array("1")))
    assertTrue(same.matches("1", Array("1", "1")))

    assertFalse(same.matches("1", Array("2")))
    assertFalse(same.matches("1", Array("2", "1")))
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

  @Test
  def Group_matches {
    var group = new Constraint.Group(1)
    assertTrue(group.matches("1", Array()))
    assertTrue(group.matches("1", Array("1")))
    assertTrue(group.matches("1", Array("1", "1")))
    assertTrue(group.matches("2", Array("1")))

    group = new Constraint.Group(2)
    assertTrue(group.matches("1", Array()))
    assertFalse(group.matches("1", Array("1")))
    assertTrue(group.matches("2", Array("1")))

    assertTrue(group.matches("1", Array("1", "2")))
    assertTrue(group.matches("2", Array("1", "2")))

    assertFalse(group.matches("1", Array("1", "1", "2")))
    assertTrue(group.matches("2", Array("1", "1", "2")))
  }
}
