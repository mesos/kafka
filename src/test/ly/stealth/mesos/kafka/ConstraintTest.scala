package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._

class ConstraintTest {
  @Test
  def condition {
    def c(s: String): Constraint.Condition = {
      new Constraint(s).condition
    }

    assertEquals(classOf[Constraint.Pattern], c("").getClass)
    assertEquals(classOf[Constraint.Pattern], c("abc").getClass)

    assertEquals(classOf[Constraint.Same], c("#same").getClass)
    assertEquals(classOf[Constraint.Unique], c("#unique").getClass)

    assertEquals(classOf[Constraint.Regex], c("#regex:.").getClass)

    assertEquals(classOf[Constraint.Group], c("#group").getClass)
    assertEquals(classOf[Constraint.Group], c("#group:2").getClass)

    // unsupported
    try { c("#unsupported"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("unsupported condition")) }
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
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.equals("unterminated escaping"))}

    // #,!
    try { r("#"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("# inside expression"))}

    try { r("!"); fail() }
    catch { case e: IllegalArgumentException => assertTrue("" + e, e.getMessage.contains("! inside expression"))}

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
