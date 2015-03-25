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
}
