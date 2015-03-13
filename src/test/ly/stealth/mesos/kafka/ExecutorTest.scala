package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._
import java.util.Properties
import java.io.StringWriter

class ExecutorTest extends MesosTestCase {
  @Test
  def optionMap {
    val props: Properties = new Properties()
    props.setProperty("a", "1")
    props.setProperty("b", "2")

    val buffer: StringWriter = new StringWriter()
    props.store(buffer, null)

    val task = this.task(data = buffer.toString)
    val read: Map[String, String] = Executor.optionMap(task)
    assertEquals(2, read.size)
    assertEquals("1", read.getOrElse("a", null))
    assertEquals("2", read.getOrElse("b", null))
  }
}
