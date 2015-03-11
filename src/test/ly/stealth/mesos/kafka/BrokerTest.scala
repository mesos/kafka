package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._

class BrokerTest {
  // static methods
  @Test
  def idFromTaskId {
    assertEquals("0", Broker.idFromTaskId(Broker.nextTaskId(new Broker("0"))))
    assertEquals("100", Broker.idFromTaskId(Broker.nextTaskId(new Broker("100"))))
  }
}
