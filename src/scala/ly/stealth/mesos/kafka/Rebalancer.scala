package ly.stealth.mesos.kafka

import java.util
import kafka.utils.ZkUtils

class Rebalancer {
  @volatile var running: Boolean = false

  def start(ids: util.List[String]) {
    System.out.println(ZkUtils.getTopicPath("topic"))
  }

  def loadRebalancer {

  }
}
