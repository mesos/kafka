package ly.stealth.mesos.kafka

import scala.collection.mutable
import net.elodina.mesos.util.Version
import java.util
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

trait Migration {
  def version: Version
  def apply(json: Map[String, Object]): Map[String, Object]
}

object Migration {
  private val logger: Logger = Logger.getLogger(this.getClass)

  private val values: util.TreeMap[Version, Migration] = new util.TreeMap[Version, Migration]()
  private def add(m: Migration): Unit = { values.put(m.version, m); }
  add(M_0_9_5_1)

  def all: util.List[Migration] = new util.ArrayList(values.values())

  def get(from: Version, fromIncl: Boolean, to: Version, toIncl: Boolean): util.List[Migration] = new util.ArrayList[Migration](values.subMap(from, fromIncl, to, toIncl).values())

  def apply(from: Version, to: Version, json: Map[String, Object]): Map[String, Object] = {
    var result: Map[String, Object] = json
    for (m: Migration <- get(from, fromIncl = false, to, toIncl = true)) {
      logger.info("Applying migration " + m.version)
      result = m.apply(result)
    }
    result
  }

  object M_0_9_5_1 extends Migration {
    def version: Version = new Version("0.9.5.1")

    def apply(_json: Map[String, Object]): Map[String, Object] = {
      val json = new mutable.HashMap[String, Object]() ++ _json

      val brokersJson: List[mutable.HashMap[String, Object]] = json("brokers").asInstanceOf[List[Map[String, Object]]]
        .map((a: Map[String, Object]) => new mutable.HashMap[String, Object]() ++= a)

      for (brokerJson <- brokersJson) {
        if (!brokerJson.contains("syslog")) brokerJson("syslog") = Boolean.box(x = false)
        if (!brokerJson.contains("stickiness")) brokerJson("stickiness") = new Broker.Stickiness().toJson.obj
      }

      json("brokers") = brokersJson.map(_.toMap)
      json.toMap
    }
  }
}
