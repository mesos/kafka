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
package ly.stealth.mesos.kafka.json


import java.util.Date
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import ly.stealth.mesos.kafka.{Broker, Cluster, Topic}
import ly.stealth.mesos.kafka.Broker._
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util.{Constraint, Period, Range, Strings}
import scala.collection.JavaConversions._

object IgnoreMetricsAttribute {}

class BindAddressSerializer extends StdSerializer[BindAddress](classOf[BindAddress]) {
  override def serialize(value: BindAddress, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString("" + value)
  }
}

class BindAddressDeserializer extends StdDeserializer[BindAddress](classOf[BindAddress]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): BindAddress = {
    new BindAddress(p.readValueAs(classOf[String]))
  }
}

case class BrokerModel(
                        id: String, active: Boolean, cpus: Double, mem: Long, heap: Long,
                        port: Range, volume: String, bindAddress: BindAddress, syslog: Boolean,
                        constraints: String,
                        options: String, log4jOptions: String, jvmOptions: String,
                        stickiness: Stickiness, failover: Failover, task: Task,
                        metrics: Metrics, needsRestart: Boolean, executionOptions: ExecutionOptions
                      )

class BrokerSerializer extends StdSerializer[Broker](classOf[Broker]) {
  override def serialize(b: Broker, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    val metrics =
      if (provider.getAttribute(IgnoreMetricsAttribute) != null) {
        Metrics()
      } else {
        b.metrics
      }

    provider.defaultSerializeValue(BrokerModel(
      b.id.toString, b.active, b.cpus, b.mem, b.heap, b.port, b.volume, b.bindAddress, b.syslog,
      Strings.formatMap(b.constraints), Strings.formatMap(b.options), Strings.formatMap(b.log4jOptions),
      b.executionOptions.jvmOptions, b.stickiness, b.failover, b.task, metrics, b.needsRestart,
      b.executionOptions
    ), gen)
  }
}

class BrokerDeserializer extends StdDeserializer[Broker](classOf[Broker]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Broker = {
    val model = p.readValueAs(classOf[BrokerModel])
    val b = new Broker(model.id.toInt)
    b.active = model.active
    b.cpus = model.cpus
    b.mem = model.mem
    b.heap = model.heap
    b.port = model.port
    b.volume = model.volume
    b.bindAddress = model.bindAddress
    b.syslog = model.syslog

    b.constraints = Strings.parseMap(model.constraints).mapValues(new Constraint(_)).toMap
    b.options = Strings.parseMap(model.options).toMap
    b.log4jOptions = Strings.parseMap(model.log4jOptions).toMap

    b.stickiness = if (model.stickiness != null) model.stickiness else new Stickiness()
    b.failover = model.failover
    b.metrics = model.metrics
    b.needsRestart = model.needsRestart

    b.task = model.task
    if (model.executionOptions != null)
      b.executionOptions = model.executionOptions
    if (model.jvmOptions != null)
      b.executionOptions = b.executionOptions.copy(jvmOptions = model.jvmOptions)

    b
  }
}

class ContainerTypeSerializer extends StdSerializer[ContainerType](classOf[ContainerType]) {
  override def serialize(value: ContainerType, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.toString)
}

class ContainerTypeDeserializer extends StdDeserializer[ContainerType](classOf[ContainerType]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): ContainerType =
    ContainerType.valueOf(p.readValueAs(classOf[String]))
}


class EndpointSerializer extends StdSerializer[Endpoint](classOf[Endpoint]) {
  override def serialize(value: Endpoint, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString("" + value)
  }
}

class EndpointDeserializer extends StdDeserializer[Endpoint](classOf[Endpoint]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Endpoint = {
    new Endpoint(p.readValueAs(classOf[String]))
  }
}

case class FailoverModel(delay: Period, maxDelay: Period, maxTries: Integer, failures: Int, failureTime: Date)

class FailoverSerializer extends StdSerializer[Failover](classOf[Failover]) {
  override def serialize(f: Failover, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    provider.defaultSerializeValue(
      FailoverModel(f.delay, f.maxDelay, f.maxTries, f.failures, f.failureTime),
      gen
    )
  }
}

class FailoverDeserializer extends StdDeserializer[Failover](classOf[Failover]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Failover = {
    val model = p.readValueAs(classOf[FailoverModel])
    val f = new Failover()
    f.delay = model.delay
    f.maxDelay = model.maxDelay
    f.maxTries = model.maxTries
    f.failures = model.failures
    f.failureTime = model.failureTime
    f
  }
}

class MetricsSerializer extends StdSerializer[Metrics](classOf[Metrics]) {
  override def serialize(value: Metrics, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    if (value.data != null) {
      val dct = Map("timestamp" -> value.timestamp.asInstanceOf[Number]) ++ value.data
      provider.defaultSerializeValue(dct, gen)
    }
  }
}

class MetricsDeserializer extends StdDeserializer[Metrics](classOf[Metrics]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Metrics = {
    val node = p.readValueAs(classOf[Map[String, Number]])
    val data = node.filter {
      case ("timestamp", _) => false
      case _ => true
    }
    val timestamp = node("timestamp").longValue()
    Metrics(data, timestamp)
  }
}


class MountModeSerializer extends StdSerializer[MountMode](classOf[MountMode]) {
  override def serialize(value: MountMode, gen: JsonGenerator, provider: SerializerProvider): Unit =
    gen.writeString(value.toString)
}

class MountModeDeserializer extends StdDeserializer[MountMode](classOf[Mount]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): MountMode =
    p.readValueAs(classOf[String]) match {
      case "rw" => MountMode.ReadWrite
      case "r" | "ro" => MountMode.ReadOnly
    }
}

class PeriodSerializer extends StdSerializer[Period](classOf[Period]) {
  override def serialize(value: Period, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString("" + value)
  }
}

class RangeSerializer extends StdSerializer[Range](classOf[Range]) {
  override def serialize(value: Range, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    gen.writeString("" + value)
  }
}

class RangeDeserializer extends StdDeserializer[Range](classOf[Range]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Range = {
    new Range(p.readValueAs(classOf[String]))
  }
}

case class StickinessModel(period: Period, stopTime: Date, hostname: String)

class StickinessSerializer extends StdSerializer[Stickiness](classOf[Stickiness]) {
  override def serialize(s: Stickiness, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    provider.defaultSerializeValue(StickinessModel(s.period, s.stopTime, s.hostname), gen)
  }
}

class StickinessDeserializer extends StdDeserializer[Stickiness](classOf[Stickiness]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Stickiness = {
    val model = p.readValueAs(classOf[StickinessModel])
    val s = new Stickiness()
    s.hostname = model.hostname
    s.stopTime = model.stopTime
    s.period = model.period
    s
  }
}

case class TaskModel(id: String, slaveId: String, executorId: String, hostname: String,
                     endpoint: Endpoint, attributes: Map[String, String], state: String)
class TaskSerializer extends StdSerializer[Task](classOf[Task]) {
  override def serialize(t: Task, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    provider.defaultSerializeValue(
      TaskModel(
        t.id, t.slaveId, t.executorId, t.hostname, t.endpoint,
        t.attributes, t.state),
      gen)
  }
}

class TaskDeserializer extends StdDeserializer[Task](classOf[Task]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Task = {
    val model = p.readValueAs(classOf[TaskModel])
    val t = Task(
      id = model.id,
      slaveId = model.slaveId,
      executorId = model.executorId,
      hostname = model.hostname,
      attributes = model.attributes
    )
    t.endpoint = model.endpoint
    t.state = model.state
    t
  }
}

abstract class TopicModel {
  @JsonDeserialize(keyAs = classOf[Integer])
  val partitions: Map[Int, Seq[Int]] = Map()
}

case class ClusterModel(frameworkId: String, brokers: Seq[Broker], version: String)

class ClusterSerializer extends StdSerializer[Cluster](classOf[Cluster]) {
  override def serialize(c: Cluster, gen: JsonGenerator, provider: SerializerProvider): Unit = {
    provider.setAttribute(IgnoreMetricsAttribute, true)
    provider.defaultSerializeValue(ClusterModel(c.frameworkId, c.getBrokers, c.version), gen)
  }
}

class ClusterDeserializer extends StdDeserializer[Cluster](classOf[Cluster]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Cluster = {
    val model = p.readValueAs(classOf[ClusterModel])
    val c = new Cluster()
    c.frameworkId = model.frameworkId
    Option(model.brokers).toList
      .flatten
      .foreach(c.addBroker)
    c
  }
}

object KafkaObjectModel extends SimpleModule {
  override def setupModule(context: SetupContext): Unit = {
    this.setMixInAnnotation(classOf[Topic], classOf[TopicModel])

    this.addSerializer(classOf[BindAddress], new BindAddressSerializer())
    this.addDeserializer(classOf[BindAddress], new BindAddressDeserializer())

    this.addSerializer(classOf[Broker], new BrokerSerializer())
    this.addDeserializer(classOf[Broker], new BrokerDeserializer())

    this.addSerializer(classOf[ContainerType], new ContainerTypeSerializer())
    this.addDeserializer(classOf[ContainerType], new ContainerTypeDeserializer())

    this.addSerializer(classOf[Cluster], new ClusterSerializer())
    this.addDeserializer(classOf[Cluster], new ClusterDeserializer())

    this.addSerializer(classOf[Endpoint], new EndpointSerializer())
    this.addDeserializer(classOf[Endpoint], new EndpointDeserializer())

    this.addSerializer(classOf[Failover], new FailoverSerializer())
    this.addDeserializer(classOf[Failover], new FailoverDeserializer())

    this.addSerializer(classOf[Metrics], new MetricsSerializer())
    this.addDeserializer(classOf[Metrics], new MetricsDeserializer())

    this.addSerializer(classOf[MountMode], new MountModeSerializer())
    this.addDeserializer(classOf[MountMode], new MountModeDeserializer())

    this.addSerializer(classOf[Period], new PeriodSerializer())

    this.addSerializer(classOf[Range], new RangeSerializer())
    this.addDeserializer(classOf[Range], new RangeDeserializer())

    this.addSerializer(classOf[Stickiness], new StickinessSerializer())
    this.addDeserializer(classOf[Stickiness], new StickinessDeserializer())

    this.addSerializer(classOf[Task], new TaskSerializer())
    this.addDeserializer(classOf[Task], new TaskDeserializer())

    super.setupModule(context)
  }
}
