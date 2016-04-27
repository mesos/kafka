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

import java.util
import org.apache.mesos.Protos.Resource.{ReservationInfo, DiskInfo}
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence
import org.apache.mesos.Protos.Volume.Mode

import scala.collection.JavaConversions._
import scala.collection
import org.apache.mesos.Protos.{Volume, Value, Resource, Offer}
import java.util._
import ly.stealth.mesos.kafka.Broker.{Metrics, Stickiness, Failover}
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util.{Strings, Period, Range, Repr}
import java.text.SimpleDateFormat
import scala.List
import scala.collection.Map
import scala.util.parsing.json.JSONObject

class Broker(_id: String = "0") {
  var id: String = _id
  @volatile var active: Boolean = false

  var cpus: Double = 1
  var mem: Long = 2048
  var heap: Long = 1024
  var port: Range = null
  var volume: String = null
  var bindAddress: BindAddress = null

  var constraints: util.Map[String, Constraint] = new util.LinkedHashMap()
  var options: util.Map[String, String] = new util.LinkedHashMap()
  var log4jOptions: util.Map[String, String] = new util.LinkedHashMap()
  var jvmOptions: String = null

  var stickiness: Stickiness = new Stickiness()
  var failover: Failover = new Failover()

  var metrics: Metrics = null

  // broker has been modified while being in non stopped state, once stopped or before task launch becomes false
  var needsRestart: Boolean = false

  def options(defaults: util.Map[String, String] = null): util.Map[String, String] = {
    val result = new util.LinkedHashMap[String, String]()
    if (defaults != null) result.putAll(defaults)
    
    result.putAll(options)

    if (bindAddress != null)
      result.put("host.name", bindAddress.resolve())

    for ((k, v) <- result)
      result.put(k, v.replace("$id", id))

    result
  }

  @volatile var task: Broker.Task = null

  def matches(offer: Offer, now: Date = new Date(), otherAttributes: Broker.OtherAttributes = Broker.NoAttributes): String = {
    // check resources
    val reservation: Broker.Reservation = getReservation(offer)
    if (reservation.cpus < cpus) return s"cpus < $cpus"
    if (reservation.mem < mem) return s"mem < $mem"
    if (reservation.port == -1) return "no suitable port"

    // check stickiness
    if (!stickiness.allowsHostname(offer.getHostname, now))
      return "hostname != stickiness host"

    // check attributes
    val offerAttributes = new util.HashMap[String, String]()
    offerAttributes.put("hostname", offer.getHostname)

    for (attribute <- offer.getAttributesList)
      if (attribute.hasText) offerAttributes.put(attribute.getName, attribute.getText.getValue)

    // check volume
    if (volume != null && reservation.volume == null)
      return s"offer missing volume: $volume"

    // check constraints
    for ((name, constraint) <- constraints) {
      if (!offerAttributes.containsKey(name)) return s"no $name"
      if (!constraint.matches(offerAttributes.get(name), otherAttributes(name))) return s"$name doesn't match $constraint"
    }

    null
  }

  def getReservation(offer: Offer): Broker.Reservation = {
    var sharedCpus: Double = 0
    var roleCpus: Double = 0
    var reservedSharedCpus: Double = 0
    var reservedRoleCpus: Double = 0

    var sharedMem: Long = 0
    var roleMem: Long = 0
    var reservedSharedMem: Long = 0
    var reservedRoleMem: Long = 0

    val sharedPorts: util.List[Range] = new util.ArrayList[Range]()
    val rolePorts: util.List[Range] = new util.ArrayList[Range]()
    var reservedSharedPort: Long = -1
    var reservedRolePort: Long = -1

    var role: String = null

    var reservedVolume: String = null
    var reservedVolumeSize: Double = 0
    var reservedVolumePrincipal: String = null

    for (resource <- offer.getResourcesList) {
      if (resource.getRole == "*") {
        // shared resources
        if (resource.getName == "cpus") sharedCpus = resource.getScalar.getValue
        if (resource.getName == "mem") sharedMem = resource.getScalar.getValue.toLong
        if (resource.getName == "ports") sharedPorts.addAll(resource.getRanges.getRangeList.map(r => new Range(r.getBegin.toInt, r.getEnd.toInt)))
      } else {
        if (role != null && role != resource.getRole)
          throw new IllegalArgumentException(s"Offer contains 2 non-default roles: $role, ${resource.getRole}")
        role = resource.getRole

        // static role-reserved resources
        if (!resource.hasReservation) {
          if (resource.getName == "cpus") roleCpus = resource.getScalar.getValue
          if (resource.getName == "mem") roleMem = resource.getScalar.getValue.toLong
          if (resource.getName == "ports") rolePorts.addAll(resource.getRanges.getRangeList.map(r => new Range(r.getBegin.toInt, r.getEnd.toInt)))
        }

        // dynamic role/principal-reserved volume
        if (volume != null && resource.hasDisk && resource.getDisk.hasPersistence && resource.getDisk.getPersistence.getId == volume) {
          reservedVolume = volume
          reservedVolumeSize = resource.getScalar.getValue
          reservedVolumePrincipal = resource.getReservation.getPrincipal
        }
      }
    }

    reservedRoleCpus = Math.min(cpus, roleCpus)
    reservedSharedCpus = Math.min(cpus - reservedRoleCpus, sharedCpus)

    reservedRoleMem = Math.min(mem, roleMem)
    reservedSharedMem = Math.min(mem - reservedRoleMem, sharedMem)

    reservedRolePort = getSuitablePort(rolePorts)
    if (reservedRolePort == -1)
      reservedSharedPort = getSuitablePort(sharedPorts)

    new Broker.Reservation(role,
      reservedSharedCpus, reservedRoleCpus,
      reservedSharedMem, reservedRoleMem,
      reservedSharedPort, reservedRolePort,
      reservedVolume, reservedVolumeSize, reservedVolumePrincipal
    )
  }

  private[kafka] def getSuitablePort(ports: util.List[Range]): Int = {
    if (ports.isEmpty) return -1

    val ports_ = ports.sortBy(r => r.start)
    if (port == null)
      return ports_.get(0).start

    for (range <- ports_) {
      val overlap = range.overlap(port)
      if (overlap != null)
        return overlap.start
    }

    -1
  }

  def shouldStart(hostname: String, now: Date = new Date()): Boolean =
    active && task == null && !failover.isWaitingDelay(now)

  def shouldStop: Boolean = !active && task != null && !task.stopping
  
  def registerStart(hostname: String): Unit = {
    stickiness.registerStart(hostname)
    failover.resetFailures()
  }

  def registerStop(now: Date = new Date(), failed: Boolean = false): Unit = {
    if (!failed || failover.failures == 0) stickiness.registerStop(now)

    if (failed) failover.registerFailure(now)
    else failover.resetFailures()
  }

  def state(now: Date = new Date()): String = {
    if (task != null && !task.starting) return task.state

    if (active) {
      if (failover.isWaitingDelay(now)) {
        var s = "failed " + failover.failures
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += " " + Repr.dateTime(failover.failureTime)
        s += ", next start " + Repr.dateTime(failover.delayExpires)
        return s
      }

      if (failover.failures > 0) {
        var s = "starting " + (failover.failures + 1)
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += ", failed " + Repr.dateTime(failover.failureTime)
        return s
      }

      return "starting"
    }

    "stopped"
  }

  def waitFor(state: String, timeout: Period): Boolean = {
    def matches: Boolean = if (state != null) task != null && task.state == state else task == null

    var t = timeout.ms
    while (t > 0 && !matches) {
      val delay = Math.min(100, t)
      Thread.sleep(delay)
      t -= delay
    }

    matches
  }

  def fromJson(node: Map[String, Object]): Unit = {
    id = node("id").asInstanceOf[String]
    active = node("active").asInstanceOf[Boolean]

    cpus = node("cpus").asInstanceOf[Number].doubleValue()
    mem = node("mem").asInstanceOf[Number].longValue()
    heap = node("heap").asInstanceOf[Number].longValue()
    if (node.contains("port")) port = new Range(node("port").asInstanceOf[String])
    if (node.contains("volume")) volume = node("volume").asInstanceOf[String]
    if (node.contains("bindAddress")) bindAddress = new BindAddress(node("bindAddress").asInstanceOf[String])

    if (node.contains("constraints")) constraints = Strings.parseMap(node("constraints").asInstanceOf[String])
                                                    .mapValues(new Constraint(_)).view.force
    if (node.contains("options")) options = Strings.parseMap(node("options").asInstanceOf[String])
    if (node.contains("log4jOptions")) log4jOptions = Strings.parseMap(node("log4jOptions").asInstanceOf[String])
    if (node.contains("jvmOptions")) jvmOptions = node("jvmOptions").asInstanceOf[String]

    if (node.contains("stickiness")) stickiness.fromJson(node("stickiness").asInstanceOf[Map[String, Object]])
    failover.fromJson(node("failover").asInstanceOf[Map[String, Object]])

    if (node.contains("task")) {
      task = new Broker.Task()
      task.fromJson(node("task").asInstanceOf[Map[String, Object]])
    }

    if (node.contains("metrics")) {
      metrics = new Broker.Metrics()
      metrics.fromJson(node("metrics").asInstanceOf[Map[String, Object]])
    }

    if (node.contains("needsRestart")) needsRestart = node("needsRestart").asInstanceOf[Boolean]
  }

  def toJson: JSONObject = {
    val obj = new collection.mutable.LinkedHashMap[String, Any]()
    obj("id") = id
    obj("active") = active

    obj("cpus") = cpus
    obj("mem") = mem
    obj("heap") = heap
    if (port != null) obj("port") = "" + port
    if (volume != null) obj("volume") = volume
    if (bindAddress != null) obj("bindAddress") = "" + bindAddress

    if (!constraints.isEmpty) obj("constraints") = Strings.formatMap(constraints)
    if (!options.isEmpty) obj("options") = Strings.formatMap(options)
    if (!log4jOptions.isEmpty) obj("log4jOptions") = Strings.formatMap(log4jOptions)
    if (jvmOptions != null) obj("jvmOptions") = jvmOptions

    obj("stickiness") = stickiness.toJson
    obj("failover") = failover.toJson
    if (task != null) obj("task") = task.toJson
    if (metrics != null) obj("metrics") = metrics.toJson
    if (needsRestart) obj("needsRestart") = needsRestart

    new JSONObject(obj.toMap)
  }
}

object Broker {
  def nextTaskId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()
  def nextExecutorId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()

  def idFromTaskId(taskId: String): String = taskId.dropRight(37).replace(Config.frameworkName + "-", "")

  def idFromExecutorId(executorId: String): String = idFromTaskId(executorId)

  def isOptionOverridable(name: String): Boolean = !List("broker.id", "port", "zookeeper.connect").contains(name)

  class Stickiness(_period: Period = new Period("10m")) {
    var period: Period = _period
    @volatile var hostname: String = null
    @volatile var stopTime: Date = null

    def expires: Date = if (stopTime != null) new Date(stopTime.getTime + period.ms) else null

    def registerStart(hostname: String): Unit = {
      this.hostname = hostname
      stopTime = null
    }

    def registerStop(now: Date = new Date()): Unit = {
      this.stopTime = now
    }

    def allowsHostname(hostname: String, now: Date = new Date()): Boolean = {
      if (this.hostname == null) return true
      if (stopTime == null || now.getTime - stopTime.getTime >= period.ms) return true
      this.hostname == hostname
    }

    def fromJson(node: Map[String, Object]): Unit = {
      period = new Period(node("period").asInstanceOf[String])
      if (node.contains("stopTime")) stopTime = dateTimeFormat.parse(node("stopTime").asInstanceOf[String])
      if (node.contains("hostname")) hostname = node("hostname").asInstanceOf[String]
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("period") = "" + period
      if (stopTime != null) obj("stopTime") = dateTimeFormat.format(stopTime)
      if (hostname != null) obj("hostname") = hostname

      new JSONObject(obj.toMap)
    }
  }

  class Failover(_delay: Period = new Period("1m"), _maxDelay: Period = new Period("10m")) {
    var delay: Period = _delay
    var maxDelay: Period = _maxDelay
    var maxTries: Integer = null

    @volatile var failures: Int = 0
    @volatile var failureTime: Date = null

    def currentDelay: Period = {
      if (failures == 0) return new Period("0")

      val multiplier = 1 << (failures - 1)
      val d = delay.ms * multiplier

      if (d > maxDelay.ms) maxDelay else new Period(delay.value * multiplier + delay.unit)
    }

    def delayExpires: Date = {
      if (failures == 0) return new Date(0)
      new Date(failureTime.getTime + currentDelay.ms)
    }

    def isWaitingDelay(now: Date = new Date()): Boolean = delayExpires.getTime > now.getTime

    def isMaxTriesExceeded: Boolean = {
      if (maxTries == null) return false
      failures >= maxTries
    }

    def registerFailure(now: Date = new Date()): Unit = {
      failures += 1
      failureTime = now
    }

    def resetFailures(): Unit = {
      failures = 0
      failureTime = null
    }

    def fromJson(node: Map[String, Object]): Unit = {
      delay = new Period(node("delay").asInstanceOf[String])
      maxDelay = new Period(node("maxDelay").asInstanceOf[String])
      if (node.contains("maxTries")) maxTries = node("maxTries").asInstanceOf[Number].intValue()

      if (node.contains("failures")) failures = node("failures").asInstanceOf[Number].intValue()
      if (node.contains("failureTime")) failureTime = dateTimeFormat.parse(node("failureTime").asInstanceOf[String])
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("delay") = "" + delay
      obj("maxDelay") = "" + maxDelay
      if (maxTries != null) obj("maxTries") = maxTries

      if (failures != 0) obj("failures") = failures
      if (failureTime != null) obj("failureTime") = dateTimeFormat.format(failureTime)

      new JSONObject(obj.toMap)
    }
  }

  class Task(
    _id: String = null,
    _slaveId: String = null,
    _executorId: String = null,
    _hostname: String = null,
    _attributes: util.Map[String, String] = Collections.emptyMap(),
    _state: String = State.STARTING
  ) {
    var id: String = _id
    var slaveId: String = _slaveId
    var executorId: String = _executorId

    var hostname: String = _hostname
    var endpoint: Broker.Endpoint = null
    var attributes: util.Map[String, String] = _attributes

    @volatile var state: String = _state

    def starting: Boolean = state == State.STARTING
    def running: Boolean = state == State.RUNNING
    def stopping: Boolean = state == State.STOPPING
    def reconciling: Boolean = state == State.RECONCILING

    def fromJson(node: Map[String, Object]): Unit = {
      id = node("id").asInstanceOf[String]
      slaveId = node("slaveId").asInstanceOf[String]
      executorId = node("executorId").asInstanceOf[String]

      hostname = node("hostname").asInstanceOf[String]
      if (node.contains("endpoint")) endpoint = new Endpoint(node("endpoint").asInstanceOf[String])
      if (endpoint == null && node.contains("port")) // bc
        endpoint = new Endpoint(hostname, node("port").asInstanceOf[Number].intValue())

      attributes = node("attributes").asInstanceOf[Map[String, String]]
      state = node("state").asInstanceOf[String]
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("id") = id
      obj("slaveId") = slaveId
      obj("executorId") = executorId

      obj("hostname") = hostname
      if (endpoint != null) obj("endpoint") = "" + endpoint

      obj("attributes") = new JSONObject(attributes.toMap)
      obj("state") = state

      new JSONObject(obj.toMap)
    }
  }

  class Endpoint(s: String) {
    var hostname: String = null
    var port: Int = -1

    {
      val idx = s.indexOf(":")
      if (idx == -1) throw new IllegalArgumentException(s)

      hostname = s.substring(0, idx)
      port = Integer.parseInt(s.substring(idx + 1))
    }

    def this(hostname: String, port: Int) = this(hostname + ":" + port)

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Endpoint]) return false
      val endpoint = obj.asInstanceOf[Endpoint]
      hostname == endpoint.hostname && port == endpoint.port
    }

    override def hashCode(): Int = 31 * hostname.hashCode + port.hashCode()

    override def toString: String = hostname + ":" + port
  }
  
  class Reservation(
     _role: String = null,
     _sharedCpus: Double = 0.0, _roleCpus: Double = 0.0,
     _sharedMem: Long = 0, _roleMem: Long = 0,
     _sharedPort: Long = -1, _rolePort: Long = -1,
     _volume: String = null, _volumeSize: Double = 0.0, _volumePrincipal: String = null
  ) {
    val role: String = _role

    val sharedCpus: Double = _sharedCpus
    val roleCpus: Double = _roleCpus
    def cpus: Double = sharedCpus + roleCpus

    val sharedMem: Long = _sharedMem
    val roleMem: Long = _roleMem
    def mem: Long = sharedMem + roleMem

    val sharedPort: Long = _sharedPort
    val rolePort: Long = _rolePort
    def port: Long = if (rolePort != -1) rolePort else sharedPort

    val volume: String = _volume
    val volumeSize: Double = _volumeSize
    val volumePrincipal: String = _volumePrincipal

    def toResources: util.List[Resource] = {
      def cpus(value: Double, role: String): Resource = {
        Resource.newBuilder
          .setName("cpus")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .build()
      }
      
      def mem(value: Long, role: String): Resource = {
        Resource.newBuilder
          .setName("mem")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .build()
      }
      
      def port(value: Long, role: String): Resource = {
        Resource.newBuilder
            .setName("ports")
            .setType(Value.Type.RANGES)
            .setRanges(Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(value).setEnd(value)))
            .setRole(role)
            .build()
      }

      def volumeDisk(id: String, value: Double, role: String, principal: String): Resource = {
        val volume = Volume.newBuilder.setMode(Mode.RW).setContainerPath("data").build()
        val persistence = Persistence.newBuilder.setId(id).build()
        
        val disk = DiskInfo.newBuilder
          .setPersistence(persistence)
          .setVolume(volume)
          .build()

        val reservation = ReservationInfo.newBuilder.setPrincipal(principal).build()
        
        Resource.newBuilder
          .setName("disk")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .setDisk(disk)
          .setReservation(reservation)
          .build()
      }
      
      val resources: util.List[Resource] = new util.ArrayList[Resource]()

      if (sharedCpus > 0) resources.add(cpus(sharedCpus, "*"))
      if (roleCpus > 0) resources.add(cpus(roleCpus, role))

      if (sharedMem > 0) resources.add(mem(sharedMem, "*"))
      if (roleMem > 0) resources.add(mem(roleMem, role))

      if (sharedPort != -1) resources.add(port(sharedPort, "*"))
      if (rolePort != -1) resources.add(port(rolePort, role))

      if (volume != null) resources.add(volumeDisk(volume, volumeSize, role, volumePrincipal))
      resources
    }
  }

  class Metrics {
    var underReplicatedPartitions: Int = 0
    var offlinePartitionsCount: Int = 0
    var activeControllerCount: Int = 0

    var timestamp: Long = 0

    def fromJson(node: Map[String, Object]): Unit = {
      underReplicatedPartitions = node("underReplicatedPartitions").asInstanceOf[Number].intValue()
      offlinePartitionsCount = node("offlinePartitionsCount").asInstanceOf[Number].intValue()
      activeControllerCount = node("activeControllerCount").asInstanceOf[Number].intValue()

      timestamp = node("timestamp").asInstanceOf[Number].longValue()
    }

    def toJson: JSONObject = {
      val obj = new collection.mutable.LinkedHashMap[String, Any]()

      obj("underReplicatedPartitions") = underReplicatedPartitions
      obj("offlinePartitionsCount") = offlinePartitionsCount
      obj("activeControllerCount") = activeControllerCount

      obj("timestamp") = timestamp

      new JSONObject(obj.toMap)
    }
  }

  object State {
    val STOPPED = "stopped"
    val STARTING = "starting"
    val RUNNING = "running"
    val RECONCILING = "reconciling"
    val STOPPING = "stopping"
  }

  type OtherAttributes = (String) => Array[String]
  def NoAttributes: OtherAttributes = _ => Array()

  private def dateTimeFormat: SimpleDateFormat = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    format
  }
}