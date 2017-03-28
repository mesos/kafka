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
package ly.stealth.mesos.kafka.cli

import java.io.IOException
import java.util
import java.util.{Collections, Date}
import ly.stealth.mesos.kafka.cli.Cli.Error
import ly.stealth.mesos.kafka.{Broker, BrokerRemoveResponse, BrokerStartResponse, BrokerStatusResponse, HttpLogResponse}
import ly.stealth.mesos.kafka.json.JsonUtil
import net.elodina.mesos.util.{Repr, Strings}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait BrokerCli {
  this: CliUtils =>

  val brokerCli: CliHandler = new BrokerCliImpl

  class BrokerCliImpl extends CliHandler {
    def handle(cmd: String, _args: Array[String], help: Boolean = false): Unit = {
      var args = _args

      if (help) {
        handleHelp(cmd)
        return
      }

      var arg: String = null
      if (args.length > 0 && !args(0).startsWith("-")) {
        arg = args(0)
        args = args.slice(1, args.length)
      }

      if (arg == null && cmd != "list") {
        handleHelp(cmd); printLine()
        throw new Error("argument required")
      }

      cmd match {
        case "list" => handleList(arg, args)
        case "metrics" => handleMetrics(arg, args)
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case "remove" => handleRemove(arg)
        case "clone" => handleClone(arg, args)
        case "start" | "stop" => handleStartStop(arg, args, cmd == "start")
        case "restart" => handleRestart(arg, args)
        case "log" => handleLog(arg, args)
        case _ => throw new Error("unsupported broker command " + cmd)
      }
    }

    private def handleHelp(cmd: String): Unit = {
      cmd match {
        case null =>
          printLine("Broker management commands\nUsage: broker <command>\n")
          printCmds()

          printLine()
          printLine("Run `help broker <command>` to see details of specific command")
        case "list" =>
          handleList(null, null, help = true)
        case "metrics" =>
          handleMetrics(null, null, help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case "remove" =>
          handleRemove(null, help = true)
        case "clone" =>
          handleClone(null, null, help = true)
        case "start" | "stop" =>
          handleStartStop(null, null, cmd == "start", help = true)
        case "restart" =>
          handleRestart(null, null, help = true)
        case "log" =>
          handleLog(null, null, help = true)
        case _ =>
          throw new Error(s"unsupported broker command $cmd")
      }
    }

    private def getBrokers(expr: String): Seq[Broker] = {
      val params = new util.HashMap[String, String]()
      if (expr != null) params.put("broker", expr)

      try { sendRequestObj[BrokerStatusResponse]("/broker/list", params).brokers }
      catch { case e: IOException => throw new Error("" + e) }
    }

    private def handleList(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      val quietSpec = parser.accepts("quiet").withOptionalArg().ofType(classOf[Boolean])

      if (help) {
        printLine("List brokers\nUsage: broker list [<broker-expr>] [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val brokers = getBrokers(expr)
      val title = if (brokers.isEmpty) "no brokers" else "broker" + (if (brokers.size > 1) "s" else "") + ":"
      printLine(title)

      val options = parseOptions(parser, args)
      val quiet = options.has(quietSpec)
      for (broker <- brokers) {
        printBroker(broker, 1, quiet)
        printLine()
      }
    }

    private def handleMetrics(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      val filterSpec = parser.accepts("filter", "metric filter regex").withOptionalArg().ofType(classOf[String])
      val jsonSpec = parser.accepts("json", "output json").withOptionalArg().ofType(classOf[Boolean])

      if (help) {
        printLine("Show broker metrics\nUsage: broker metrics <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val options = parseOptions(parser, args)
      val filterRegex = if (options.has(filterSpec)) {
        Some(options.valuesOf[String](filterSpec).asScala)
      } else {
        None
      }

      val brokers = getBrokers(expr)
      if (options.has(jsonSpec)) {
        out.println(JsonUtil.toJson(brokers.map(b => b.id -> b.metrics).toMap))
      } else {
        for (b <- brokers.sortBy(_.id)) {
          val brokerUpdatedOn = new Date(b.metrics.timestamp)
          out.println(s"broker ${b.id} (updated $brokerUpdatedOn)")
          val metrics = b.metrics.data.toSeq.filter({
            case (name, value) =>
              filterRegex match {
                case Some(r) => r.forall(name.matches)
                case None => true
              }
          }).sortBy(_._1)
          for ((name, value) <- metrics) {
            out.println(s"   $name => $value")
          }
        }
      }
    }

    private def handleAddUpdate(expr: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("cpus", "cpu amount (0.5, 1, 2)").withRequiredArg().ofType(classOf[java.lang.Double])
      parser.accepts("mem", "mem amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])
      parser.accepts("heap", "heap amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])
      parser.accepts("port", "port or range (31092, 31090..31100). Default - auto").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("volume", "pre-reserved persistent volume id").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("bind-address", "broker bind address (broker0, 192.168.50.*, if:eth1). Default - auto").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("syslog", "enable syslog logging. Default - false").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("stickiness-period", "stickiness period to preserve same node for broker (5m, 10m, 1h)").withRequiredArg().ofType(classOf[String])

      parser.accepts("options", "options or file. Examples:\n log.dirs=/tmp/kafka/$id,num.io.threads=16\n file:server.properties").withRequiredArg()
      parser.accepts("log4j-options", "log4j options or file. Examples:\n log4j.logger.kafka=DEBUG\\, kafkaAppender\n file:log4j.properties").withRequiredArg()
      parser.accepts("jvm-options", "jvm options string (-Xms128m -XX:PermSize=48m)").withRequiredArg()
      parser.accepts("constraints", "constraints (hostname=like:master,rack=like:1.*). See below.").withRequiredArg()

      parser.accepts("failover-delay", "failover delay (10s, 5m, 3h)").withRequiredArg().ofType(classOf[String])
      parser.accepts("failover-max-delay", "max failover delay. See failoverDelay.").withRequiredArg().ofType(classOf[String])
      parser.accepts("failover-max-tries", "max failover tries. Default - none").withRequiredArg().ofType(classOf[String])
      val javaCmdArg = parser.accepts("java-cmd", "command to launch java.  Default - java").withRequiredArg().ofType(classOf[String])
      val containerTypeArg = parser.accepts("container-type", "the type of container the broker should be launched in. \nMaybe be either mesos or docker").withRequiredArg().ofType(classOf[String])
      val containerImageArg = parser.accepts("container-image", "the container image to launch the broker in (if any)" ).withRequiredArg().ofType(classOf[String])
      val containerMountArg = parser.accepts("container-mounts", "a comma separated list of host:container[:mode][,...] tuples to mount into the launched container.").withRequiredArg().ofType(classOf[String])

      if (help) {
        val cmd = if (add) "add" else "update"
        printLine(s"${cmd.capitalize} broker\nUsage: broker $cmd <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()

        printLine()
        printConstraintExamples()

        if (!add) printLine("\nNote: use \"\" arg to unset an option")
        return
      }

      val options = parseOptions(parser, args)
      val cpus = options.valueOf("cpus").asInstanceOf[java.lang.Double]
      val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
      val heap = options.valueOf("heap").asInstanceOf[java.lang.Long]
      val port = options.valueOf("port").asInstanceOf[String]
      val volume = options.valueOf("volume").asInstanceOf[String]
      val bindAddress = options.valueOf("bind-address").asInstanceOf[String]
      val syslog = options.valueOf("syslog").asInstanceOf[String]
      val stickinessPeriod = options.valueOf("stickiness-period").asInstanceOf[String]

      val constraints = options.valueOf("constraints").asInstanceOf[String]
      val options_ = options.valueOf("options").asInstanceOf[String]
      val log4jOptions = options.valueOf("log4j-options").asInstanceOf[String]
      val jvmOptions = options.valueOf("jvm-options").asInstanceOf[String]

      val failoverDelay = options.valueOf("failover-delay").asInstanceOf[String]
      val failoverMaxDelay = options.valueOf("failover-max-delay").asInstanceOf[String]
      val failoverMaxTries = options.valueOf("failover-max-tries").asInstanceOf[String]

      val javaCmd = options.valueOf(javaCmdArg)
      val containerType = options.valueOf(containerTypeArg)
      val containerImage = options.valueOf(containerImageArg)
      val containerMounts = options.valueOf(containerMountArg)

      val params = new util.LinkedHashMap[String, String]
      params.put("broker", expr)

      if (cpus != null) params.put("cpus", "" + cpus)
      if (mem != null) params.put("mem", "" + mem)
      if (heap != null) params.put("heap", "" + heap)
      if (port != null) params.put("port", port)
      if (volume != null) params.put("volume", volume)
      if (bindAddress != null) params.put("bindAddress", bindAddress)
      if (syslog != null) params.put("syslog", syslog)
      if (stickinessPeriod != null) params.put("stickinessPeriod", stickinessPeriod)

      if (options_ != null) params.put("options", optionsOrFile(options_))
      if (constraints != null) params.put("constraints", constraints)
      if (log4jOptions != null) params.put("log4jOptions", optionsOrFile(log4jOptions))
      if (jvmOptions != null) params.put("jvmOptions", jvmOptions)

      if (failoverDelay != null) params.put("failoverDelay", failoverDelay)
      if (failoverMaxDelay != null) params.put("failoverMaxDelay", failoverMaxDelay)
      if (failoverMaxTries != null) params.put("failoverMaxTries", failoverMaxTries)

      if (javaCmdArg != null) params.put("javaCmd", javaCmd)
      if (containerType != null) params.put("containerType", containerType)
      if (containerImage != null) params.put("containerImage", containerImage)
      if (containerMounts != null) params.put("containerMounts", containerMounts)

      val brokers =
        try { sendRequestObj[BrokerStatusResponse]("/broker/" + (if (add) "add" else "update"), params).brokers }
        catch { case e: IOException => throw new Error("" + e) }

      val addedUpdated = if (add) "added" else "updated"
      val numBrokers = "broker" + (if (brokers.length > 1) "s" else "")

      printLine(s"$numBrokers $addedUpdated:")
      for (broker <- brokers) {
        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleRemove(expr: String, help: Boolean = false): Unit = {
      if (help) {
        printLine("Remove broker\nUsage: broker remove <broker-expr> [options]\n")
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val json =
        try { sendRequestObj[BrokerRemoveResponse]("/broker/remove", Collections.singletonMap("broker", expr)) }
        catch { case e: IOException => throw new Error("" + e) }

      val ids = json.ids
      val brokers = "broker" + (if (ids.length > 1) "s" else "")

      printLine(s"$brokers ${ids.mkString(",")} removed")
    }

    private def handleClone(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("source", "source broker id to copy settings from").withRequiredArg().ofType(classOf[String])

      if (help) {
        printLine("Clone broker\nUsage: broker clone <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val options = parseOptions(parser, args)
      val sourceBrokerId = options.valueOf("source").asInstanceOf[String]

      val brokers =
        try { sendRequestObj[BrokerStatusResponse]("/broker/clone", Map("broker" -> expr, "source" -> sourceBrokerId)).brokers }
        catch { case e: IOException => throw new Error("" + e) }
      val numBrokers = "broker" + (if (brokers.length > 1) "s" else "")

      printLine(s"$numBrokers added:")
      for (broker <- brokers) {
        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleStartStop(expr: String, args: Array[String], start: Boolean, help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])
      if (!start) parser.accepts("force", "forcibly stop").withOptionalArg().ofType(classOf[String])

      if (help) {
        val cmd = if (start) "start" else "stop"
        printLine(s"${cmd.capitalize} broker\nUsage: broker $cmd <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val options = parseOptions(parser, args)
      val cmd: String = if (start) "start" else "stop"
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val force: Boolean = options.has("force")

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", expr)
      if (timeout != null) params.put("timeout", timeout)
      if (force) params.put("force", null)

      val ret =
        try { sendRequestObj[BrokerStartResponse]("/broker/" + cmd, params) }
        catch { case e: IOException => throw new Error("" + e) }

      val status = ret.status
      val brokers = ret.brokers

      val numBrokers = "broker" + (if (brokers.size > 1) "s" else "")
      val startStop = if (start) "start" else "stop"

      // started|stopped|scheduled|timeout
      if (status == "timeout") throw new Error(s"$numBrokers $startStop timeout")
      else if (status == "scheduled") printLine(s"$numBrokers scheduled to $startStop:")
      else printLine(s"$numBrokers $status:")

      for (broker <- brokers) {
        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleRestart(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("timeout", "time to wait until broker restarts (30s, 1m, 1h). Default - 2m").withRequiredArg().ofType(classOf[String])
      parser.accepts("noWaitForReplication", "don't wait for replication to catch up before proceeding to the next broker").withOptionalArg().ofType(classOf[Boolean])

      if (help) {
        printLine(s"Restart broker\nUsage: broker restart <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printBrokerExprExamples()
        return
      }

      val options = parseOptions(parser, args)
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", expr)
      if (timeout != null) params.put("timeout", timeout)
      if (options.has("noWaitForReplication")) params.put("noWaitForReplication", "true")

      val ret =
        try { sendRequestObj[BrokerStartResponse]("/broker/restart", params) }
        catch { case e: IOException => throw new Error("" + e) }

      val status = ret.status

      // restarted|timeout
      if (status == "timeout") throw new Error(ret.message.getOrElse("Unknown error"))

      val brokers = ret.brokers
      val numBrokers = "broker" + (if (brokers.size > 1) "s" else "")
      printLine(s"$numBrokers $status:")

      for (broker <- brokers) {
        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleLog(brokerId: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("timeout", "timeout (30s, 1m, 1h). Default - 30s").withRequiredArg().ofType(classOf[String])
      parser.accepts("name", "name of log file (stdout, stderr, server.log). Default - stdout").withRequiredArg().ofType(classOf[String])
      parser.accepts("lines", "maximum number of lines to read from the end of file. Default - 100").withRequiredArg().ofType(classOf[Integer])

      if (help) {
        printLine(s"Retrieve broker log\nUsage: broker log <broker-id> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        return
      }

      val options = parseOptions(parser, args)
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val name: String = options.valueOf("name").asInstanceOf[String]
      val lines: Integer = options.valueOf("lines").asInstanceOf[java.lang.Integer]

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", brokerId)
      if (timeout != null) params.put("timeout", timeout)
      if (name != null) params.put("name", name)
      if (lines != null) params.put("lines", "" + lines)

      val json =
        try { sendRequestObj[HttpLogResponse]("/broker/log", params) }
        catch { case e: IOException => throw new Error("" + e) }

      val status = json.status
      val content = json.content

      if (status == "timeout") throw new Error(s"broker $brokerId log retrieve timeout")
      else if (status == "failure") throw new Error(s"broker $brokerId failed")
      else printLine(content)

      if (!content.isEmpty && content.last != '\n') printLine()
    }

    private def printCmds(): Unit = {
      printLine("Commands:")
      printLine("list       - list brokers", 1)
      printLine("metrics    - dump broker metrics", 1)
      printLine("add        - add broker", 1)
      printLine("update     - update broker", 1)
      printLine("remove     - remove broker", 1)
      printLine("clone      - clone broker", 1)
      printLine("start      - start broker", 1)
      printLine("stop       - stop broker", 1)
      printLine("restart    - restart broker", 1)
      printLine("log        - retrieve broker log", 1)
    }

    private def printBroker(broker: Broker, indent: Int, quiet: Boolean = false): Unit = {
      printLine("id: " + broker.id, indent)
      printLine("active: " + broker.active, indent)
      printLine("state: " + broker.state() + (if (broker.needsRestart) " (modified, needs restart)" else ""), indent)
      printLine("resources: " + brokerResources(broker), indent)

      if (broker.bindAddress != null) printLine("bind-address: " + broker.bindAddress, indent)
      if (broker.syslog) printLine("syslog: " + broker.syslog, indent)
      if (broker.constraints.nonEmpty) printLine("constraints: " + Strings.formatMap(broker.constraints), indent)
      if (broker.executionOptions.javaCmd != "exec java") printLine("java-cmd: " + broker.executionOptions.javaCmd, indent)
      broker.executionOptions.container.foreach { i =>
        printLine(s"image: ${i.name} [${i.ctype.toString}]", indent)
        printLine("mounts:", indent)
        i.mounts.foreach { m =>
          printLine(s"${m.hostPath} => ${m.containerPath} [${m.mode}]", indent + 1)
        }
      }

      if (!quiet) {
        if (broker.options.nonEmpty) printLine("options: " + Strings.formatMap(broker.options), indent)
        if (broker.log4jOptions.nonEmpty) printLine("log4j-options: " + Strings.formatMap(broker.log4jOptions), indent)
        if (broker.executionOptions.jvmOptions != null) printLine("jvm-options: " + broker.executionOptions.jvmOptions, indent)
      }
      var failover = "failover:"
      failover += " delay:" + broker.failover.delay
      failover += ", max-delay:" + broker.failover.maxDelay
      if (broker.failover.maxTries != null) failover += ", max-tries:" + broker.failover.maxTries
      printLine(failover, indent)

      var stickiness = "stickiness:"
      stickiness += " period:" + broker.stickiness.period
      if (broker.stickiness.hostname != null) stickiness += ", hostname:" + broker.stickiness.hostname
      if (broker.stickiness.stopTime != null) stickiness += ", expires:" + Repr.dateTime(broker.stickiness.expires)
      printLine(stickiness, indent)

      val task = broker.task
      if (task != null) {
        printLine("task: ", indent)
        printLine("id: " + broker.task.id, indent + 1)
        printLine("state: " + task.state, indent + 1)
        if (task.endpoint != null) printLine("endpoint: " + task.endpoint + (if (broker.bindAddress != null) " (" + task.hostname + ")" else ""), indent + 1)
        if (task.attributes.nonEmpty) printLine("attributes: " + Strings.formatMap(task.attributes), indent + 1)
      }

      val metrics = broker.metrics
      if (metrics != null) {
        printLine("metrics: ", indent)
        printLine("collected: " + Repr.dateTime(new Date(metrics.timestamp)), indent + 1)
        printLine("under-replicated-partitions: " +
          metrics("kafka.server,ReplicaManager,UnderReplicatedPartitions")
            .orElse(metrics("underReplicatedPartitions"))
            .getOrElse(0),
          indent + 1)
        printLine("offline-partitions-count: " +
          metrics("kafka.controller,KafkaController,OfflinePartitionsCount")
            .orElse(metrics("offlinePartitionsCount"))
            .getOrElse(0),
          indent + 1)
        printLine("is-active-controller: " +
          metrics("kafka.controller,KafkaController,ActiveControllerCount")
            .orElse(metrics("activeControllerCount"))
            .getOrElse(0),
          indent + 1)
      } else if (broker.state() == Broker.State.RUNNING) {
        // No metrics yet, but the broker is running so we should recieve them soon.
        printLine("metrics: ", indent)
        printLine("waiting for broker to report metrics", indent + 1)
      }
    }

    private def brokerResources(broker: Broker): String = {
      var s: String = ""

      s += "cpus:" + "%.2f".format(broker.cpus)
      s += ", mem:" + broker.mem
      s += ", heap:" + broker.heap
      s += ", port:" + (if (broker.port != null) broker.port else "auto")
      if (broker.volume != null) s += ", volume:" + broker.volume

      s
    }

    private def printConstraintExamples(): Unit = {
      printLine("constraint examples:")
      printLine("like:master     - value equals 'master'", 1)
      printLine("unlike:master   - value not equals 'master'", 1)
      printLine("like:slave.*    - value starts with 'slave'", 1)
      printLine("unique          - all values are unique", 1)
      printLine("cluster         - all values are the same", 1)
      printLine("cluster:master  - value equals 'master'", 1)
      printLine("groupBy         - all values are the same", 1)
      printLine("groupBy:3       - all values are within 3 different groups", 1)
    }
  }


}
