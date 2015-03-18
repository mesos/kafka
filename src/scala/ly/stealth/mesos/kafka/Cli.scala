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

import joptsimple.{OptionException, OptionSet, OptionParser}
import java.net.{HttpURLConnection, URLEncoder, URL}
import scala.io.Source
import java.io.{PrintStream, IOException}
import java.util
import scala.collection.JavaConversions._
import java.util.Collections

object Cli {
  var out: PrintStream = System.out
  var err: PrintStream = System.err

  def main(args: Array[String]): Unit = {
    try { exec(args) }
    catch { case e: Error =>
      err.println("Error: " + e.getMessage)
      System.exit(1)
    }
  }
  
  def exec(_args: Array[String]): Unit = {
    var args = _args

    if (args.length == 0) {
      handleHelp()
      throw new Error("command required")
    }

    val command = args(0)
    args = args.slice(1, args.length)
    
    if (command == "help") { handleHelp(if (args.length > 0) args(0) else null); return }
    if (command == "scheduler") { Scheduler.main(args); return }
    if (command == "status") { handleStatus(); return }

    // rest of the commands require <argument>
    if (args.length < 1) throw new Error("argument required")
    val arg = args(0)
    args = args.slice(1, args.length)

    command match {
      case "add" | "update" => handleAddUpdateBroker(arg, args, command == "add")
      case "remove" => handleRemoveBroker(arg)
      case "start" | "stop" => handleStartStopBroker(arg, args, command == "start")
      case "rebalance" => handleRebalance(arg, args)
      case _ => throw new Error("unsupported command " + command)
    }
  }

  private def handleHelp(command: String = null): Unit = {
    command match {
      case null =>
        out.println("Usage: {help {command}|scheduler|status|add|update|remove|start|stop|rebalance}")
      case "help" =>
        out.println("Print general or command-specific help\nUsage: help {command}")
      case "scheduler" =>
        out.println("Start scheduler process\nUsage: scheduler")
      case "status" =>
        handleStatus(help = true)
      case "add" | "update" => 
        handleAddUpdateBroker(null, null, command == "add", help = true)
      case "remove" =>
        handleRemoveBroker(null, help = true)
      case "start" | "stop" =>
        handleStartStopBroker(null, null, command == "start", help = true)
      case "rebalance" =>
        handleRebalance(null, null, help = true)
      case _ =>
        throw new Error("unsupported command " + command)
    }
  }

  private def handleStatus(help: Boolean = false): Unit = {
    if (help) {
      out.println("Print cluster status\nUsage: status")
      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/status", Collections.emptyMap()) }
    catch { case e: IOException => throw new Error("" + e) }
    
    val cluster: Cluster = new Cluster()
    cluster.fromJson(json)

    printLine("Cluster status received\n")
    printLine("cluster:")
    printCluster(cluster)
  }

  private def handleAddUpdateBroker(id: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("host", "slave hostname").withRequiredArg()
    parser.accepts("cpus", "cpu amount (0.5, 1, 2)").withRequiredArg().ofType(classOf[java.lang.Double])
    parser.accepts("mem", "mem amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])
    parser.accepts("heap", "heap amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])

    parser.accepts("options", "kafka options (a=1;b=2)").withRequiredArg()
    parser.accepts("attributes", "slave attributes (rack:1;role:master)").withRequiredArg()

    parser.accepts("failoverDelay", "failover delay (10s, 5m, 3h)").withRequiredArg().ofType(classOf[String])
    parser.accepts("failoverMaxDelay", "max failover delay. See failoverDelay.").withRequiredArg().ofType(classOf[String])
    parser.accepts("failoverMaxTries", "max failover tries").withRequiredArg().ofType(classOf[String])

    if (help) {
      val command = if (add) "add" else "update"
      out.println(s"${command.capitalize} broker\nUsage: $command <id-expr>\n")
      printIdExprExamples()
      parser.printHelpOn(out)
      if (!add) out.println("\nNote: use \"\" arg to unset the option")
      return
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        out.println()
        throw new Error(e.getMessage)
    }

    val host = options.valueOf("host").asInstanceOf[String]
    val cpus = options.valueOf("cpus").asInstanceOf[java.lang.Double]
    val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
    val heap = options.valueOf("heap").asInstanceOf[java.lang.Long]

    val options_ = options.valueOf("options").asInstanceOf[String]
    val attributes = options.valueOf("attributes").asInstanceOf[String]

    val failoverDelay = options.valueOf("failoverDelay").asInstanceOf[String]
    val failoverMaxDelay = options.valueOf("failoverMaxDelay").asInstanceOf[String]
    val failoverMaxTries = options.valueOf("failoverMaxTries").asInstanceOf[String]

    val params = new util.LinkedHashMap[String, String]
    params.put("id", id)
    if (host != null) params.put("host", host)
    if (cpus != null) params.put("cpus", "" + cpus)
    if (mem != null) params.put("mem", "" + mem)
    if (heap != null) params.put("heap", "" + heap)

    if (options_ != null) params.put("options", options_)
    if (attributes != null) params.put("attributes", attributes)

    if (failoverDelay != null) params.put("failoverDelay", failoverDelay)
    if (failoverMaxDelay != null) params.put("failoverMaxDelay", failoverMaxDelay)
    if (failoverMaxTries != null) params.put("failoverMaxTries", failoverMaxTries)

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/" + (if (add) "add" else "update"), params) }
    catch { case e: IOException => throw new Error("" + e) }
    val brokerNodes: List[Map[String, Object]] = json("brokers").asInstanceOf[List[Map[String, Object]]]

    val addedUpdated = if (add) "added" else "updated"
    val brokers = "broker" + (if (brokerNodes.length > 1) "s" else "")

    printLine(s"${brokers.capitalize} $addedUpdated\n")
    printLine(s"$brokers:")
    for (brokerNode <- brokerNodes) {
      val broker: Broker = new Broker()
      broker.fromJson(brokerNode)

      printBroker(broker, 1)
      printLine()
    }
  }

  private def handleRemoveBroker(id: String, help: Boolean = false): Unit = {
    if (help) {
      out.println("Remove broker\nUsage: remove <id-expr>\n")
      printIdExprExamples()
      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/remove", Collections.singletonMap("id", id)) }
    catch { case e: IOException => throw new Error("" + e) }

    val ids = json("ids").asInstanceOf[String]
    val brokers = "Broker" + (if (ids.contains(",")) "s" else "")

    printLine(s"$brokers $ids removed")
  }

  private def handleStartStopBroker(id: String, args: Array[String], start: Boolean, help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])

    if (help) {
      val command = if (start) "start" else "stop"
      out.println(s"${command.capitalize} broker\nUsage: $command <id-expr>\n")
      printIdExprExamples()
      parser.printHelpOn(out)
      return 
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        out.println()
        throw new Error(e.getMessage)
    }

    val command: String = if (start) "start" else "stop"
    val timeout: String = options.valueOf("timeout").asInstanceOf[String]

    val params = new util.LinkedHashMap[String, String]()
    params.put("id", id)
    if (timeout != null) params.put("timeout", timeout)

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/" + command, params) }
    catch { case e: IOException => throw new Error("" + e) }

    val status = json("status").asInstanceOf[String]
    val ids = json("ids").asInstanceOf[String]
    
    val brokers = "Broker" + (if (ids.contains(",")) "s" else "")
    val startStop = if (start) "start" else "stop"

    // started|stopped|scheduled|timeout
    if (status == "timeout") throw new Error(s"$brokers $ids scheduled to $startStop. Got timeout")
    else if (status == "scheduled") printLine(s"$brokers $ids scheduled to $startStop")
    else printLine(s"$brokers $ids $status")
  }

  private def handleRebalance(arg: String, args: Array[String], help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])

    if (help) {
      out.println("Rebalance\nUsage: rebalance <id-expr>|status\n")
      printIdExprExamples()
      parser.printHelpOn(out)
      return
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        out.println()
        throw new Error(e.getMessage)
    }

    val timeout: String = options.valueOf("timeout").asInstanceOf[String]

    val params = new util.LinkedHashMap[String, String]()
    if (arg != "status") params.put("id", arg)
    if (timeout != null) params.put("timeout", timeout)

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/rebalance", params) }
    catch { case e: IOException => throw new Error("" + e) }

    val status = json("status").asInstanceOf[String]
    printLine(s"Rebalance $status")

    val state: String = json("state").asInstanceOf[String]
    if (!state.isEmpty) printLine("\nReassignments:\n" + state)
  }

  private def printCluster(cluster: Cluster): Unit = {
    printLine("brokers:", 1)
    for (broker <- cluster.getBrokers) {
      printBroker(broker, 2)
      printLine()
    }
  }

  private def printBroker(broker: Broker, indent: Int): Unit = {
    printLine("id: " + broker.id, indent)
    printLine("active: " + broker.active, indent)
    printLine("state: " + broker.state(), indent)

    if (broker.host != null) printLine("host: " + broker.host, indent)
    printLine("resources: " + "cpus:" + "%.2f".format(broker.cpus) + ", mem:" + broker.mem + ", heap:" + broker.heap, indent)

    if (broker.attributes != null) printLine("attributes: " + broker.attributes, indent)
    if (broker.options != null) printLine("options: " + broker.options, indent)

    var failover = "failover:"
    failover += " delay:" + broker.failover.delay
    failover += ", maxDelay:" + broker.failover.maxDelay
    if (broker.failover.maxTries != null) failover += ", maxTries:" + broker.failover.maxTries
    printLine(failover, indent)

    val task = broker.task
    if (task != null) {
      printLine("task: ", indent)
      printLine("id: " + broker.task.id, indent + 1)
      printLine("running: " + task.running, indent + 1)
      printLine("endpoint: " + task.endpoint, indent + 1)
    }
  }

  private def printIdExprExamples(): Unit = {
    printLine("id-expr examples:")
    printLine("0      - broker 0", 1)
    printLine("0,1    - brokers 0,1", 1)
    printLine("0..2   - brokers 0,1,2", 1)
    printLine("0,1..2 - brokers 0,1,2", 1)
    printLine("*      - any broker", 1)
    printLine()
  }

  private def printLine(s: Object = "", indent: Int = 0): Unit = out.println("  " * indent + s)

  private[kafka] def sendRequest(uri: String, params: util.Map[String, String]): Map[String, Object] = {
    def queryString(params: util.Map[String, String]): String = {
      var s = ""
      for ((name, value) <- params) {
        if (!s.isEmpty) s += "&"
        s += URLEncoder.encode(name, "utf-8")
        if (value != null) s += "=" + URLEncoder.encode(value, "utf-8")
      }
      s
    }

    val qs: String = queryString(params)
    val url: String = Config.schedulerUrl + "/api" + uri + "?" + qs

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      try { response = Source.fromInputStream(connection.getInputStream).getLines().mkString}
      catch {
        case e: IOException =>
          if (connection.getResponseCode != 200) throw new IOException(connection.getResponseCode + " - " + connection.getResponseMessage)
          else throw e
      }
    } finally {
      connection.disconnect()
    }

    if (response.trim().isEmpty) return null

    var node: Map[String, Object] = null
    try { node = Util.parseJson(response)}
    catch { case e: IllegalArgumentException => throw new IOException(e) }

    node
  }

  class Error(message: String) extends java.lang.Error(message) {}
}
