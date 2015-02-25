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
import java.io.IOException
import java.util
import scala.collection.JavaConversions._
import java.util.Collections
import scala.util.parsing.json.JSON

object CLI {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      printHelp()
      System.exit(1)
    }

    val command = args(0)
    if (command == "help") { printHelp(if (args.length > 1) args(1) else null); return }
    if (command == "scheduler") { Scheduler.main(args); return }
    if (command == "status") { handleStatus(); return }

    // rest of the commands require <id>
    if (args.length < 2) die("id required")
    val id = args(1)
    val a = args.slice(2, args.length)

    command match {
      case "add" | "update" => handleAddUpdateBroker(id, a, command == "add")
      case "remove" => handleRemoveBroker(id)
      case "start" | "stop" => handleStartStopBroker(id, a, command == "start")
      case _ => die("unsupported command: " + command)
    }
  }

  private def printHelp(command: String = null): Unit = {
    command match {
      case null =>
        System.out.println("Usage: {help {command}|scheduler|status|add|update|remove|start|stop}")
      case "help" =>
        System.out.println("Print general or command-specific help\nUsage: help {command}")
      case "scheduler" =>
        System.out.println("Start scheduler process\nUsage: scheduler")
      case "status" =>
        handleStatus(help = true)
      case "add" | "update" => 
        handleAddUpdateBroker(null, null, command == "add", help = true)
      case "remove" =>
        handleRemoveBroker(null, help = true)
      case "start" | "stop" =>
        handleStartStopBroker(null, null, command == "start", help = true)
      case _ =>
        die("unsupported command " + command)
    }
  }

  private def handleStatus(help: Boolean = false): Unit = {
    if (help) {
      System.out.println("Print cluster status\nUsage: status")
      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/status", Collections.emptyMap()) }
    catch { case e: IOException => die(e.getMessage) }
    
    val cluster: Cluster = new Cluster()
    cluster.fromJson(json)

    printLine("Cluster status received\n")
    printLine("cluster:")
    printCluster(cluster)
  }

  private def handleAddUpdateBroker(id: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("host", "slave hostname").withRequiredArg()
    parser.accepts("cpus", "cpu amount").withRequiredArg().ofType(classOf[java.lang.Double])
    parser.accepts("mem", "mem amount").withRequiredArg().ofType(classOf[java.lang.Long])
    parser.accepts("options", "kafka options (a=1;b=2)").withRequiredArg()
    parser.accepts("attributes", "slave attributes (rack:1;role:master)").withRequiredArg()

    if (help) {
      val command = if (add) "add" else "update"
      System.out.println(s"${command.capitalize} broker\nUsage: $command <broker-id-expression>\n")
      printBrokerIdExpressions()
      parser.printHelpOn(System.out)
      if (!add) System.out.println("\nNote: use \"\" arg to unset the option")
      return
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        System.err.println("Error: " + e.getMessage)
        System.out.println()
        parser.printHelpOn(System.out)
        System.exit(1)
    }

    val host = options.valueOf("host").asInstanceOf[String]
    val cpus = options.valueOf("cpus").asInstanceOf[java.lang.Double]
    val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
    val options_ = options.valueOf("options").asInstanceOf[String]
    val attributes = options.valueOf("attributes").asInstanceOf[String]

    val params = new util.LinkedHashMap[String, String]
    params.put("id", id)
    if (host != null) params.put("host", host)
    if (cpus != null) params.put("cpus", "" + cpus)
    if (mem != null) params.put("mem", "" + mem)
    if (options_ != null) params.put("options", options_)
    if (attributes != null) params.put("attributes", attributes)

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/" + (if (add) "add" else "update"), params) }
    catch { case e: IOException => die(e.getMessage) }
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
      System.out.println("Remove broker\nUsage: remove <broker-id-expression>\n")
      printBrokerIdExpressions()
      return
    }

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/remove", Collections.singletonMap("id", id)) }
    catch { case e: IOException => die(e.getMessage) }

    val ids = json("ids").asInstanceOf[String]
    val brokers = "Broker" + (if (ids.contains(",")) "s" else "")

    printLine(s"$brokers $ids removed")
  }

  private def handleStartStopBroker(id: String, args: Array[String], start: Boolean, help: Boolean = false): Unit = {
    val parser = new OptionParser()
    parser.accepts("timeout", "timeout in seconds. 0 - for no timeout").withRequiredArg().ofType(classOf[Integer]).defaultsTo(30)

    if (help) {
      val command = if (start) "start" else "stop"
      System.out.println(s"${command.capitalize} broker\nUsage: $command <broker-id-expression>\n")
      printBrokerIdExpressions()
      parser.printHelpOn(System.out)
      return 
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        System.err.println("Error: " + e.getMessage)
        System.out.println()
        parser.printHelpOn(System.out)
        System.exit(1)
    }

    val command: String = if (start) "start" else "stop"
    val timeout: java.lang.Integer = options.valueOf("timeout").asInstanceOf[Integer]

    val params = new util.LinkedHashMap[String, String]()
    params.put("id", id)
    params.put("timeout", "" + timeout * 1000)

    var json: Map[String, Object] = null
    try { json = sendRequest("/brokers/" + command, params) }
    catch { case e: IOException => die(e.getMessage) }

    val success = json("success").asInstanceOf[Boolean]
    val ids = json("ids").asInstanceOf[String]
    
    val brokers = "Broker" + (if (ids.contains(",")) "s" else "")
    val startStop = if (start) "start" else "stop" 
    val startStopped = if (start) "started" else "stopped"

    if (success) printLine(s"$brokers $ids $startStopped")
    else if (timeout == 0) printLine(s"$brokers $ids scheduled to $startStop")
    else die(s"$brokers $ids scheduled to $startStop. Timeout limit exceeded")
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
    printLine("started: " + broker.started, indent)

    if (broker.host != null) printLine("host: " + broker.host, indent)
    printLine("resources: " + "cpus:" + "%.2f".format(broker.cpus) + ", mem:" + broker.mem, indent)

    if (broker.attributes != null) printLine("attributes: " + broker.attributes, indent)
    if (broker.options != null) printLine("options: " + broker.options, indent)

    val task = broker.task
    if (task != null) {
      printLine("task: ", indent)
      printLine("id: " + broker.task.id, indent + 1)
      printLine("running: " + task.running, indent + 1)
      printLine("endpoint: " + task.endpoint, indent + 1)
    }
  }

  private def printBrokerIdExpressions(): Unit = {
    printLine("Expression examples:")
    printLine("0      - broker 0", 1)
    printLine("0,1    - brokers 0,1", 1)
    printLine("0..2   - brokers 0,1,2", 1)
    printLine("0,1..2 - brokers 0,1,2", 1)
    printLine("\"*\"    - any broker", 1)
    printLine()
  }

  private def printLine(s: Object = "", indent: Int = 0): Unit = println("  " * indent + s)

  private def sendRequest(uri: String, params: util.Map[String, String]): Map[String, Object] = {
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

    val node: Map[String, Object] = JSON.parseFull(response).getOrElse(null).asInstanceOf[Map[String, Object]]
    if (node == null) throw new IOException("Failed to parse json response: " + response)

    node
  }

  private def die(error: String): Unit = {
    System.err.println("Error: " + error)
    //System.exit(1)
  }
}
