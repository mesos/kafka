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

import joptsimple.{BuiltinHelpFormatter, OptionException, OptionSet, OptionParser}
import java.net.{HttpURLConnection, URLEncoder, URL}
import scala.io.Source
import java.io._
import java.util
import scala.collection.JavaConversions._
import java.util.{Properties, Collections}
import ly.stealth.mesos.kafka.Util.{BindAddress, Str, Period}
import ly.stealth.mesos.kafka.Topics.Topic

object Cli {
  var api: String = null
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
      handleHelp(); out.println()
      throw new Error("command required")
    }

    val cmd = args(0)
    args = args.slice(1, args.length)
    if (cmd == "scheduler" && !noScheduler) { handleScheduler(args); return }
    if (cmd == "help") { handleHelp(if (args.length > 0) args(0) else null, if (args.length > 1) args(1) else null); return }

    args = handleGenericOptions(args)

    // rest of cmds require <subCmd>
    if (args.length < 1) {
      handleHelp(cmd); out.println()
      throw new Error("command required")
    }

    val subCmd = args(0)
    args = args.slice(1, args.length)

    cmd match {
      case "topics" => TopicsCli.handle(subCmd, args)
      case "brokers" => BrokersCli.handle(subCmd, args)
      case _ => throw new Error("unsupported command " + cmd)
    }
  }

  private def handleHelp(cmd: String = null, subCmd: String = null): Unit = {
    cmd match {
      case null =>
        out.println("Usage: <command>\n")
        printCmds()
      case "help" =>
        out.println("Print general or command-specific help\nUsage: help {command}")
      case "scheduler" =>
        if (noScheduler) throw new Error(s"unsupported command $cmd")
        handleScheduler(null, help = true)
      case "brokers" =>
        BrokersCli.handle(subCmd, null, help = true)
      case "topics" =>
        TopicsCli.handle(subCmd, null, help = true)
      case _ =>
        throw new Error(s"unsupported command $cmd")
    }
  }

  private def handleScheduler(args: Array[String], help: Boolean = false): Unit = {
    val parser = newParser()
    parser.accepts("debug", "Debug mode. Default - " + Config.debug)
      .withRequiredArg().ofType(classOf[java.lang.Boolean])

    parser.accepts("storage",
      """Storage for cluster state. Examples:
        | - file:kafka-mesos.json
        | - zk:/kafka-mesos
        |Default - """.stripMargin + Config.storage)
      .withRequiredArg().ofType(classOf[String])


    parser.accepts("master",
      """Master connection settings. Examples:
        | - master:5050
        | - master:5050,master2:5050
        | - zk://master:2181/mesos
        | - zk://username:password@master:2181
        | - zk://master:2181,master2:2181/mesos""".stripMargin)
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("user", "Mesos user to run tasks. Default - none")
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("principal", "Principal (username) used to register framework. Default - none")
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("secret", "Secret (password) used to register framework. Default - none")
      .withRequiredArg().ofType(classOf[String])


    parser.accepts("framework-name", "Framework name. Default - " + Config.frameworkName)
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("framework-role", "Framework role. Default - " + Config.frameworkRole)
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("framework-timeout", "Framework timeout (30s, 1m, 1h). Default - " + Config.frameworkTimeout)
      .withRequiredArg().ofType(classOf[String])


    parser.accepts("api", "Api url. Example: http://master:7000")
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("bind-address", "Scheduler bind address (master, 0.0.0.0, 192.168.50.*, if:eth1). Default - all")
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("zk",
      """Kafka zookeeper.connect. Examples:
        | - master:2181
        | - master:2181,master2:2181""".stripMargin)
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("jre", "JRE zip-file (jre-7-openjdk.zip). Default - none.")
      .withRequiredArg().ofType(classOf[String])

    parser.accepts("log", "Log file to use. Default - stdout.")
      .withRequiredArg().ofType(classOf[String])


    val configArg = parser.nonOptions()

    if (help) {
      out.println("Start scheduler \nUsage: scheduler [options] [config.properties]\n")
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

    var configFile = if (options.valueOf(configArg) != null) new File(options.valueOf(configArg)) else null
    if (configFile != null && !configFile.exists()) throw new Error(s"config-file $configFile not found")

    if (configFile == null && Config.DEFAULT_FILE.exists()) configFile = Config.DEFAULT_FILE

    if (configFile != null) {
      out.println("Loading config defaults from " + configFile)
      Config.load(configFile)
    }

    val debug = options.valueOf("debug").asInstanceOf[java.lang.Boolean]
    if (debug != null) Config.debug = debug

    val storage = options.valueOf("storage").asInstanceOf[String]
    if (storage != null) Config.storage = storage

    val provideOption = "Provide either cli option or config default value"

    val master = options.valueOf("master").asInstanceOf[String]
    if (master != null) Config.master = master
    else if (Config.master == null) throw new Error(s"Undefined master. $provideOption")

    val user = options.valueOf("user").asInstanceOf[String]
    if (user != null) Config.user = user

    val principal = options.valueOf("principal").asInstanceOf[String]
    if (principal != null) Config.principal = principal

    val secret = options.valueOf("secret").asInstanceOf[String]
    if (secret != null) Config.secret = secret


    val frameworkName = options.valueOf("framework-name").asInstanceOf[String]
    if (frameworkName != null) Config.frameworkName = frameworkName

    val frameworkRole = options.valueOf("framework-role").asInstanceOf[String]
    if (frameworkRole != null) Config.frameworkRole = frameworkRole

    val frameworkTimeout = options.valueOf("framework-timeout").asInstanceOf[String]
    if (frameworkTimeout != null)
      try { Config.frameworkTimeout = new Period(frameworkTimeout) }
      catch { case e: IllegalArgumentException => throw new Error("Invalid framework-timeout") }


    val api = options.valueOf("api").asInstanceOf[String]
    if (api != null) Config.api = api
    else if (Config.api == null) throw new Error(s"Undefined api. $provideOption")

    val bindAddress = options.valueOf("bind-address").asInstanceOf[String]
    if (bindAddress != null)
      try { Config.bindAddress = new BindAddress(bindAddress) }
      catch { case e: IllegalArgumentException => throw new Error("Invalid bind-address") }

    val zk = options.valueOf("zk").asInstanceOf[String]
    if (zk != null) Config.zk = zk
    else if (Config.zk == null) throw new Error(s"Undefined zk. $provideOption")

    val jre = options.valueOf("jre").asInstanceOf[String]
    if (jre != null) Config.jre = new File(jre)
    if (Config.jre != null && !Config.jre.exists()) throw new Error("JRE file doesn't exists")

    val log = options.valueOf("log").asInstanceOf[String]
    if (log != null) Config.log = new File(log)
    if (Config.log != null) out.println(s"Logging to ${Config.log}")

    Scheduler.start()
  }

  private[kafka] def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
    val parser = newParser()
    parser.accepts("api", "Api url. Example: http://master:7000").withRequiredArg().ofType(classOf[java.lang.String])
    parser.allowsUnrecognizedOptions()

    if (help) {
      out.println("Generic Options")
      parser.printHelpOn(out)
      return args
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        out.println()
        throw new Error(e.getMessage)
    }

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }
  
  private def optionsOrFile(value: String): String = {
    if (!value.startsWith("file:")) return value
    
    val file = new File(value.substring("file:".length))
    if (!file.exists()) throw new Error(s"File $file does not exists")
    
    val props: Properties = new Properties()
    val reader = new FileReader(file)
    try { props.load(reader) }
    finally { reader.close() }

    val map = new util.HashMap[String, String](props.toMap)
    Util.formatMap(map)
  }

  private def newParser(): OptionParser = {
    val parser: OptionParser = new OptionParser()
    parser.formatHelpWith(new BuiltinHelpFormatter(Util.terminalWidth, 2))
    parser
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("help [cmd [cmd]] - print general or command-specific help", 1)
    if (!noScheduler) printLine("scheduler        - start scheduler", 1)
    printLine("brokers          - broker management commands", 1)
    printLine("topics           - topic management commands", 1)
  }

  private def printLine(s: Object = "", indent: Int = 0): Unit = out.println("  " * indent + s)

  private[kafka] def resolveApi(apiOption: String): Unit = {
    if (api != null) return

    if (apiOption != null) {
      api = apiOption
      return
    }

    if (System.getenv("KM_API") != null) {
      api = System.getenv("KM_API")
      return
    }

    if (Config.DEFAULT_FILE.exists()) {
      val props: Properties = new Properties()
      val stream: FileInputStream = new FileInputStream(Config.DEFAULT_FILE)
      props.load(stream)
      stream.close()

      api = props.getProperty("api")
      if (api != null) return
    }

    throw new Error("Undefined api. Provide either cli option or config default value")
  }

  private[kafka] def noScheduler: Boolean = System.getenv("KM_NO_SCHEDULER") != null

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
    val url: String = api + (if (api.endsWith("/")) "" else "/") + "api" + uri

    val connection: HttpURLConnection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    var response: String = null
    try {
      connection.setRequestMethod("POST")
      connection.setDoOutput(true)

      val data = qs.getBytes("utf-8")
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
      connection.setRequestProperty("Content-Length", "" + data.length)
      connection.getOutputStream.write(data)

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
  
  object BrokersCli {
    def handle(cmd: String, _args: Array[String], help: Boolean = false): Unit = {
      if (help) {
        handleHelp(cmd)
        return
      }

      if (cmd == "list") { handleList(); return }

      // rest of cmds require <arg>
      var args = _args
      if (args.length < 1) {
        handleHelp(cmd); out.println()
        throw new Error("argument required")
      }

      val arg = args(0)
      args = args.slice(1, args.length)

      cmd match {
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case "remove" => handleRemove(arg)
        case "start" | "stop" => handleStartStop(arg, args, cmd == "start")
        case "rebalance" => handleRebalance(arg, args)
        case _ => throw new Error("unsupported command " + cmd)
      }
    }

    private def handleHelp(cmd: String): Unit = {
      cmd match {
        case null =>
          out.println("Broker management commands\nUsage: brokers <command>\n")
          printCmds()
        case "list" =>
          handleList(help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case "remove" =>
          handleRemove(null, help = true)
        case "start" | "stop" =>
          handleStartStop(null, null, cmd == "start", help = true)
        case "rebalance" =>
          handleRebalance(null, null, help = true)
        case _ =>
          throw new Error(s"unsupported command $cmd")
      }
    }

    private def handleList(help: Boolean = false): Unit = {
      if (help) {
        out.println("List brokers\nUsage: brokers list [options]\n")
        handleGenericOptions(null, help = true)
        return
      }

      var json: Map[String, Object] = null
      try { json = sendRequest("/brokers/list", Collections.emptyMap()) }
      catch { case e: IOException => throw new Error("" + e) }

      val cluster: Cluster = new Cluster()
      cluster.fromJson(json)
      val brokers: util.List[Broker] = cluster.getBrokers

      val title = if (brokers.isEmpty) "no brokers" else "broker" + (if (brokers.size() > 1) "s" else "") + ":"
      printLine(title)
      for (broker <- brokers) {
        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleAddUpdate(id: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("cpus", "cpu amount (0.5, 1, 2)").withRequiredArg().ofType(classOf[java.lang.Double])
      parser.accepts("mem", "mem amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])
      parser.accepts("heap", "heap amount in Mb").withRequiredArg().ofType(classOf[java.lang.Long])
      parser.accepts("port", "port or range (31092, 31090..31100). Default - auto").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("bind-address", "broker bind address (broker0, 192.168.50.*, if:eth1). Default - auto").withRequiredArg().ofType(classOf[java.lang.String])
      parser.accepts("stickiness-period", "stickiness period to preserve same node for broker (5m, 10m, 1h)").withRequiredArg().ofType(classOf[String])

      parser.accepts("options", "options or file. Examples:\n log.dirs=/tmp/kafka/$id,num.io.threads=16\n file:server.properties").withRequiredArg()
      parser.accepts("log4j-options", "log4j options or file. Examples:\n log4j.logger.kafka=DEBUG\\, kafkaAppender\n file:log4j.properties").withRequiredArg()
      parser.accepts("jvm-options", "jvm options string (-Xms128m -XX:PermSize=48m)").withRequiredArg()
      parser.accepts("constraints", "constraints (hostname=like:master,rack=like:1.*). See below.").withRequiredArg()

      parser.accepts("failover-delay", "failover delay (10s, 5m, 3h)").withRequiredArg().ofType(classOf[String])
      parser.accepts("failover-max-delay", "max failover delay. See failoverDelay.").withRequiredArg().ofType(classOf[String])
      parser.accepts("failover-max-tries", "max failover tries. Default - none").withRequiredArg().ofType(classOf[String])

      if (help) {
        val cmd = if (add) "add" else "update"
        out.println(s"${cmd.capitalize} brokers\nUsage: $cmd <id-expr> [options]\n")
        parser.printHelpOn(out)

        out.println()
        handleGenericOptions(null, help = true)

        out.println()
        printIdExprExamples()

        out.println()
        printConstraintExamples()

        if (!add) out.println("\nNote: use \"\" arg to unset an option")
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

      val cpus = options.valueOf("cpus").asInstanceOf[java.lang.Double]
      val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
      val heap = options.valueOf("heap").asInstanceOf[java.lang.Long]
      val port = options.valueOf("port").asInstanceOf[String]
      val bindAddress = options.valueOf("bind-address").asInstanceOf[String]
      val stickinessPeriod = options.valueOf("stickiness-period").asInstanceOf[String]

      val constraints = options.valueOf("constraints").asInstanceOf[String]
      val options_ = options.valueOf("options").asInstanceOf[String]
      val log4jOptions = options.valueOf("log4j-options").asInstanceOf[String]
      val jvmOptions = options.valueOf("jvm-options").asInstanceOf[String]

      val failoverDelay = options.valueOf("failover-delay").asInstanceOf[String]
      val failoverMaxDelay = options.valueOf("failover-max-delay").asInstanceOf[String]
      val failoverMaxTries = options.valueOf("failover-max-tries").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]
      params.put("id", id)

      if (cpus != null) params.put("cpus", "" + cpus)
      if (mem != null) params.put("mem", "" + mem)
      if (heap != null) params.put("heap", "" + heap)
      if (port != null) params.put("port", port)
      if (bindAddress != null) params.put("bindAddress", bindAddress)
      if (stickinessPeriod != null) params.put("stickinessPeriod", stickinessPeriod)

      if (options_ != null) params.put("options", optionsOrFile(options_))
      if (constraints != null) params.put("constraints", constraints)
      if (log4jOptions != null) params.put("log4jOptions", optionsOrFile(log4jOptions))
      if (jvmOptions != null) params.put("jvmOptions", jvmOptions)

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

    private def handleRemove(id: String, help: Boolean = false): Unit = {
      if (help) {
        out.println("Remove brokers\nUsage: remove <id-expr> [options]\n")
        handleGenericOptions(null, help = true)

        out.println()
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

    private def handleStartStop(id: String, args: Array[String], start: Boolean, help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])
      if (!start) parser.accepts("force", "forcibly stop").withOptionalArg().ofType(classOf[String])

      if (help) {
        val cmd = if (start) "start" else "stop"
        out.println(s"${cmd.capitalize} brokers\nUsage: $cmd <id-expr> [options]\n")
        parser.printHelpOn(out)

        out.println()
        handleGenericOptions(null, help = true)

        out.println()
        printIdExprExamples()
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

      val cmd: String = if (start) "start" else "stop"
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val force: Boolean = options.has("force")

      val params = new util.LinkedHashMap[String, String]()
      params.put("id", id)
      if (timeout != null) params.put("timeout", timeout)
      if (force) params.put("force", null)

      var json: Map[String, Object] = null
      try { json = sendRequest("/brokers/" + cmd, params) }
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
      val parser = newParser()
      parser.accepts("topics", "<topic-expr>. Default - *. See below.").withRequiredArg().ofType(classOf[String])
      parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])

      if (help) {
        out.println("Rebalance topics\nUsage: rebalance <id-expr>|status [options]\n")
        parser.printHelpOn(out)

        out.println()
        handleGenericOptions(null, help = true)

        out.println()
        printTopicExprExamples()

        out.println()
        printIdExprExamples()
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

      val topics: String = options.valueOf("topics").asInstanceOf[String]
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]()
      if (arg != "status") params.put("id", arg)
      if (topics != null) params.put("topics", topics)
      if (timeout != null) params.put("timeout", timeout)

      var json: Map[String, Object] = null
      try { json = sendRequest("/brokers/rebalance", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val status = json("status").asInstanceOf[String]
      val error = if (json.contains("error")) json("error").asInstanceOf[String] else ""
      val state: String = json("state").asInstanceOf[String]

      val is: String = if (status == "idle" || status == "running") "is " else ""
      val colon: String = if (state.isEmpty &&  error.isEmpty) "" else ":"

      // started|completed|failed|running|idle|timeout
      if (status == "timeout") throw new Error("Rebalance timeout:\n" + state)
      printLine(s"Rebalance $is$status$colon $error")
      if (error.isEmpty && !state.isEmpty) printLine(state)
    }

    private def printCmds(): Unit = {
      printLine("Commands:")
      printLine("list       - list brokers", 1)
      printLine("add        - add brokers", 1)
      printLine("update     - update brokers", 1)
      printLine("remove     - remove brokers", 1)
      printLine("start      - start brokers", 1)
      printLine("stop       - stop brokers", 1)
      printLine("rebalance  - rebalance topics", 1)
    }

    private def printBroker(broker: Broker, indent: Int): Unit = {
      printLine("id: " + broker.id, indent)
      printLine("active: " + broker.active, indent)
      printLine("state: " + broker.state(), indent)
      printLine("resources: " + "cpus:" + "%.2f".format(broker.cpus) + ", mem:" + broker.mem + ", heap:" + broker.heap + ", port:" + (if (broker.port != null) broker.port else "auto"), indent)

      if (broker.bindAddress != null) printLine("bind-address: " + broker.bindAddress, indent)
      if (!broker.constraints.isEmpty) printLine("constraints: " + Util.formatMap(broker.constraints), indent)
      if (!broker.options.isEmpty) printLine("options: " + Util.formatMap(broker.options), indent)
      if (!broker.log4jOptions.isEmpty) printLine("log4j-options: " + Util.formatMap(broker.log4jOptions), indent)
      if (broker.jvmOptions != null) printLine("jvm-options: " + broker.jvmOptions, indent)

      var failover = "failover:"
      failover += " delay:" + broker.failover.delay
      failover += ", max-delay:" + broker.failover.maxDelay
      if (broker.failover.maxTries != null) failover += ", max-tries:" + broker.failover.maxTries
      printLine(failover, indent)

      var stickiness = "stickiness:"
      stickiness += " period:" + broker.stickiness.period
      if (broker.stickiness.hostname != null) stickiness += ", hostname:" + broker.stickiness.hostname
      if (broker.stickiness.stopTime != null) stickiness += ", expires:" + Str.dateTime(broker.stickiness.expires)
      printLine(stickiness, indent)

      val task = broker.task
      if (task != null) {
        printLine("task: ", indent)
        printLine("id: " + broker.task.id, indent + 1)
        printLine("state: " + task.state, indent + 1)
        if (task.endpoint != null) printLine("endpoint: " + task.endpoint + (if (broker.bindAddress != null) " (" + task.hostname + ")" else ""), indent + 1)
        if (!task.attributes.isEmpty) printLine("attributes: " + Util.formatMap(task.attributes), indent + 1)
      }
    }

    private def printIdExprExamples(): Unit = {
      printLine("id-expr examples:")
      printLine("0      - broker 0", 1)
      printLine("0,1    - brokers 0,1", 1)
      printLine("0..2   - brokers 0,1,2", 1)
      printLine("0,1..2 - brokers 0,1,2", 1)
      printLine("*      - any broker", 1)
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

    private def printTopicExprExamples(): Unit = {
      printLine("topic-expr examples:")
      printLine("t0        - topic t0 with default RF (replication-factor)", 1)
      printLine("t0,t1     - topics t0, t1 with default RF", 1)
      printLine("t0:3      - topic t0 with RF=3", 1)
      printLine("t0,t1:2   - topic t0 with default RF, topic t1 with RF=2", 1)
      printLine("*         - all topics with default RF", 1)
      printLine("*:2       - all topics with RF=2", 1)
      printLine("t0:1,*:2  - all topics with RF=2 except topic t0 with RF=1", 1)
    }
  }
  
  object TopicsCli {
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
        handleHelp(cmd); out.println()
        throw new Error("argument required")
      }

      cmd match {
        case "list" => handleList(arg)
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case _ => throw new Error("unsupported topics command " + cmd)
      }
    }

    def handleHelp(cmd: String): Unit = {
      cmd match {
        case null =>
          out.println("Topic management commands\nUsage: topics <command>\n")
          printCmds()
        case "list" =>
          handleList(null, help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case _ =>
          throw new Error(s"unsupported command $cmd")
      }
    }

    def handleList(name: String, help: Boolean = false): Unit = {
      if (help) {
        out.println("List topics\nUsage: topics list [name-regex]\n")
        handleGenericOptions(null, help = true)
        return
      }

      val params = new util.LinkedHashMap[String, String]
      if (name != null) params.put("name", name)

      var json: Map[String, Object] = null
      try { json = sendRequest("/topics/list", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val topicsNodes: List[Map[String, Object]] = json("topics").asInstanceOf[List[Map[String, Object]]]

      val title: String = if (topicsNodes.isEmpty) "no topics" else "topic" + (if (topicsNodes.size > 1) "s" else "") + ":"
      out.println(title)

      for (topicNode <- topicsNodes) {
        val topic = new Topic()
        topic.fromJson(topicNode)

        printTopic(topic, 1)
        out.println()
      }
    }

    def handleAddUpdate(name: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
      val cmd = if (add) "add" else "update"

      val parser = newParser()
      if (add) {
        parser.accepts("partitions", "partitions count. Default - 1").withRequiredArg().ofType(classOf[Integer])
        parser.accepts("replicas", "replicas count. Default - 1").withRequiredArg().ofType(classOf[Integer])
      }
      parser.accepts("options", "topic options. Example: flush.ms=60000,retention.ms=6000000").withRequiredArg().ofType(classOf[String])

      if (help) {
        out.println(s"${cmd.capitalize} topic\nUsage: topics $cmd <name> [options]\n")
        parser.printHelpOn(out)

        out.println()
        handleGenericOptions(null, help = true)
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

      val partitions = options.valueOf("partitions").asInstanceOf[Integer]
      val replicas = options.valueOf("replicas").asInstanceOf[Integer]
      val options_ = options.valueOf("options").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]
      params.put("name", name)
      if (partitions != null) params.put("partitions", "" + partitions)
      if (replicas != null) params.put("replicas", "" + replicas)
      if (options != null) params.put("options", options_)

      var json: Map[String, Object] = null
      try { json = sendRequest(s"/topics/$cmd", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val topic: Topic = new Topic()
      topic.fromJson(json("topic").asInstanceOf[Map[String, Object]])

      val addedUpdated = if (add) "added" else "updated"
      printLine(s"Topic $addedUpdated:")
      printTopic(topic, 1)
    }

    private def printCmds(): Unit = {
      printLine("Commands:")
      printLine("list       - list topics", 1)
      printLine("add        - add topic", 1)
      printLine("update     - update topic", 1)
    }

    private def printTopic(topic: Topic, indent: Int): Unit = {
      printLine("name: " + topic.name, indent)
      printLine("partitions: " + topic.partitionsState, indent)
      if (!topic.options.isEmpty) printLine("options: " + Util.formatMap(topic.options), indent)
    }
  }
}
