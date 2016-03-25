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
import java.util.{Date, Properties, Collections}
import ly.stealth.mesos.kafka.Util.{BindAddress}
import net.elodina.mesos.util.{Strings, Period, Repr}
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
      handleHelp(); printLine()
      throw new Error("command required")
    }

    val cmd = args(0)
    args = args.slice(1, args.length)
    if (cmd == "help") { handleHelp(if (args.length > 0) args(0) else null, if (args.length > 1) args(1) else null); return }
    if (cmd == "scheduler" && SchedulerCli.isEnabled) { SchedulerCli.handle(args); return }

    args = handleGenericOptions(args)

    // rest of cmds require <subCmd>
    if (args.length < 1) {
      handleHelp(cmd); printLine()
      throw new Error("command required")
    }

    val subCmd = args(0)
    args = args.slice(1, args.length)

    cmd match {
      case "topic" => TopicCli.handle(subCmd, args)
      case "broker" => BrokerCli.handle(subCmd, args)
      case _ => throw new Error("unsupported command " + cmd)
    }
  }

  private def handleHelp(cmd: String = null, subCmd: String = null): Unit = {
    cmd match {
      case null =>
        printLine("Usage: <command>\n")
        printCmds()

        printLine()
        printLine("Run `help <command>` to see details of specific command")
      case "help" =>
        printLine("Print general or command-specific help\nUsage: help [cmd [cmd]]")
      case "scheduler" =>
        if (!SchedulerCli.isEnabled) throw new Error(s"unsupported command $cmd")
        SchedulerCli.handle(null, help = true)
      case "broker" =>
        BrokerCli.handle(subCmd, null, help = true)
      case "topic" =>
        TopicCli.handle(subCmd, null, help = true)
      case _ =>
        throw new Error(s"unsupported command $cmd")
    }
  }

  private[kafka] def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
    val parser = newParser()
    parser.accepts("api", "Api url. Example: http://master:7000").withRequiredArg().ofType(classOf[java.lang.String])
    parser.allowsUnrecognizedOptions()

    if (help) {
      printLine("Generic Options")
      parser.printHelpOn(out)
      return args
    }

    var options: OptionSet = null
    try { options = parser.parse(args: _*) }
    catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
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
    Strings.formatMap(map)
  }

  private def newParser(): OptionParser = {
    val parser: OptionParser = new OptionParser()
    parser.formatHelpWith(new BuiltinHelpFormatter(Util.terminalWidth, 2))
    parser
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("help [cmd [cmd]] - print general or command-specific help", 1)
    if (SchedulerCli.isEnabled) printLine("scheduler        - start scheduler", 1)
    printLine("broker           - broker management commands", 1)
    printLine("topic            - topic management commands", 1)
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

  object SchedulerCli {
    def isEnabled: Boolean = System.getenv("KM_NO_SCHEDULER") == null

    def handle(args: Array[String], help: Boolean = false): Unit = {
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
        printLine("Start scheduler \nUsage: scheduler [options] [config.properties]\n")
        parser.printHelpOn(out)
        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      var configFile = if (options.valueOf(configArg) != null) new File(options.valueOf(configArg)) else null
      if (configFile != null && !configFile.exists()) throw new Error(s"config-file $configFile not found")

      if (configFile == null && Config.DEFAULT_FILE.exists()) configFile = Config.DEFAULT_FILE

      if (configFile != null) {
        printLine("Loading config defaults from " + configFile)
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
      if (Config.log != null) printLine(s"Logging to ${Config.log}")

      Scheduler.start()
    }
  }

  object BrokerCli {
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
        case "list" => handleList(arg)
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case "remove" => handleRemove(arg)
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
          handleList(null, help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case "remove" =>
          handleRemove(null, help = true)
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

    private def handleList(expr: String, help: Boolean = false): Unit = {
      if (help) {
        printLine("List brokers\nUsage: broker list [<broker-expr>] [options]\n")
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printBrokerExprExamples(out)

        return
      }

      val params = new util.HashMap[String, String]()
      if (expr != null) params.put("broker", expr)

      var json: Map[String, Object] = null
      try { json = sendRequest("/broker/list", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val brokerNodes = json("brokers").asInstanceOf[List[Map[String, Object]]]
      val title = if (brokerNodes.isEmpty) "no brokers" else "broker" + (if (brokerNodes.size > 1) "s" else "") + ":"
      printLine(title)

      for (brokerNode <- brokerNodes) {
        val broker = new Broker()
        broker.fromJson(brokerNode)

        printBroker(broker, 1)
        printLine()
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
        printLine(s"${cmd.capitalize} broker\nUsage: broker $cmd <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printBrokerExprExamples(out)

        printLine()
        printConstraintExamples()

        if (!add) printLine("\nNote: use \"\" arg to unset an option")
        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val cpus = options.valueOf("cpus").asInstanceOf[java.lang.Double]
      val mem = options.valueOf("mem").asInstanceOf[java.lang.Long]
      val heap = options.valueOf("heap").asInstanceOf[java.lang.Long]
      val port = options.valueOf("port").asInstanceOf[String]
      val volume = options.valueOf("volume").asInstanceOf[String]
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
      params.put("broker", expr)

      if (cpus != null) params.put("cpus", "" + cpus)
      if (mem != null) params.put("mem", "" + mem)
      if (heap != null) params.put("heap", "" + heap)
      if (port != null) params.put("port", port)
      if (volume != null) params.put("volume", volume)
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
      try { json = sendRequest("/broker/" + (if (add) "add" else "update"), params) }
      catch { case e: IOException => throw new Error("" + e) }
      val brokerNodes: List[Map[String, Object]] = json("brokers").asInstanceOf[List[Map[String, Object]]]

      val addedUpdated = if (add) "added" else "updated"
      val brokers = "broker" + (if (brokerNodes.length > 1) "s" else "")

      printLine(s"$brokers $addedUpdated:")
      for (brokerNode <- brokerNodes) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)

        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleRemove(expr: String, help: Boolean = false): Unit = {
      if (help) {
        printLine("Remove broker\nUsage: broker remove <broker-expr> [options]\n")
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printBrokerExprExamples(out)
        return
      }

      var json: Map[String, Object] = null
      try { json = sendRequest("/broker/remove", Collections.singletonMap("broker", expr)) }
      catch { case e: IOException => throw new Error("" + e) }

      val ids = json("ids").asInstanceOf[String]
      val brokers = "broker" + (if (ids.contains(",")) "s" else "")

      printLine(s"$brokers $ids removed")
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
        Expr.printBrokerExprExamples(out)
        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val cmd: String = if (start) "start" else "stop"
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val force: Boolean = options.has("force")

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", expr)
      if (timeout != null) params.put("timeout", timeout)
      if (force) params.put("force", null)

      var json: Map[String, Object] = null
      try { json = sendRequest("/broker/" + cmd, params) }
      catch { case e: IOException => throw new Error("" + e) }

      val status = json("status").asInstanceOf[String]
      val brokerNodes: List[Map[String, Object]] = json("brokers").asInstanceOf[List[Map[String, Object]]]

      val brokers = "broker" + (if (brokerNodes.size > 1) "s" else "")
      val startStop = if (start) "start" else "stop"

      // started|stopped|scheduled|timeout
      if (status == "timeout") throw new Error(s"$brokers $startStop timeout")
      else if (status == "scheduled") printLine(s"$brokers scheduled to $startStop:")
      else printLine(s"$brokers $status:")

      for (brokerNode <- brokerNodes) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)

        printBroker(broker, 1)
        printLine()
      }
    }

    private def handleRestart(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("timeout", "time to wait until broker restarts (30s, 1m, 1h). Default - 2m").withRequiredArg().ofType(classOf[String])

      if (help) {
        printLine(s"Restart broker\nUsage: broker restart <broker-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printBrokerExprExamples(out)
        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val timeout: String = options.valueOf("timeout").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", expr)
      if (timeout != null) params.put("timeout", timeout)

      var json: Map[String, Object] = null
      try { json = sendRequest("/broker/restart", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val status = json("status").asInstanceOf[String]

      // restarted|timeout
      if (status == "timeout") throw new Error(json("message").asInstanceOf[String])

      val brokerNodes: List[Map[String, Object]] = json("brokers").asInstanceOf[List[Map[String, Object]]]
      val brokers = "broker" + (if (brokerNodes.size > 1) "s" else "")
      printLine(s"$brokers $status:")

      for (brokerNode <- brokerNodes) {
        val broker: Broker = new Broker()
        broker.fromJson(brokerNode)

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

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val name: String = options.valueOf("name").asInstanceOf[String]
      val lines: Integer = options.valueOf("lines").asInstanceOf[java.lang.Integer]

      val params = new util.LinkedHashMap[String, String]()
      params.put("broker", brokerId)
      if (timeout != null) params.put("timeout", timeout)
      if (name != null) params.put("name", name)
      if (lines != null) params.put("lines", "" + lines)

      var json: Map[String, Object] = null
      try { json = sendRequest("/broker/log", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val status = json("status").asInstanceOf[String]
      val content = json("content").asInstanceOf[String]

      if (status == "timeout") throw new Error(s"broker $brokerId log retrieve timeout")
      else printLine(content)

      if (!content.isEmpty && content.last != '\n') printLine()
    }

    private def printCmds(): Unit = {
      printLine("Commands:")
      printLine("list       - list brokers", 1)
      printLine("add        - add broker", 1)
      printLine("update     - update broker", 1)
      printLine("remove     - remove broker", 1)
      printLine("start      - start broker", 1)
      printLine("stop       - stop broker", 1)
      printLine("restart    - restart broker", 1)
      printLine("log        - retrieve broker log", 1)
    }

    private def printBroker(broker: Broker, indent: Int): Unit = {
      printLine("id: " + broker.id, indent)
      printLine("active: " + broker.active, indent)
      printLine("state: " + broker.state() + (if (broker.needsRestart) " (modified, needs restart)" else ""), indent)
      printLine("resources: " + brokerResources(broker), indent)

      if (broker.bindAddress != null) printLine("bind-address: " + broker.bindAddress, indent)
      if (!broker.constraints.isEmpty) printLine("constraints: " + Strings.formatMap(broker.constraints), indent)
      if (!broker.options.isEmpty) printLine("options: " + Strings.formatMap(broker.options), indent)
      if (!broker.log4jOptions.isEmpty) printLine("log4j-options: " + Strings.formatMap(broker.log4jOptions), indent)
      if (broker.jvmOptions != null) printLine("jvm-options: " + broker.jvmOptions, indent)

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
        if (!task.attributes.isEmpty) printLine("attributes: " + Strings.formatMap(task.attributes), indent + 1)
      }

      val metrics = broker.metrics
      if (metrics != null) {
        printLine("metrics: ", indent)
        printLine("collected: " + Repr.dateTime(new Date(metrics.timestamp)), indent + 1)
        printLine("under-replicated-partitions: " + metrics.underReplicatedPartitions, indent + 1)
        printLine("offline-partitions-count: " + metrics.offlinePartitionsCount, indent + 1)
        printLine("is-active-controller: " + metrics.activeControllerCount, indent + 1)
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
  
  object TopicCli {
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
        case "list" => handleList(arg)
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case "rebalance" => handleRebalance(arg, args)
        case _ => throw new Error("unsupported topic command " + cmd)
      }
    }

    def handleHelp(cmd: String): Unit = {
      cmd match {
        case null =>
          printLine("Topic management commands\nUsage: topic <command>\n")
          printCmds()

          printLine()
          printLine("Run `help topic <command>` to see details of specific command")
        case "list" =>
          handleList(null, help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case "rebalance" =>
          handleRebalance(null, null, help = true)
        case _ =>
          throw new Error(s"unsupported topic command $cmd")
      }
    }

    def handleList(expr: String, help: Boolean = false): Unit = {
      if (help) {
        printLine("List topics\nUsage: topic list [<topic-expr>]\n")
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printTopicExprExamples(out)

        return
      }

      val params = new util.LinkedHashMap[String, String]
      if (expr != null) params.put("topic", expr)

      var json: Map[String, Object] = null
      try { json = sendRequest("/topic/list", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val topicsNodes: List[Map[String, Object]] = json("topics").asInstanceOf[List[Map[String, Object]]]
      val title: String = if (topicsNodes.isEmpty) "no topics" else "topic" + (if (topicsNodes.size > 1) "s" else "") + ":"
      printLine(title)

      for (topicNode <- topicsNodes) {
        val topic = new Topic()
        topic.fromJson(topicNode)

        printTopic(topic, 1)
        printLine()
      }
    }

    def handleAddUpdate(name: String, args: Array[String], add: Boolean, help: Boolean = false): Unit = {
      val cmd = if (add) "add" else "update"

      val parser = newParser()
      if (add) {
        parser.accepts("broker", "<broker-expr>. Default - *. See below.").withRequiredArg().ofType(classOf[String])
        parser.accepts("partitions", "partitions count. Default - 1").withRequiredArg().ofType(classOf[Integer])
        parser.accepts("replicas", "replicas count. Default - 1").withRequiredArg().ofType(classOf[Integer])
      }
      parser.accepts("options", "topic options. Example: flush.ms=60000,retention.ms=6000000").withRequiredArg().ofType(classOf[String])

      if (help) {
        printLine(s"${cmd.capitalize} topic\nUsage: topic $cmd <topic-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printTopicExprExamples(out)

        if (add) {
          printLine()
          Expr.printBrokerExprExamples(out)
        }

        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val broker = options.valueOf("broker").asInstanceOf[String]
      val partitions = options.valueOf("partitions").asInstanceOf[Integer]
      val replicas = options.valueOf("replicas").asInstanceOf[Integer]
      val options_ = options.valueOf("options").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]
      params.put("topic", name)
      if (broker != null) params.put("broker", broker)
      if (partitions != null) params.put("partitions", "" + partitions)
      if (replicas != null) params.put("replicas", "" + replicas)
      if (options != null) params.put("options", options_)

      var json: Map[String, Object] = null
      try { json = sendRequest(s"/topic/$cmd", params) }
      catch { case e: IOException => throw new Error("" + e) }

      val topicNodes = json("topics").asInstanceOf[List[Map[String, Object]]]

      val addedUpdated = if (add) "added" else "updated"
      val title = s"topic${if (topicNodes.size > 1) "s" else ""} $addedUpdated:"
      printLine(title)

      for (topicNode <- topicNodes) {
        val topic = new Topic()
        topic.fromJson(topicNode)

        printTopic(topic, 1)
        printLine()
      }
    }

    private def handleRebalance(exprOrStatus: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("broker", "<broker-expr>. Default - *. See below.").withRequiredArg().ofType(classOf[String])
      parser.accepts("replicas", "replicas count. Default - 1").withRequiredArg().ofType(classOf[Integer])
      parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg().ofType(classOf[String])

      if (help) {
        printLine("Rebalance topics\nUsage: topic rebalance <topic-expr>|status [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        Expr.printTopicExprExamples(out)

        printLine()
        Expr.printBrokerExprExamples(out)
        return
      }

      var options: OptionSet = null
      try { options = parser.parse(args: _*) }
      catch {
        case e: OptionException =>
          parser.printHelpOn(out)
          printLine()
          throw new Error(e.getMessage)
      }

      val broker: String = options.valueOf("broker").asInstanceOf[String]
      val replicas: Integer = options.valueOf("replicas").asInstanceOf[Integer]
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]

      val params = new util.LinkedHashMap[String, String]()
      if (exprOrStatus != "status") params.put("topic", exprOrStatus)
      if (broker != null) params.put("broker", broker)
      if (replicas != null) params.put("replicas", "" + replicas)
      if (timeout != null) params.put("timeout", timeout)

      var json: Map[String, Object] = null
      try { json = sendRequest("/topic/rebalance", params) }
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
      printLine("list       - list topics", 1)
      printLine("add        - add topic", 1)
      printLine("update     - update topic", 1)
      printLine("rebalance  - rebalance topics", 1)
    }

    private def printTopic(topic: Topic, indent: Int): Unit = {
      printLine("name: " + topic.name, indent)
      printLine("partitions: " + topic.partitionsState, indent)
      if (!topic.options.isEmpty) printLine("options: " + Strings.formatMap(topic.options), indent)
    }
  }
}
