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

import java.io._
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util
import java.util.Properties
import joptsimple.{BuiltinHelpFormatter, OptionException, OptionParser, OptionSet}
import ly.stealth.mesos.kafka._
import net.elodina.mesos.util.Strings
import scala.collection.JavaConversions._
import scala.io.Source

trait CliHandler {
  def handle(cmd: String, _args: Array[String], help: Boolean = false): Unit
}

trait CliUtils
{
  var out: PrintStream
  var err: PrintStream
  var api: String

  def printLine(s: Object = "", indent: Int = 0): Unit = out.println("  " * indent + s)

  def newParser(): OptionParser = {
    val parser: OptionParser = new OptionParser()
    parser.formatHelpWith(new BuiltinHelpFormatter(Util.terminalWidth, 2))
    parser
  }

  def handleGenericOptions(args: Array[String], help: Boolean = false): Array[String] = {
    val parser = newParser()
    parser.accepts("api", "Api url. Example: http://master:7000").withRequiredArg().ofType(classOf[java.lang.String])
    parser.allowsUnrecognizedOptions()

    if (help) {
      printLine("Generic Options")
      parser.printHelpOn(out)
      return args
    }

    val options = parseOptions(parser, args)

    resolveApi(options.valueOf("api").asInstanceOf[String])
    options.nonOptionArguments().toArray(new Array[String](0))
  }

  def parseOptions(parser: OptionParser, args: Array[String]): OptionSet = {
    try {
      parser.parse(args:_*)
    } catch {
      case e: OptionException =>
        parser.printHelpOn(out)
        printLine()
        throw new Error(e.getMessage)
    }
  }

  def resolveApi(apiOption: String): Unit = {
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

  private[kafka] def sendRequestString(uri: String, params: util.Map[String, String]): String = {
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
    response
  }

  private[kafka] def sendRequest(uri: String, params: util.Map[String, String]): Unit = {
    sendRequestString(uri, params)
  }

  def sendRequestObj[T](uri: String, params: util.Map[String, String])(implicit m: Manifest[T]): T = {
    val response = sendRequestString(uri, params)
    if (response == null) {
      throw new NullPointerException()
    }
    else {
      json.JsonUtil.fromJson[T](response)
    }
  }

  def printBrokerExprExamples(): Unit = {
    out.println("broker-expr examples:")
    out.println("  0      - broker 0")
    out.println("  0,1    - brokers 0,1")
    out.println("  0..2   - brokers 0,1,2")
    out.println("  0,1..2 - brokers 0,1,2")
    out.println("  *      - any broker")
    out.println("attribute filtering:")
    out.println("  *[rack=r1]           - any broker having rack=r1")
    out.println("  *[hostname=slave*]   - any broker on host with name starting with 'slave'")
    out.println("  0..4[rack=r1,dc=dc1] - any broker having rack=r1 and dc=dc1")
  }


  def optionsOrFile(value: String): String = {
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
}

object Cli extends CliUtils with TopicCli with BrokerCli with SchedulerCli {
  var api: String = _
  var out: PrintStream = System.out
  var err: PrintStream = System.err

  def main(args: Array[String]): Unit = {
    try { exec(args) }
    catch { case e: Error =>
      err.println("Error: " + e.getMessage)
      System.exit(1)
    }
  }

  def exec(_args: Array[String], out: PrintStream = System.out, err: PrintStream = System.err): Unit = {
    var args = _args

    if (args.length == 0) {
      handleHelp(); printLine()
      throw new Error("command required")
    }

    val cmd = args(0)
    args = args.slice(1, args.length)
    if (cmd == "help") { handleHelp(if (args.length > 0) args(0) else null, if (args.length > 1) args(1) else null); return }
    if (cmd == "scheduler" && SchedulerCli.isEnabled) { schedulerCli.handle(null, args); return }

    args = handleGenericOptions(args)

    // rest of cmds require <subCmd>
    if (args.length < 1) {
      handleHelp(cmd); printLine()
      throw new Error("command required")
    }

    val subCmd = args(0)
    args = args.slice(1, args.length)

    cmd match {
      case "topic" => topicCli.handle(subCmd, args)
      case "broker" => brokerCli.handle(subCmd, args)
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
        schedulerCli.handle(null, null, help = true)
      case "broker" =>
        brokerCli.handle(subCmd, null, help = true)
      case "topic" =>
        topicCli.handle(subCmd, null, help = true)
      case _ =>
        throw new Error(s"unsupported command $cmd")
    }
  }

  private def printCmds(): Unit = {
    printLine("Commands:")
    printLine("help [cmd [cmd]] - print general or command-specific help", 1)
    if (SchedulerCli.isEnabled) printLine("scheduler        - start scheduler", 1)
    printLine("broker           - broker management commands", 1)
    printLine("topic            - topic management commands", 1)
  }

  class Error(message: String) extends java.lang.Error(message) {}

  object SchedulerCli {
    def isEnabled: Boolean = System.getenv("KM_NO_SCHEDULER") == null
  }
}
