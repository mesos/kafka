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
import joptsimple.OptionParser
import ly.stealth.mesos.kafka.cli.Cli.Error
import ly.stealth.mesos.kafka.{ListTopicsResponse, Partition, RebalanceStartResponse, Topic}
import net.elodina.mesos.util.Strings
import scala.collection.JavaConversions._

trait TopicCli {
  this: CliUtils =>

  val topicCli: CliHandler = new TopicCliImpl

  class TopicCliImpl extends CliHandler {
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
        handleHelp(cmd);
        printLine()
        throw new Error("argument required")
      }

      cmd match {
        case "list" => handleList(arg, args)
        case "add" | "update" => handleAddUpdate(arg, args, cmd == "add")
        case "rebalance" => handleRebalance(arg, args)
        case "realign" => handleRealign(arg, args)
        case "partitions" => handlePartitions(arg)
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
          handleList(null, null, help = true)
        case "add" | "update" =>
          handleAddUpdate(null, null, cmd == "add", help = true)
        case "rebalance" =>
          handleRebalance(null, null, help = true)
        case "realign" =>
          handleRealign(null, null, help = true)
        case "partitions" =>
          handlePartitions(null, help = true)
        case _ =>
          throw new Error(s"unsupported topic command $cmd")
      }
    }

    def printTopicExprExamples(): Unit = {
      out.println("topic-expr examples:")
      out.println("  t0        - topic t0")
      out.println("  t0,t1     - topics t0, t1")
      out.println("  *         - any topic")
      out.println("  t*        - topics starting with 't'")
    }


    def handleList(expr: String, args: Array[String], help: Boolean = false): Unit = {
      val parser = newParser()
      parser.accepts("quiet", "Displays a less verbose list of topics.  Default - false")
        .withOptionalArg()

      if (help) {
        printLine("List topics\nUsage: topic list [<topic-expr>]\n")
        handleGenericOptions(null, help = true)

        printLine()
        printTopicExprExamples()

        return
      }

      val options = parser.parse(args: _*)
      val params = new util.LinkedHashMap[String, String]
      if (expr != null) params.put("topic", expr)

      val json =
        try {
          sendRequestObj[ListTopicsResponse]("/topic/list", params)
        }
        catch {
          case e: IOException => throw new Error("" + e)
        }

      val topics = json.topics
      val title: String = if (topics.isEmpty) "no topics" else "topic" + (if (topics
        .size > 1) "s" else "") + ":"
      printLine(title)

      val quiet = options.has("quiet")
      for (topic <- topics) {
        if (quiet) {
          printLine(topic.name, 1)
        }
        else {
          printTopic(topic, 1)
          printLine()
        }
      }
    }

    def handleAddUpdate(
      name: String,
      args: Array[String],
      add: Boolean,
      help: Boolean = false
    ): Unit = {
      val cmd = if (add) "add" else "update"

      val parser = newParser()
      if (add) {
        parser.accepts("broker", "<broker-expr>. Default - *. See below.").withRequiredArg()
          .ofType(classOf[String])
        parser.accepts("partitions", "partitions count. Default - 1").withRequiredArg()
          .ofType(classOf[Integer])
        parser.accepts("replicas", "replicas count. Default - 1").withRequiredArg()
          .ofType(classOf[Integer])
        parser
          .accepts("fixedStartIndex", "index into the broker set to start assigning partitions at.  Default - -1 (random)")
          .withRequiredArg().ofType(classOf[Integer])
        parser
          .accepts("startPartitionId", "partition id to begin assignment at. Default - -1 (random)")
          .withRequiredArg().ofType(classOf[Integer])
      }
      parser.accepts("options", "topic options. Example: flush.ms=60000,retention.ms=6000000")
        .withRequiredArg().ofType(classOf[String])

      if (help) {
        printLine(s"${ cmd.capitalize } topic\nUsage: topic $cmd <topic-expr> [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printTopicExprExamples()

        if (add) {
          printLine()
          printBrokerExprExamples()
        }

        return
      }

      val options = parseOptions(parser, args)

      val broker = options.valueOf("broker").asInstanceOf[String]
      val partitions = options.valueOf("partitions").asInstanceOf[Integer]
      val replicas = options.valueOf("replicas").asInstanceOf[Integer]
      val options_ = options.valueOf("options").asInstanceOf[String]
      val fixedStartIndex = options.valueOf("fixedStartIndex").asInstanceOf[Integer]
      val startPartitionId = options.valueOf("startPartitionId").asInstanceOf[Integer]

      val params = new util.LinkedHashMap[String, String]
      params.put("topic", name)
      if (broker != null) params.put("broker", broker)
      if (partitions != null) params.put("partitions", "" + partitions)
      if (replicas != null) params.put("replicas", "" + replicas)
      if (options != null) params.put("options", options_)
      if (fixedStartIndex != null) params.put("fixedStartIndex", "" + fixedStartIndex)
      if (startPartitionId != null) params.put("startPartitionId", "" + startPartitionId)

      val json =
        try {
          sendRequestObj[ListTopicsResponse](s"/topic/$cmd", params)
        }
        catch {
          case e: IOException => throw new Error("" + e)
        }

      val topics = json.topics
      val addedUpdated = if (add) "added" else "updated"
      val title = s"topic${ if (topics.size > 1) "s" else "" } $addedUpdated:"
      printLine(title)

      for (topic <- topics) {
        printTopic(topic, 1)
        printLine()
      }
    }

    private def buildRebalanceBaseParser(): OptionParser = {
      val parser = newParser()
      parser.accepts("broker", "<broker-expr>. Default - *. See below.").withRequiredArg()
        .ofType(classOf[String])
      parser.accepts("replicas", "replicas count. Default - -1 (no change)").withRequiredArg()
        .ofType(classOf[Integer])
      parser.accepts("timeout", "timeout (30s, 1m, 1h). 0s - no timeout").withRequiredArg()
        .ofType(classOf[String])
      parser
        .accepts("fixedStartIndex", "index into the broker set to start assigning partitions at.  Default - -1 (random)")
        .withRequiredArg().ofType(classOf[Integer])
      return parser
    }

    private def handleRebalance(
      exprOrStatus: String,
      args: Array[String],
      help: Boolean = false
    ): Unit = {
      val parser = buildRebalanceBaseParser()
      parser
        .accepts("startPartitionId", "partition id to begin assignment at. Default - -1 (random)")
        .withRequiredArg().ofType(classOf[Integer])
      performRebalance("Rebalance", parser, exprOrStatus, args, help)
    }

    private def handleRealign(
      exprOrStatus: String,
      args: Array[String],
      help: Boolean = false
    ): Unit = {
      performRebalance("Realign", buildRebalanceBaseParser(), exprOrStatus, args, help)
    }

    private def performRebalance(
      verb: String,
      parser: OptionParser,
      exprOrStatus: String,
      args: Array[String],
      help: Boolean = false
    ): Unit = {

      val lowercaseVerb = verb.toLowerCase()
      if (help) {
        printLine(s"$verb topics\nUsage: topic $lowercaseVerb <topic-expr>|status [options]\n")
        parser.printHelpOn(out)

        printLine()
        handleGenericOptions(null, help = true)

        printLine()
        printTopicExprExamples()

        printLine()
        printBrokerExprExamples()
        return
      }

      val options = parseOptions(parser, args)

      val broker: String = options.valueOf("broker").asInstanceOf[String]
      val replicas: Integer = options.valueOf("replicas").asInstanceOf[Integer]
      val timeout: String = options.valueOf("timeout").asInstanceOf[String]
      val fixedStartIndex = options.valueOf("fixedStartIndex").asInstanceOf[Integer]
      val startPartitionId = options.valueOf("startPartitionId").asInstanceOf[Integer]

      val params = new util.LinkedHashMap[String, String]()
      if (exprOrStatus != "status") params.put("topic", exprOrStatus)
      if (broker != null) params.put("broker", broker)
      if (replicas != null) params.put("replicas", "" + replicas)
      if (timeout != null) params.put("timeout", timeout)
      if (fixedStartIndex != null) params.put("fixedStartIndex", "" + fixedStartIndex)
      if (startPartitionId != null) params.put("startPartitionId", "" + startPartitionId)
      if (lowercaseVerb == "realign") params.put("realign", "true")

      val json =
        try {
          sendRequestObj[RebalanceStartResponse]("/topic/rebalance", params)
        }
        catch {
          case e: IOException => throw new Error("" + e)
        }

      val status = json.status
      val error = json.error
      val state = json.state

      val is: String = if (status == "idle" || status == "running") "is " else ""
      val colon: String = if (state.isEmpty && error.isEmpty) "" else ":"

      // started|completed|failed|running|idle [(no-op)]|timeout
      if (status == "timeout") throw new Error(s"$verb timeout:\n" + state)
      printLine(s"$verb $is$status$colon ${ error.getOrElse("") }")
      if (error.isEmpty && !state.isEmpty) printLine(state)
    }

    def handlePartitions(expr: String, help: Boolean = false): Unit = {
      if (help) {
        printLine("List partitions\nUsage: topic partition [<topic>]\n")
        handleGenericOptions(null, help = true)

        printLine()
        printTopicExprExamples()

        return
      }

      val params = new util.LinkedHashMap[String, String]
      if (expr != null) params.put("topic", expr)

      val json =
        try {
          sendRequestObj[Map[String, Seq[Partition]]]("/partition/list", params)
        }
        catch {
          case e: IOException => throw new Error("" + e)
        }

      if (json.isEmpty) {
        printLine("topic not found")
        return
      }

      printLine("  part | leader | expected | brokers (*not-isr)", 2)
      val topicList = json.toSeq.sortBy(_._1)
      for ((topic, partitions) <- topicList) {
        printLine(s"$topic:", 1)
        for (p <- partitions.sortBy(_.id)) {
          val isr = p.isr.toSet
          val brokerIsrs = p.replicas.map(b => if (isr.contains(b)) b.toString else s"*$b")
          val displayIsr = s"[${ brokerIsrs.mkString(", ") }]"
          val errorString = if (isr != p.replicas.toSet) "!" else " "

          printLine(f"$errorString ${ p.id }%4d | ${ p.leader }%6d | ${
            p
              .expectedLeader
          }%8d | $displayIsr", 2)
        }
      }
    }

    private def printCmds(): Unit = {
      printLine("Commands:")
      printLine("list       - list topics", 1)
      printLine("add        - add topic", 1)
      printLine("update     - update topic", 1)
      printLine("rebalance  - rebalance topics", 1)
      printLine("realign    - realign topics, keeping existing broker assignments where possible", 1)
      printLine("partitions - list partition details for a topic", 1)
    }

    private def printTopic(topic: Topic, indent: Int): Unit = {
      printLine("name: " + topic.name, indent)
      printLine("partitions: " + topic.partitionsState, indent)
      if (!topic.options.isEmpty) printLine("options: " + Strings.formatMap(topic.options), indent)
    }
  }
}
