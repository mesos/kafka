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

import java.io.File
import ly.stealth.mesos.kafka.cli.Cli.Error
import ly.stealth.mesos.kafka.Config
import ly.stealth.mesos.kafka.Util.BindAddress
import ly.stealth.mesos.kafka.scheduler.ProductionRegistry
import ly.stealth.mesos.kafka.scheduler.mesos.KafkaMesosScheduler
import net.elodina.mesos.util.Period

trait SchedulerCli {
  this: CliUtils =>

  val schedulerCli: CliHandler = new SchedulerCliImpl

  class SchedulerCliImpl extends CliHandler {
    def handle(cmd: String, args: Array[String], help: Boolean = false): Unit = {
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


      parser.accepts("reconciliation-timeout", "Reconciliation timeout (5m, 1h). Default - " + Config.reconciliationTimeout)
        .withRequiredArg().ofType(classOf[String])

      parser.accepts("reconciliation-attempts", "Number of reconciliation attempts before giving up. Default - " + Config.reconciliationAttempts)
        .withRequiredArg().ofType(classOf[Integer])


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

      val options = parseOptions(parser, args)

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

      val reconciliationTimeout = options.valueOf("reconciliation-timeout").asInstanceOf[String]
      if (reconciliationTimeout != null)
        try { Config.reconciliationTimeout = new Period(reconciliationTimeout)  }
        catch { case e: IllegalArgumentException => throw new Error("Invalid reconciliation-timeout") }

      val reconciliationAttempts = options.valueOf("reconciliation-attempts").asInstanceOf[Integer]
      if (reconciliationAttempts != null) Config.reconciliationAttempts = reconciliationAttempts

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

      val registry = new ProductionRegistry()
      KafkaMesosScheduler.start(registry)
    }
  }
}
