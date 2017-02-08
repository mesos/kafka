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

package ly.stealth.mesos.kafka.executor

import java.io.{File, FileInputStream}
import java.net.{URL, URLClassLoader}
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import ly.stealth.mesos.kafka.Util.BindAddress
import ly.stealth.mesos.kafka.Broker
import net.elodina.mesos.util.{IO, Version}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

case class LaunchConfig(
  id: Int,
  options: Map[String, String] = Map(),
  syslog: Boolean = false,
  log4jOptions: Map[String, String] = Map(),
  bindAddress: BindAddress = null,
  defaults: Map[String, String] = Map()) {

  def interpolatedOptions: Map[String, String] = {
    var result = Map[String, String]()
    if (defaults != null)
      result ++= defaults

    result ++= options

    if (bindAddress != null) {
      val addr = bindAddress.resolve()
      result += ("host.name" -> addr)
      if (result.contains("listeners")) {
        result += ("listeners" -> s"PLAINTEXT://$addr:${result("port")}")
      }
    }

    result.mapValues(v => v.replace("$id", id.toString))
  }

  def interpolatedLog4jOptions: Map[String, String] = {
    log4jOptions.mapValues(v => v.replace("$id", id.toString))
  }
}


abstract class BrokerServer {
  def isStarted: Boolean
  def start(launchInfo: LaunchConfig, send: Broker.Metrics => Unit): Broker.Endpoint
  def stop(): Unit
  def waitFor(): Unit
  def getClassLoader: ClassLoader
}

class KafkaServer extends BrokerServer {
  val logger = Logger.getLogger("BrokerServer")
  @volatile var server: Object = _
  @volatile private var collector: MetricCollectorProxy = _
  private val stopping = new AtomicBoolean(false)

  def isStarted: Boolean = server != null

  def start(config: LaunchConfig, send: Broker.Metrics => Unit): Broker.Endpoint = {
    if (isStarted) throw new IllegalStateException("started")

    BrokerServer.distro.configureLog4j(config)
    val options = config.interpolatedOptions
    BrokerServer.distro.startReporters(options)

    logger.info("Starting KafkaServer")
    server = BrokerServer.distro.newServer(options)
    server.getClass.getMethod("startup").invoke(server)

    collector = BrokerServer.distro.startCollector(server, send)

    new Broker.Endpoint(options("host.name"), Integer.parseInt(options("port")))
  }

  def stop(): Unit = {
    val wasStopping = stopping.getAndSet(true)
    if (!isStarted || wasStopping)
      return

    logger.info("Stopping KafkaServer")
    Option(collector).foreach { _.shutdown() }
    Option(server).foreach { s => s.getClass.getMethod("shutdown").invoke(s) }

    waitFor()
    server = null
    collector = null
  }

  def waitFor(): Unit = {
    if (server != null)
      server.getClass.getMethod("awaitShutdown").invoke(server)
  }

  def getClassLoader: ClassLoader = KafkaServer.Distro.loader
}

trait BaseDistro {
  def newServer(options: Map[String, String]): Object
  def startReporters(options: Map[String, String]): Object
  def startCollector(server: AnyRef, send: Broker.Metrics => Unit, interval: Int = 60): MetricCollectorProxy
  def configureLog4j(config: LaunchConfig): Unit
  val dir: File
}

object KafkaServer {
  object Distro extends BaseDistro {
    val (dir, loader) = init()

    // Loader that loads classes in reverse order: 1. from self, 2. from parent.
    // This is required, because current jar have classes incompatible with classes from kafka distro.
    class Loader(urls: Seq[URL]) extends URLClassLoader(urls.toArray) {
      private[this] val snappyHackedClasses = Array[String]("org.xerial.snappy.SnappyNativeAPI", "org.xerial.snappy.SnappyNative", "org.xerial.snappy.SnappyErrorCode")
      private[this] val snappyHackEnabled = checkSnappyVersion

      private def checkSnappyVersion: Boolean = {
        urls
          .map(u => new File(u.getFile).getName)
          .find(_.matches("snappy.*jar"))
          .exists(j => {
          val hIdx = j.lastIndexOf("-")
          val extIdx = j.lastIndexOf(".jar")
          if (hIdx == -1 || extIdx == -1) return false

          val version = new Version(j.substring(hIdx + 1, extIdx))
          version.compareTo(new Version(1,1,0)) <= 0
        })
      }

      override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
        getClassLoadingLock(name) synchronized {
          // Handle Snappy class loading hack:
          // Snappy injects 3 classes and native lib to root ClassLoader
          // See - org.xerial.snappy.SnappyLoader.injectSnappyNativeLoader
          if (snappyHackEnabled && snappyHackedClasses.contains(name))
            return super.loadClass(name, true)

          // Check class is loaded
          var c: Class[_] = findLoadedClass(name)

          // Load from self
          try { if (c == null) c = findClass(name) }
          catch { case e: ClassNotFoundException => }

          // Load from parent
          if (c == null) c = super.loadClass(name, true)

          if (resolve) resolveClass(c)
          c
        }
      }
    }

    def newServer(options: Map[String, String]): Object = {
      val serverClass = loader.loadClass("kafka.server.KafkaServerStartable")
      val configClass = loader.loadClass("kafka.server.KafkaConfig")

      val config: Object = newKafkaConfig(this.props(options, "server.properties"))
      val serverStartable: Object = serverClass.getConstructor(configClass).newInstance(config)
        .asInstanceOf[Object]

      val serverField = serverStartable.getClass.getDeclaredField("server")
      serverField.setAccessible(true)
      val server: Object = serverField.get(serverStartable)
      server
    }

    def startReporters(options: Map[String, String]): Object = {
      val metricsReporter = loader.loadClass("kafka.metrics.KafkaMetricsReporter$").getField("MODULE$").get(null)
      val metricsReporterClass = metricsReporter.getClass

      val props = this.props(options, "server.properties")
      val verifiablePropsClass: Class[_] = loader.loadClass("kafka.utils.VerifiableProperties")
      val verifiableProps: Object = verifiablePropsClass.getConstructor(classOf[Properties]).newInstance(props).asInstanceOf[Object]

      metricsReporterClass.getMethod("startReporters", verifiableProps.getClass).invoke(metricsReporter, verifiableProps)
      metricsReporter
    }

    def startCollector(server: AnyRef, send: Broker.Metrics => Unit, interval: Int = 60): MetricCollectorProxy = {
      val collector = new MetricCollectorProxy(loader, server, send)
      collector.start()
      collector
    }

    private def newKafkaConfig(props: Properties): Object = {
      val configClass = loader.loadClass("kafka.server.KafkaConfig")
      var config: Object = null

      // in kafka <= 0.8.x constructor is KafkaConfig(java.util.Properties)
      try { config = configClass.getConstructor(classOf[Properties]).newInstance(props).asInstanceOf[Object] }
      catch { case e: NoSuchMethodException => }

      if (config == null) {
        // in kafka 0.9.0.0 constructor is KafkaConfig(java.util.Map[_,_])
        val map: util.Map[_,_] = props.toMap.asInstanceOf[Map[_,_]]
        try { config = configClass.getConstructor(classOf[util.Map[String, String]]).newInstance(map).asInstanceOf[Object] }
        catch { case e: NoSuchMethodError => }
      }

      if (config == null) throw new IllegalStateException("Can't create KafkaConfig. Unsupported kafka distro?")
      config
    }

    def configureLog4j(config: LaunchConfig): Unit = {
      if (config.syslog) {
        val pattern = System.getenv("MESOS_SYSLOG_TAG") + ": [%t] %-5p %c %x - %m%n"

        IO.replaceInFile(new File(dir + "/config/log4j.properties"), Map[String, String](
          "log4j.rootLogger=INFO, stdout" ->
            s"""
               |log4j.rootLogger=INFO, stdout, syslog
               |
            |log4j.appender.syslog=org.apache.log4j.net.SyslogAppender
               |log4j.appender.syslog.syslogHost=localhost
               |log4j.appender.syslog.header=true
               |log4j.appender.syslog.layout=org.apache.log4j.PatternLayout
               |log4j.appender.syslog.layout.conversionPattern=$pattern
          """.stripMargin
        ))
      }

      System.setProperty("kafka.logs.dir", "" + new File(dir, "log"))
      val props: Properties = this.props(config.interpolatedLog4jOptions, "log4j.properties")

      val configurator: Class[_] = loader.loadClass("org.apache.log4j.PropertyConfigurator")
      configurator.getMethod("configure", classOf[Properties]).invoke(null, props)
    }

    private def props(options: util.Map[String, String], defaultsFile: String): Properties = {
      val p: Properties = new Properties()
      val stream: FileInputStream = new FileInputStream(dir + "/config/" + defaultsFile)

      try { p.load(stream) }
      finally { stream.close() }

      for ((k, v) <- options)
        if (v != null) p.setProperty(k, v)
        else p.remove(k)

      p
    }

    private def init(): (File, Loader) = {
      // find kafka dir
      new File(".").listFiles().toSeq.find {
        f => f.isDirectory && f.getName.startsWith("kafka")
      }.map { d =>
        val classPath =
          new File(d, "libs").listFiles().map(_.toURI.toURL) ++
            Seq(classOf[MetricCollectorProxy].getProtectionDomain.getCodeSource.getLocation)

        (d, new Loader(classPath))
      }.getOrElse {throw new IllegalStateException("Kafka distribution dir not found") }
    }
  }
}

object BrokerServer {
  var distro: BaseDistro = KafkaServer.Distro
}
