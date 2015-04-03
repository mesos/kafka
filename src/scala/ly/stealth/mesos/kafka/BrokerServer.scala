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

import org.apache.log4j.Logger
import java.io.{FileInputStream, File}
import java.net.{URL, URLClassLoader}
import java.util.Properties
import java.util
import scala.collection.JavaConversions._

abstract class BrokerServer {
  def isStarted: Boolean
  def start(props: util.Map[String, String] = new util.HashMap()): Unit
  def stop(): Unit
  def waitFor(): Unit
}

class KafkaServer extends BrokerServer {
  val logger = Logger.getLogger(classOf[KafkaServer])
  @volatile var server: Object = null

  def isStarted: Boolean = server != null

  def start(props: util.Map[String, String]): Unit = {
    if (isStarted) throw new IllegalStateException("started")

    server = BrokerServer.Distro.newServer(props)

    logger.info("Starting KafkaServer")
    server.getClass.getMethod("startup").invoke(server)
  }

  def stop(): Unit = {
    if (!isStarted) throw new IllegalStateException("!started")

    logger.info("Stopping KafkaServer")
    server.getClass.getMethod("shutdown").invoke(server)

    waitFor()
    server = null
  }

  def waitFor(): Unit = {
    if (server != null)
      server.getClass.getMethod("awaitShutdown").invoke(server)
  }
}

object BrokerServer {
  object Distro {
    var dir: File = null
    var loader: URLClassLoader = null
    init()

    def newServer(props: util.Map[String, String]): Object = {
      val p: Properties = new Properties()
      val stream: FileInputStream = new FileInputStream(dir + "/config/server.properties")
      try { p.load(stream) }
      finally { stream.close() }

      for ((k, v) <- props) p.setProperty(k, v)

      val serverClass = loader.loadClass("kafka.server.KafkaServerStartable")
      val configClass = loader.loadClass("kafka.server.KafkaConfig")

      val config: Object = configClass.getConstructor(classOf[Properties]).newInstance(p).asInstanceOf[Object]
      val server: Object = serverClass.getConstructor(configClass).newInstance(config).asInstanceOf[Object]

      server
    }

    private def init(): Unit = {
      // find kafka dir
      for (file <- new File(".").listFiles())
        if (file.isDirectory && file.getName.startsWith("kafka"))
          dir = file

      if (dir == null) throw new IllegalStateException("Kafka distribution dir not found")

      // create kafka classLoader
      val classpath = new util.ArrayList[URL]()
      for (file <- new File(dir, "libs").listFiles())
        classpath.add(new URL("" + file.toURI))

      loader = new Loader(classpath.toArray(Array()))

      // configure log4j
      loader.loadClass("org.apache.log4j.PropertyConfigurator").getMethod("configure", classOf[String]).invoke(null, dir + "/config/log4j.properties")
    }
  }

  class Loader(urls: Array[URL]) extends URLClassLoader(urls) {
    override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
      getClassLoadingLock(name) synchronized {
        var c: Class[_] = findLoadedClass(name)

        try { if (c == null) c = findClass(name) }
        catch { case e: ClassNotFoundException => }

        if (c == null) c = super.loadClass(name, true)
        if (resolve) resolveClass(c)
        c
      }
    }
  }
}
