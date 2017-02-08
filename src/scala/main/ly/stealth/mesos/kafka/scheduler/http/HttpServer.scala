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

package ly.stealth.mesos.kafka.scheduler.http

import com.fasterxml.jackson.databind.ObjectMapper
import java.io._
import javax.ws.rs.core.Response
import javax.ws.rs.ext.ContextResolver
import javax.ws.rs.{GET, POST, Path, QueryParam}
import ly.stealth.mesos.kafka._
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.mesos.{ClusterComponent, SchedulerComponent}
import ly.stealth.mesos.kafka.scheduler.{BrokerLifecycleManagerComponent, BrokerLogManagerComponent, HttpApiComponent, KafkaDistributionComponent}
import org.apache.log4j.{Level, Logger}
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.jackson.JacksonFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

trait HttpServerComponent {
  val httpServer: HttpServer

  trait HttpServer {
    def start(): Unit
    def initLogging(): Unit
    def stop(): Unit
  }
}

trait HttpServerComponentImpl extends HttpServerComponent {
  this: ClusterComponent
    with SchedulerComponent
    with KafkaDistributionComponent
    with BrokerLogManagerComponent
    with BrokerLifecycleManagerComponent
    with HttpApiComponent =>

  val httpServer = new HttpServerImpl

  class ApiObjectMapperProvider extends ContextResolver[ObjectMapper] {
    override def getContext(`type`: Class[_]): ObjectMapper = {
      JsonUtil.mapper
    }
  }

  class HttpServerImpl extends HttpServer {
    private val logger = Logger.getLogger("HttpServerImpl")
    private var server: Server = _

    @Path("/")
    class FileServer {
      private def fileResponse(file: File): Response = {
        Response
          .ok(new FileInputStream(file))
          .header("Content-Disposition", "attachment; filename=\"" + file.getName + "\"")
          .header("Content-Length", "" + file.length())
          .`type`("application/zip")
          .build()
      }

      @Path("/jar/{file}")
      @GET
      def downloadJar(): Response = fileResponse(kafkaDistribution.distInfo.jar)

      @Path("/kafka/{file}")
      @GET
      def downloadKafka(): Response = fileResponse(kafkaDistribution.distInfo.kafkaDist)

      @Path("/jre/{file}")
      @GET
      def downloadJre(): Response = {
        if (Config.jre == null)
          Response.status(Response.Status.NOT_FOUND).build()
        else
          fileResponse(Config.jre)
      }
    }

    @Path("/")
    class AdminServer {
      @Path("/quitquitquit")
      @POST
      def qqq(): Response = {
        scheduler.stop()
        Response.ok().build()
      }

      @Path("/quitquitquit")
      @GET
      def qqq_get(): Response = Response.status(Response.Status.METHOD_NOT_ALLOWED).build()

      @Path("/abortabortabort")
      @POST
      def aaa(): Response = {
        scheduler.kill()
        Response.ok().build()
      }

      @Path("/abortabortabort")
      @GET
      def aaa_get(): Response = Response.status(Response.Status.METHOD_NOT_ALLOWED).build()

      @Path("/health")
      @GET
      def health(): String = "ok"

      @Path("/health")
      @POST
      def health_post(): String = health()

      @Path("/loglevel")
      @POST
      def setLogLevel(
        @QueryParam("logger") inLogger: String,
        @QueryParam("level") level: String
      ): Response = {
        val logger = inLogger match {
          case "root" => Logger.getRootLogger
          case "scheduler" => Logger.getLogger("KafkaMesosScheduler")
          case "brokerManager" => Logger.getLogger("BrokerLifecycleManager")
          case l => Logger.getLogger(l)
        }
        logger.setLevel(Level.toLevel(level))
        Response.ok.build()
      }
    }

    private def makeContext(resources: Any*): ServletContextHandler = {
      val ctx = new ServletContextHandler()
      val config = new ResourceConfig()
      resources.foreach({
        case r: Class[_] => config.register(r)
        case r => config.register(r, 0)
      })

      ctx.addServlet(new ServletHolder(new ServletContainer(config)), "/*")
      ctx
    }

    def start() {
      if (server != null) throw new IllegalStateException("started")

      val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
      threadPool.setName("Jetty")

      server = new Server(threadPool)
      val connector = new ServerConnector(server)
      connector.setPort(Config.apiPort)
      if (Config.bindAddress != null) connector.setHost(Config.bindAddress.resolve())
      connector.setIdleTimeout(60 * 1000)

      val api = makeContext(
        brokerApi,
        topicApi,
        partitionApi,
        quotaApi,
        new ApiObjectMapperProvider,
        classOf[BothParamFeature],
        classOf[JacksonFeature]
      )
      api.setContextPath("/api")
      val adminServer = makeContext(new FileServer, new AdminServer)

      val handlers = new HandlerList
      handlers.setHandlers(Seq(api, adminServer).toArray)
      server.setHandler(handlers)
      server.addConnector(connector)
      server.start()

      if (Config.apiPort == 0) Config.replaceApiPort(connector.getLocalPort)
      logger.info("started on port " + connector.getLocalPort)
    }

    def stop() {
      if (server == null) throw new IllegalStateException("!started")

      server.stop()
      server.join()
      server = null

      logger.info("stopped")
    }

    def initLogging(): Unit = {
      System.setProperty("org.eclipse.jetty.util.log.class", classOf[JettyLog4jLogger].getName)
      Logger.getLogger("org.eclipse.jetty").setLevel(Level.WARN)
      Logger.getLogger("Jetty").setLevel(Level.WARN)
    }

    class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
      private var logger: Logger = Logger.getLogger("Jetty")

      def this(logger: Logger) {
        this()
        this.logger = logger
      }

      def isDebugEnabled: Boolean = logger.isDebugEnabled

      def setDebugEnabled(enabled: Boolean) = logger
        .setLevel(if (enabled) Level.DEBUG else Level.INFO)

      def getName: String = logger.getName

      def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger
        .getLogger(name))

      def info(s: String, args: AnyRef*) = logger.info(format(s, args))

      def info(s: String, t: Throwable) = logger.info(s, t)

      def info(t: Throwable) = logger.info("", t)

      def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))

      def debug(s: String, t: Throwable) = logger.debug(format(s, t))

      def debug(msg: String, value: Long): Unit = logger.debug(msg)

      def debug(t: Throwable) = logger.debug("", t)

      def warn(s: String, args: AnyRef*) = logger.warn(format(s, args))

      def warn(s: String, t: Throwable) = logger.warn(s, t)

      def warn(s: String) = logger.warn(s)

      def warn(t: Throwable) = logger.warn("", t)

      def ignore(t: Throwable) = logger.info("Ignored", t)

    }

    private def format(s: String, args: AnyRef*): String = {
      var result: String = ""
      var i: Int = 0

      for (token <- s.split("\\{\\}")) {
        result += token
        if (args.length > i) result += args(i)
        i += 1
      }

      result
    }
  }

}