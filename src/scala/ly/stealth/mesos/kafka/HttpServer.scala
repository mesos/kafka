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

import java.io._
import org.apache.log4j.{Level, Logger}
import org.eclipse.jetty.server._
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util._
import org.eclipse.jetty.server.Request
import ly.stealth.mesos.kafka.Broker.State
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSONObject

object HttpServer {
  var jar: File = null
  var kafkaDist: File = null
  var kafkaVersion: Version = null

  val logger = Logger.getLogger(HttpServer.getClass)
  var server: Server = null

  def start(resolveDeps: Boolean = true) {
    if (server != null) throw new IllegalStateException("started")
    if (resolveDeps) this.resolveDeps

    val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
    threadPool.setName("Jetty")

    server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(Config.apiPort)
    if (Config.bindAddress != null) connector.setHost(Config.bindAddress.resolve())
    connector.setIdleTimeout(60 * 1000)

    val handler = new ServletContextHandler
    handler.addServlet(new ServletHolder(new Servlet()), "/")
    handler.setErrorHandler(new ErrorHandler())

    server.setHandler(handler)
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

  private def resolveDeps: Unit = {
    val jarMask: String = "kafka-mesos.*\\.jar"
    val kafkaMask: String = "kafka.*\\.tgz"

    for (file <- new File(".").listFiles()) {
      if (file.getName.matches(jarMask)) jar = file
      if (file.getName.matches(kafkaMask)) kafkaDist = file
    }

    if (jar == null) throw new IllegalStateException(jarMask + " not found in current dir")
    if (kafkaDist == null) throw new IllegalStateException(kafkaMask + " not found in in current dir")

    // extract version: "kafka-dist-1.2.3.tgz" => "1.2.3"
    val distName: String = kafkaDist.getName
    val tgzIdx = distName.lastIndexOf(".tgz")
    val hIdx = distName.lastIndexOf("-")
    if (tgzIdx == -1 || hIdx == -1) throw new IllegalStateException("Can't extract version number from " + distName)
    kafkaVersion = new Version(distName.substring(hIdx + 1, tgzIdx))
  }

  private class Servlet extends HttpServlet {
    override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = doGet(request, response)
    override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val url = request.getRequestURL + (if (request.getQueryString != null) "?" + request.getQueryString else "")
      logger.info("handling - " + url)

      try {
        handle(request, response)
        logger.info("finished handling")
      } catch {
        case e: Exception =>
          logger.error("error handling", e)
          response.sendError(500, "" + e)
      }
    }

    def handle(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val uri = request.getRequestURI
      if (uri.startsWith("/jar/")) downloadFile(HttpServer.jar, response)
      else if (uri.startsWith("/kafka/")) downloadFile(HttpServer.kafkaDist, response)
      else if (uri.startsWith("/jre/") && Config.jre != null) downloadFile(Config.jre, response)
      else if (uri.startsWith("/api/broker")) handleBrokerApi(request, response)
      else if (uri.startsWith("/api/topic")) handleTopicApi(request, response)
      else if (uri.startsWith("/api/partition")) handlePartitionApi(request, response)
      else if (uri.startsWith("/health")) handleHealth(response)
      else if (uri.startsWith("/quitquitquit")) handleQuit(request, response)
      else if (uri.startsWith("/abortabortabort")) handleAbort(request, response)
      else response.sendError(404, "uri not found")
    }

    def downloadFile(file: File, response: HttpServletResponse): Unit = {
      response.setContentType("application/zip")
      response.setHeader("Content-Length", "" + file.length())
      response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName + "\"")
      IO.copyAndClose(new FileInputStream(file), response.getOutputStream)
    }

    def handleHealth(response: HttpServletResponse): Unit = {
      response.setContentType("text/plain; charset=utf-8")
      response.getWriter.println("ok")
    }

    def handleBrokerApi(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      request.setAttribute("jsonResponse", true)
      response.setContentType("application/json; charset=utf-8")
      var uri: String = request.getRequestURI.substring("/api/broker".length)
      if (uri.startsWith("/")) uri = uri.substring(1)

      if (uri == "list") handleListBrokers(request, response)
      else if (uri == "add" || uri == "update") handleAddUpdateBroker(request, response)
      else if (uri == "remove") handleRemoveBroker(request, response)
      else if (uri == "start" || uri == "stop") handleStartStopBroker(request, response)
      else if (uri == "restart") handleRestartBroker(request, response)
      else if (uri == "log") handleBrokerLog(request, response)
      else if (uri == "clone") handleCloneBroker(request, response)
      else response.sendError(404, "uri not found")
    }

    def handleListBrokers(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster = Scheduler.cluster

      var expr = request.getParameter("broker")
      if (expr == null) expr = "*"

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val brokerNodes = new ListBuffer[JSONObject]()
      for (id <- ids) {
        val broker = cluster.getBroker(id)
        if (broker != null) brokerNodes.add(cluster.getBroker(id).toJson())
      }

      response.getWriter.println("" + new JSONObject(Map("brokers" -> new JSONArray(brokerNodes.toList))))
    }

    def handleAddUpdateBroker(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster = Scheduler.cluster
      val add: Boolean = request.getRequestURI.endsWith("add")
      val errors = new util.ArrayList[String]()

      val expr: String = request.getParameter("broker")
      if (expr == null || expr.isEmpty) errors.add("broker required")

      var cpus: java.lang.Double = null
      if (request.getParameter("cpus") != null)
        try { cpus = java.lang.Double.valueOf(request.getParameter("cpus")) }
        catch { case e: NumberFormatException => errors.add("Invalid cpus") }

      var mem: java.lang.Long = null
      if (request.getParameter("mem") != null)
        try { mem = java.lang.Long.valueOf(request.getParameter("mem")) }
        catch { case e: NumberFormatException => errors.add("Invalid mem") }

      var heap: java.lang.Long = null
      if (request.getParameter("heap") != null)
        try { heap = java.lang.Long.valueOf(request.getParameter("heap")) }
        catch { case e: NumberFormatException => errors.add("Invalid heap") }

      val port: String = request.getParameter("port")
      if (port != null && port != "")
        try { new Range(request.getParameter("port")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid port") }

      val volume: java.lang.String = request.getParameter("volume")

      val bindAddress: String = request.getParameter("bindAddress")
      if (bindAddress != null)
        try { new BindAddress(request.getParameter("bindAddress")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid bindAddress") }

      var syslog: java.lang.Boolean = null
      if (request.getParameter("syslog") != null)
        syslog = java.lang.Boolean.valueOf(request.getParameter("syslog"))

      var stickinessPeriod: Period = null
      if (request.getParameter("stickinessPeriod") != null)
        try { stickinessPeriod = new Period(request.getParameter("stickinessPeriod")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid stickinessPeriod") }

      var options: util.Map[String, String] = null
      if (request.getParameter("options") != null)
        try { options = Strings.parseMap(request.getParameter("options")).filterKeys(Broker.isOptionOverridable).view.force }
        catch { case e: IllegalArgumentException => errors.add("Invalid options: " + e.getMessage) }

      var log4jOptions: util.Map[String, String] = null
      if (request.getParameter("log4jOptions") != null)
        try { log4jOptions = Strings.parseMap(request.getParameter("log4jOptions")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid log4jOptions: " + e.getMessage) }

      val jvmOptions: String = request.getParameter("jvmOptions")

      var constraints: util.Map[String, Constraint] = null
      if (request.getParameter("constraints") != null)
        try { constraints = Strings.parseMap(request.getParameter("constraints")).mapValues(new Constraint(_)).view.force }
        catch { case e: IllegalArgumentException => errors.add("Invalid constraints: " + e.getMessage) }


      var failoverDelay: Period = null
      if (request.getParameter("failoverDelay") != null)
        try { failoverDelay = new Period(request.getParameter("failoverDelay")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid failoverDelay") }

      var failoverMaxDelay: Period = null
      if (request.getParameter("failoverMaxDelay") != null)
        try { failoverMaxDelay = new Period(request.getParameter("failoverMaxDelay")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid failoverMaxDelay") }

      val failoverMaxTries: String = request.getParameter("failoverMaxTries")
      if (failoverMaxTries != null && failoverMaxTries != "")
        try { Integer.valueOf(failoverMaxTries) }
        catch { case e: NumberFormatException => errors.add("Invalid failoverMaxTries") }


      if (!errors.isEmpty) { response.sendError(400, errors.mkString("; ")); return }

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val brokers = new util.ArrayList[Broker]()

      for (id <- ids) {
        var broker = cluster.getBroker(id)

        if (add)
          if (broker != null) errors.add(s"Broker $id already exists")
          else broker = new Broker(id)
        else
          if (broker == null) errors.add(s"Broker $id not found")

        brokers.add(broker)
      }

      if (!errors.isEmpty) { response.sendError(400, errors.mkString("; ")); return }

      for (broker <- brokers) {
        if (cpus != null) broker.cpus = cpus
        if (mem != null) broker.mem = mem
        if (heap != null) broker.heap = heap
        if (port != null) broker.port = if (port != "") new Range(port) else null
        if (volume != null) broker.volume = if (volume != "") volume else null
        if (bindAddress != null) broker.bindAddress = if (bindAddress != "") new BindAddress(bindAddress) else null
        if (syslog != null) broker.syslog = syslog
        if (stickinessPeriod != null) broker.stickiness.period = stickinessPeriod

        if (constraints != null) broker.constraints = constraints
        if (options != null) broker.options = options
        if (log4jOptions != null) broker.log4jOptions = log4jOptions
        if (jvmOptions != null) broker.jvmOptions = if (jvmOptions != "") jvmOptions else null

        if (failoverDelay != null) broker.failover.delay = failoverDelay
        if (failoverMaxDelay != null) broker.failover.maxDelay = failoverMaxDelay
        if (failoverMaxTries != null) broker.failover.maxTries = if (failoverMaxTries != "") Integer.valueOf(failoverMaxTries) else null

        if (add) cluster.addBroker(broker)
        else if (broker.active || broker.task != null) broker.needsRestart = true
      }
      cluster.save()

      val brokerNodes = new ListBuffer[JSONObject]()
      for (broker <- brokers) brokerNodes.add(broker.toJson())

      response.getWriter.println("" + new JSONObject(Map("brokers" -> new JSONArray(brokerNodes.toList))))
    }

    def handleRemoveBroker(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster = Scheduler.cluster

      val expr = request.getParameter("broker")
      if (expr == null) { response.sendError(400, "broker required"); return }

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val brokers = new util.ArrayList[Broker]()
      for (id <- ids) {
        val broker = Scheduler.cluster.getBroker(id)
        if (broker == null) { response.sendError(400, s"broker $id not found"); return }
        if (broker.active) { response.sendError(400, s"broker $id is active"); return }
        brokers.add(broker)
      }

      brokers.foreach(cluster.removeBroker)
      cluster.save()

      val result = new collection.mutable.LinkedHashMap[String, Any]()
      result("ids") = ids.mkString(",")

      response.getWriter.println(JSONObject(result.toMap))
    }

    def handleStartStopBroker(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster: Cluster = Scheduler.cluster
      val start: Boolean = request.getRequestURI.endsWith("start")

      var timeout: Period = new Period("60s")
      if (request.getParameter("timeout") != null)
        try { timeout = new Period(request.getParameter("timeout")) }
        catch { case ignore: IllegalArgumentException => response.sendError(400, "invalid timeout"); return }

      val force: Boolean = request.getParameter("force") != null

      val expr: String = request.getParameter("broker")
      if (expr == null) { response.sendError(400, "broker required"); return }

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val brokers = new util.ArrayList[Broker]()
      for (id <- ids) {
        val broker = cluster.getBroker(id)
        if (broker == null) { response.sendError(400, "broker " + id + " not found"); return }
        if (!force && broker.active == start) { response.sendError(400, "broker " + id + " is" + (if (start) "" else " not") +  " active"); return }
        brokers.add(broker)
      }

      for (broker <- brokers) {
        broker.active = start
        broker.failover.resetFailures()
        if (!start && force) Scheduler.forciblyStopBroker(broker)
      }
      cluster.save()

      def waitForBrokers(): String = {
        if (timeout.ms == 0) return "scheduled"

        for (broker <- brokers)
          if (!broker.waitFor(if (start) State.RUNNING else null, timeout))
            return "timeout"

        if (start) "started" else "stopped"
      }

      val status = waitForBrokers()
      val brokerNodes = new ListBuffer[JSONObject]()

      for (broker <- brokers) brokerNodes.add(broker.toJson())
      response.getWriter.println(JSONObject(Map("status" -> status, "brokers" -> new JSONArray(brokerNodes.toList))))
    }

    def handleRestartBroker(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster: Cluster = Scheduler.cluster

      var timeout: Period = new Period("2m")
      if (request.getParameter("timeout") != null)
        try { timeout = new Period(request.getParameter("timeout")) }
        catch { case ignore: IllegalArgumentException => response.sendError(400, "invalid timeout"); return }

      val expr: String = request.getParameter("broker")
      if (expr == null) { response.sendError(400, "broker required"); return }

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val brokers = new util.ArrayList[Broker]()
      for (id <- ids) {
        val broker = cluster.getBroker(id)
        if (broker == null) { response.sendError(400, s"broker $id not found"); return }
        if (!broker.active || broker.task == null || !broker.task.running) { response.sendError(400, s"broker $id is not running"); return }
        brokers.add(broker)
      }

      def timeoutJson(broker: Broker, stage: String): JSONObject =
        new JSONObject(Map("status" -> "timeout", "message" -> s"broker ${broker.id} timeout on $stage"))

      for (broker <- brokers) {
        if (!broker.active || broker.task == null || !broker.task.running) { response.sendError(400, s"broker ${broker.id} is not running"); return }

        // stop
        broker.active = false
        broker.failover.resetFailures()
        val begin = System.currentTimeMillis()
        cluster.save()

        if (!broker.waitFor(null, timeout)) { response.getWriter.println("" + timeoutJson(broker, "stop")); return }

        val startTimeout = new Period(Math.max(timeout.ms - (System.currentTimeMillis() - begin), 0L) + "ms")

        // start
        broker.active = true
        cluster.save()

        if (!broker.waitFor(State.RUNNING, startTimeout)) { response.getWriter.println("" + timeoutJson(broker, "start")); return }
      }

      response.getWriter.println(JSONObject(Map("status" -> "restarted", "brokers" -> new JSONArray(brokers.map(_.toJson()).toList))))
    }

    def handleBrokerLog(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster: Cluster = Scheduler.cluster

      var timeout: Period = new Period("30s")
      if (request.getParameter("timeout") != null)
        try { timeout = new Period(request.getParameter("timeout")) }
        catch { case ignore: IllegalArgumentException => response.sendError(400, "invalid timeout"); return }

      val id: String = request.getParameter("broker")
      if (id == null) { response.sendError(400, "broker required"); return }

      var name: String = request.getParameter("name")
      if (name == null) name = "stdout"

      var lines: Int = 100
      if (request.getParameter("lines") != null)
        try { lines = Integer.parseInt(request.getParameter("lines")) }
        catch { case e: NumberFormatException => response.sendError(400, "invalid lines"); return  }
      if (lines <= 0) { response.sendError(400, "lines has to be greater than 0"); return }

      val broker = cluster.getBroker(id)
      if (broker == null) { response.sendError(400, "broker " + id + " not found"); return }
      if (!broker.active) { response.sendError(400, "broker " + id + " is not active"); return }
      if (broker.task == null || !broker.task.running) { response.sendError(400, "broker " + id + " is not running"); return }

      val requestId = Scheduler.requestBrokerLog(broker, name, lines)

      if (requestId == -1) { response.sendError(500, "disconnected from the master"); return }

      def receivedLog: Boolean = Scheduler.receivedLog(requestId)

      def waitForLog(): String = {
        var t = timeout.ms
        while (t > 0 && !receivedLog) {
          val delay = Math.min(100, t)
          Thread.sleep(delay)
          t -= delay
        }

        if (receivedLog) "ok" else "timeout"
      }

      val status = waitForLog()
      val content = if (status == "ok") Scheduler.logContent(requestId) else ""

      Scheduler.removeLog(requestId)

      response.getWriter.println(JSONObject(Map("status" -> status, "content" -> content)))
    }

    def handleCloneBroker(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster: Cluster = Scheduler.cluster

      val expr: String = request.getParameter("broker")
      if (expr == null) { response.sendError(400, "broker required"); return }

      var ids: util.List[String] = null
      try { ids = Expr.expandBrokers(cluster, expr) }
      catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      val sourceBrokerId: String = request.getParameter("source")
      if (sourceBrokerId == null) { response.sendError(400, "source broker required"); return }

      val sourceBroker = cluster.getBroker(sourceBrokerId)
      if (sourceBroker == null) { response.sendError(400, s"broker $sourceBrokerId not found"); return }

      val brokerNodes = new ListBuffer[JSONObject]()
      for (id <- ids) {
        val newBroker = sourceBroker.clone(id)
        cluster.addBroker(newBroker)

        brokerNodes.add(newBroker.toJson())
      }
      cluster.save()

      response.getWriter.println("" + new JSONObject(Map("brokers" -> new JSONArray(brokerNodes.toList))))
    }

    def handleTopicApi(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      request.setAttribute("jsonResponse", true)
      response.setContentType("application/json; charset=utf-8")
      var uri: String = request.getRequestURI.substring("/api/topic".length)
      if (uri.startsWith("/")) uri = uri.substring(1)

      if (uri == "list") handleListTopics(request, response)
      else if (uri == "add" || uri == "update") handleAddUpdateTopic(request, response)
      else if (uri == "rebalance") handleTopicRebalance(request, response)
      else response.sendError(404, "uri not found")
    }

    def handleListTopics(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val topics: Topics = Scheduler.cluster.topics

      var expr = request.getParameter("topic")
      if (expr == null) expr = "*"
      val names: util.List[String] = Expr.expandTopics(expr)

      val topicNodes = new ListBuffer[JSONObject]()
      for (topic <- topics.getTopics)
        if (names.contains(topic.name))
          topicNodes.add(topic.toJson)

      response.getWriter.println("" + new JSONObject(Map("topics" -> new JSONArray(topicNodes.toList))))
    }

    def handleAddUpdateTopic(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val topics: Topics = Scheduler.cluster.topics
      val add: Boolean = request.getRequestURI.endsWith("add")
      val errors = new util.ArrayList[String]()

      val topicExpr: String = request.getParameter("topic")
      if (topicExpr == null || topicExpr.isEmpty) errors.add("topic required")
      val topicNames: util.List[String] = Expr.expandTopics(topicExpr)
      
      var brokerIds: util.List[Int] = null
      if (request.getParameter("broker") != null)
        try { brokerIds = Expr.expandBrokers(Scheduler.cluster, request.getParameter("broker"), sortByAttrs = true).map(Integer.parseInt) }
        catch { case e: IllegalArgumentException => errors.add("Invalid broker-expr") }

      var partitions: Int = 1
      if (add && request.getParameter("partitions") != null)
        try { partitions = Integer.parseInt(request.getParameter("partitions")) }
        catch { case e: NumberFormatException => errors.add("Invalid partitions") }

      var replicas: Int = 1
      if (add && request.getParameter("replicas") != null)
        try { replicas = Integer.parseInt(request.getParameter("replicas")) }
        catch { case e: NumberFormatException => errors.add("Invalid replicas") }

      var options: util.Map[String, String] = null
      if (request.getParameter("options") != null)
        try { options = Strings.parseMap(request.getParameter("options")) }
        catch { case e: IllegalArgumentException => errors.add("Invalid options: " + e.getMessage) }

      if (!add && options == null)
        errors.add("options required")

      val optionErr: String = if (options != null) topics.validateOptions(options) else null
      if (optionErr != null) errors.add(optionErr)

      for (name <- topicNames) {
        val topic = topics.getTopic(name)
        if (add && topic != null) errors.add(s"Topic $name already exists")
        if (!add && topic == null) errors.add(s"Topic $name not found")
      }

      if (!errors.isEmpty) { response.sendError(400, errors.mkString("; ")); return }

      val topicNodes= new ListBuffer[JSONObject]
      for (name <- topicNames) {
        if (add) topics.addTopic(name, topics.fairAssignment(partitions, replicas, brokerIds), options)
        else topics.updateTopic(topics.getTopic(name), options)

        topicNodes.add(topics.getTopic(name).toJson)
      }

      response.getWriter.println(JSONObject(Map("topics" -> new JSONArray(topicNodes.toList))))
    }

    def handleTopicRebalance(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val cluster: Cluster = Scheduler.cluster
      val rebalancer: Rebalancer = cluster.rebalancer

      val topicExpr = request.getParameter("topic")
      var topics: util.List[String] = null
      if (topicExpr != null)
        try { topics = Expr.expandTopics(topicExpr)}
        catch { case e: IllegalArgumentException => response.sendError(400, "invalid topics"); return }

      if (topics != null && rebalancer.running) { response.sendError(400, "rebalance is already running"); return }
      if (topics != null && topics.isEmpty) { response.sendError(400, "no topics specified"); return }

      val brokerExpr: String = if (request.getParameter("broker") != null) request.getParameter("broker") else "*"
      var brokers: util.List[String] = null
      if (brokerExpr != null)
        try { brokers = Expr.expandBrokers(cluster, brokerExpr, sortByAttrs = true) }
        catch { case e: IllegalArgumentException => response.sendError(400, "invalid broker-expr"); return }

      var timeout: Period = new Period("0")
      if (request.getParameter("timeout") != null)
        try { timeout = new Period(request.getParameter("timeout")) }
        catch { case e: IllegalArgumentException => response.sendError(400, "invalid timeout"); return }

      var replicas: Int = -1
      if (request.getParameter("replicas") != null)
        try { replicas = Integer.parseInt(request.getParameter("replicas")) }
        catch { case e: NumberFormatException => response.sendError(400, "invalid replicas"); return  }


      def startRebalance: (String, String) = {
        try { rebalancer.start(topics, brokers, replicas) }
        catch { case e: Rebalancer.Exception => return ("failed", e.getMessage) }

        if (timeout.ms > 0)
          if (!rebalancer.waitFor(running = false, timeout)) return ("timeout", null)
          else return ("completed", null)

        ("started", null)
      }

      var status: String = null
      var error: String = null

      if (topics != null) {
        val result: (String, String) = startRebalance
        status = result._1
        error = result._2
      } else
        status = if (rebalancer.running) "running" else "idle"

      val result = new collection.mutable.LinkedHashMap[String, Any]()
      result("status") = status
      if (error != null) result("error") = error
      result("state") = rebalancer.state

      response.getWriter.println(JSONObject(result.toMap))
    }

    def handlePartitionApi(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      request.setAttribute("jsonResponse", true)
      response.setContentType("application/json; charset=utf-8")
      var uri: String = request.getRequestURI.substring("/api/partition".length)
      if (uri.startsWith("/")) uri = uri.substring(1)

      if (uri == "list") handleListPartitions(request, response)
      else response.sendError(404, "uri not found")
    }

    def handleListPartitions(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val topicExpr = request.getParameter("topic")
      var topics: util.List[String] = null
      if (topicExpr != null)
        try { topics = Expr.expandTopics(topicExpr)}
        catch { case e: IllegalArgumentException => response.sendError(400, "invalid topics"); return }

      val topicsAndPartitions = Scheduler.cluster.topics.getPartitions(topics)
      response.getWriter.println(
        new JSONObject(
          topicsAndPartitions.mapValues(
            v => new JSONArray(v.map(_.toJson).toList)
          ).toMap
        )
      )
    }

    def handleQuit(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      if (request.getMethod != "POST") {
        response.sendError(405, "wrong method")
      }
      Scheduler.stop()
    }

    def handleAbort(request: HttpServletRequest, response: HttpServletResponse): Unit = {
      if (request.getMethod != "POST") {
        response.sendError(405, "wrong method")
      }
      Scheduler.kill()
    }
  }

  class ErrorHandler extends handler.ErrorHandler () {
    override def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val code: Int = response.getStatus
      val error: String = response match {
        case response: Response => response.getReason
        case _ => ""
      }

      val writer: PrintWriter = response.getWriter

      if (request.getAttribute("jsonResponse") != null) {
        response.setContentType("application/json; charset=utf-8")
        writer.println("" + new JSONObject(Map("code" -> code, "error" -> error)))
      } else {
        response.setContentType("text/plain; charset=utf-8")
        writer.println(code + " - " + error)
      }

      writer.flush()
      baseRequest.setHandled(true)
    }
  }

  class JettyLog4jLogger extends org.eclipse.jetty.util.log.Logger {
    private var logger: Logger = Logger.getLogger("Jetty")

    def this(logger: Logger) {
      this()
      this.logger = logger
    }

    def isDebugEnabled: Boolean = logger.isDebugEnabled
    def setDebugEnabled(enabled: Boolean) = logger.setLevel(if (enabled) Level.DEBUG else Level.INFO)

    def getName: String = logger.getName
    def getLogger(name: String): org.eclipse.jetty.util.log.Logger = new JettyLog4jLogger(Logger.getLogger(name))

    def info(s: String, args: AnyRef*) = logger.info(format(s, args))
    def info(s: String, t: Throwable) = logger.info(s, t)
    def info(t: Throwable) = logger.info("", t)

    def debug(s: String, args: AnyRef*) = logger.debug(format(s, args))
    def debug(s: String, t: Throwable) = logger.debug(s, t)

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
