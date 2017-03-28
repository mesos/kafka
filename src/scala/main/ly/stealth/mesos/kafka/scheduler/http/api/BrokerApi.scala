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
package ly.stealth.mesos.kafka.scheduler.http.api

import java.lang.{Boolean => JBool, Double => JDouble, Integer => JInt, Long => JLong}
import java.util.concurrent.{TimeUnit, TimeoutException}
import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.{Produces, _}
import ly.stealth.mesos.kafka.Broker.{Container, ContainerType, ExecutionOptions, Mount, State}
import ly.stealth.mesos.kafka.Util.BindAddress
import ly.stealth.mesos.kafka._
import ly.stealth.mesos.kafka.RunnableConversions._
import ly.stealth.mesos.kafka.scheduler.http.BothParam
import ly.stealth.mesos.kafka.scheduler.mesos.{ClusterComponent, EventLoopComponent, SchedulerComponent}
import ly.stealth.mesos.kafka.scheduler.{BrokerLifecycleManagerComponent, BrokerState, Expr, ZkUtilsWrapper}
import net.elodina.mesos.util.{Period, Range}
import org.apache.log4j.Logger
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.collection.mutable

trait BrokerApiComponent {
  val brokerApi: BrokerApi
  trait BrokerApi {}
}

trait BrokerApiComponentImpl extends BrokerApiComponent {
  this: ClusterComponent
    with BrokerLifecycleManagerComponent
    with SchedulerComponent
    with EventLoopComponent =>

  val brokerApi: BrokerApi = new BrokerApiImpl

  @Path("/broker")
  class BrokerApiImpl extends BrokerApi {
    private[this] val logger = Logger.getLogger("BrokerApi")

    @Path("list")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def listBrokers(@BothParam("broker") broker: String): Response = {
      val brokers =
        Try(Expr.expandBrokers(cluster, Option(broker).getOrElse("*")))
          .map(_.map(cluster.getBroker).filter(_ != null))

      brokers match {
        case Success(brokerNodes) =>
          Response.ok(BrokerStatusResponse(brokerNodes))
            .build()
        case Failure(e) =>
          Response.status(Response.Status.BAD_REQUEST).build()
      }
    }

    @Path("list")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    def listBrokersGet(@BothParam("broker") broker: String) = listBrokers(broker)

    @Path("{op: (add|update)}")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def addBroker(
      @PathParam("op") operation: String,
      @BothParam("broker") broker: String,
      @BothParam("cpus") cpus: JDouble,
      @BothParam("mem") mem: JLong,
      @BothParam("heap") heap: JLong,
      @BothParam("port") port: Range,
      @BothParam("volume") volume: String,
      @BothParam("bindAddress") bindAddress: BindAddress,
      @BothParam("syslog") syslog: JBool,
      @BothParam("stickinessPeriod") stickinessPeriod: Period,
      @BothParam("options") options: StringMap,
      @BothParam("log4jOptions") log4jOptions: StringMap,
      @BothParam("jvmOptions") jvmOptions: String,
      @BothParam("constraints") constraints: ConstraintMap,
      @BothParam("failoverDelay") failoverDelay: Period,
      @BothParam("failoverMaxDelay") failoverMaxDelay: Period,
      @BothParam("failoverMaxTries") failoverMaxTries: JInt,
      @BothParam("javaCmd") javaCmd: String,
      @BothParam("containerType") containerTypeStr: String,
      @BothParam("containerImage") containerImage: String,
      @BothParam("containerMounts") containerMounts: String
    ): Response = {
      val add = operation == "add"
      val errors = mutable.Buffer[String]()
      if (broker == null) {
        errors.append("broker required")
      }

      val mounts = try {
        containerMounts match {
          case "" => Some(Seq())
          case null => None
          case m => Some(m.split(',').map(Mount.parse).toSeq)
        }
      } catch {
        case e: IllegalArgumentException =>
          errors.append(e.getMessage)
          None
      }
      val containerType =
        if (containerTypeStr != null)
          Some(ContainerType.valueOf(containerTypeStr))
        else
          None

      val ids = Expr.expandBrokers(cluster, broker)
      val brokers = mutable.Buffer[Broker]()

      for (id <- ids) {
        var broker = cluster.getBroker(id)

        if (add)
          if (broker != null) errors.append(s"Broker $id already exists")
          else broker = new Broker(id)
        else if (broker == null) errors.append(s"Broker $id not found")

        brokers.append(broker)
      }

      if (errors.nonEmpty) {
        return Status.BadRequest(errors.mkString("; '"))
      }

      for (broker <- brokers) {
        if (cpus != null) broker.cpus = cpus
        if (mem != null) broker.mem = mem
        if (heap != null) broker.heap = heap
        if (port != null) broker.port = port
        if (volume != null) broker.volume = volume
        if (bindAddress != null) broker.bindAddress = bindAddress
        if (syslog != null) broker.syslog = syslog
        if (stickinessPeriod != null) broker.stickiness.period = stickinessPeriod

        if (constraints != null) broker.constraints = constraints.toMap
        if (options != null) broker.options = options.toMap
        if (log4jOptions != null) broker.log4jOptions = log4jOptions.toMap
        if (jvmOptions != null)
          broker.executionOptions = broker.executionOptions.copy(jvmOptions = jvmOptions)

        if (failoverDelay != null) broker.failover.delay = failoverDelay
        if (failoverMaxDelay != null) broker.failover.maxDelay = failoverMaxDelay
        if (failoverMaxTries != null) broker.failover.maxTries = failoverMaxTries
        if (javaCmd != null)
          broker.executionOptions = broker.executionOptions.copy(javaCmd = javaCmd)

        if (containerImage == "" || containerImage == "none") {
          broker.executionOptions = broker.executionOptions.copy(container = None)
        } else if (containerImage != null) {
          broker.executionOptions = broker.executionOptions.copy(
            container =
              broker.executionOptions.container
                .map(_.copy(name = containerImage))
                .orElse(
                  Some(Container(ctype = ContainerType.Docker, name = containerImage))))
        }
        containerType.zip(broker.executionOptions.container).foreach {
          case (ct, c) => broker.executionOptions = broker.executionOptions.copy(
            container = Some(c.copy(ctype = ct))
          )
        }

        val newMounts = (broker.executionOptions.container.map(_.mounts), mounts) match {
          // New list set, overwrite
          case (_, Some(n)) => n
          // No new list set, but existing one is, use the old one
          case (Some(o), None) => o
          // Neither old nor new set one, initialize to empty list.
          case (None, None) => Seq()
        }
        broker.executionOptions = broker.executionOptions.copy(
          container = broker.executionOptions.container.map(_.copy(mounts = newMounts))
        )

        if (add) cluster.addBroker(broker)
        else if (broker.active || broker.task != null) broker.needsRestart = true
      }
      cluster.save()

      val resp = BrokerStatusResponse(brokers)
      Response.status(Response.Status.OK).entity(resp).build()
    }

    private val noFn = new PartialFunction[Broker, Broker] {
      override def isDefinedAt(x: Broker): Boolean = false
      override def apply(v1: Broker): Broker = ???
    }

    private def expandBrokers(expr: String, rejector: PartialFunction[Broker, Broker] = noFn): Try[Seq[Broker]] = {
      val brokers = Try(Expr.expandBrokers(cluster, expr))
        .flatMap(ids => Try(
          ids.map(id => {
            val broker = cluster.getBroker(id)
            if (broker == null)
              throw new IllegalArgumentException(s"broker $id not found")
            if (rejector.isDefinedAt(broker))
              rejector(broker)
            else
              broker
          })))
      brokers
    }

    @Path("remove")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def removeBroker(@BothParam("broker") expr: String): Response = {
      val brokers = expandBrokers(expr, {
        case b if b.active => throw new IllegalArgumentException(s"broker ${b.id} is active.")
      })
      brokers match {
        case Success(b) =>
          b.foreach(cluster.removeBroker)
          cluster.save()
          Response.status(Response.Status.OK)
            .entity(BrokerRemoveResponse(b.map(_.id.toString)))
            .build()
        case Failure(e) => Status.BadRequest(e.getMessage)
      }
    }

    @Path("{op: (start|stop)}")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def startStopBroker(
      @PathParam("op") operation: String,
      @DefaultValue("60s") @BothParam("timeout") timeout: Period,
      @BothParam("force") force: Boolean,
      @BothParam("broker") expr: String
    ): Response = {
      logger.info(s"Handling $operation for broker $expr")

      val start = operation == "start"
      val maybeBrokers = expandBrokers(expr)

      maybeBrokers.map(brokers => startStopBrokersImpl(brokers, start, timeout, force)) match {
        case Success(s) => s
        case Failure(e) => Status.BadRequest(e.getMessage)
      }
    }

    private def startStopBrokersImpl(brokers: Seq[Broker], start: Boolean, timeout: Period, force: Boolean) = {
      eventLoop.submit(() =>
        for (broker <- brokers) {
          if (start) {
            brokerLifecycleManager.tryTransition(broker, BrokerState.Active())
          } else {
            brokerLifecycleManager.tryTransition(broker, BrokerState.Inactive(force))
          }
          broker.failover.resetFailures()
        }).get
      cluster.save()

      def waitForBrokers(): String = {
        if (timeout.ms == 0) return "scheduled"

        for (broker <- brokers)
          if (!broker.waitFor(if (start) State.RUNNING else null, timeout))
            return "timeout"

        if (start) "started" else "stopped"
      }

      val status = waitForBrokers()
      val resp = BrokerStartResponse(brokers, status)
      Response.status(Response.Status.OK).entity(resp).build()
    }

    @Path("restart")
    @POST
    @Produces(Array(MediaType.APPLICATION_JSON))
    def restartBroker(
      @BothParam("noWaitForReplication") noWaitForReplication: JBool,
      @BothParam("timeout") givenTimeout: Period,
      @BothParam("broker") expr: String
    ): Response = {
      val shouldWaitForRepl = noWaitForReplication == null
      val timeout = Option(givenTimeout).getOrElse({
        new Period(if (shouldWaitForRepl) "5m" else "2m")
      })
      val maybeBrokers = expandBrokers(expr)
      maybeBrokers.map(b => restartBrokersImpl(b, timeout, shouldWaitForRepl)) match {
        case Success(s) => s
        case Failure(e) => Status.BadRequest(e.getMessage)
      }
    }

    private def restartBrokersImpl(brokers: Seq[Broker], timeout: Period, waitForRepl: Boolean): Response = {
      def timeoutJson(broker: Broker, stage: String) =
        Response.status(Response.Status.OK).entity(
          BrokerStartResponse(Seq(), "timeout", Some(s"broker ${broker.id} timeout on $stage"))
        ).build()

      for (broker <- brokers) {
        val begin = System.currentTimeMillis()
          // stop
        eventLoop.submit(() => brokerLifecycleManager.tryTransition(broker, BrokerState.Inactive())).get()
        cluster.save()

        if (!broker.waitFor(null, timeout)) {
          return timeoutJson(broker, "stop")
        }

        val startTimeout = new Period(Math
          .max(timeout.ms - (System.currentTimeMillis() - begin), 0L) + "ms")

        // start
        eventLoop.submit(() => brokerLifecycleManager.tryTransition(broker, BrokerState.Active())).get
        cluster.save()

        val startBegin = System.currentTimeMillis()
        if (!broker.waitFor(State.RUNNING, startTimeout)) {
          return timeoutJson(broker, "start")
        }

        if (waitForRepl) {
          val replicationTimeout = new Period(Math
            .max(timeout.ms - (System.currentTimeMillis() - startBegin), 0L) + "ms")
          if (!waitForReplication(replicationTimeout)) {
            return timeoutJson(broker, "replication")
          }
        }
      }

      val resp = BrokerStartResponse(brokers, "restarted")
      Response.status(Response.Status.OK).entity(resp).build()
    }

    private def waitForReplication(timeout: Period): Boolean = {
      var t = timeout.ms
      logger.info("Starting poll for replication catch-up")

      val topics = ZkUtilsWrapper().getAllTopics()
      def outOfSyncReplicas() = {
        cluster.topics.getPartitions(topics).flatMap({
          case (_, partitions) => partitions.flatMap(p => Set(p.replicas: _*) &~ Set(p.isr: _*))
        }).toSet
      }

      while (t > 0) {
        val oos = outOfSyncReplicas()
        if (oos.nonEmpty) {
          logger.info(s"Waiting for brokers $oos to become in sync.")
        } else {
          logger.info("All replicas in sync")
          return true
        }

        val delay = Math.min(5000, t)
        Thread.sleep(delay)
        t -= delay
      }
      false
    }

    @POST
    @Path("log")
    @Produces(Array(MediaType.APPLICATION_JSON))
    def brokerLog(
      @DefaultValue("30s") @BothParam("timeout") timeout: Period,
      @BothParam("broker") expr: String,
      @DefaultValue("stdout") @BothParam("name") name: String,
      @DefaultValue("100") @BothParam("lines") lines: Int
    ): Response = {
      if (lines <= 0)
        return Status.BadRequest("lines has to be greater than 0")

      val maybeBrokers = expandBrokers(expr, {
        case b if !b.active => throw new IllegalArgumentException(s"broker ${b.id} is not active")
        case b if b.task == null || !b.task.running =>
          throw new IllegalArgumentException(s"broker ${b.id} is not running")
      })
      val result = maybeBrokers.map(b => brokerLogImpl(b, name, lines, timeout))
      result match {
        case Failure(e) => Status.BadRequest(e.getMessage)
        case Success(r) if r.size == 1 => Response.ok(r.head._2).build()
        case Success(r) if r.size > 1 => Response.ok(r).build()
        case _ => Response.noContent().build()
      }
    }

    private def brokerLogImpl(
      brokers: Seq[Broker],
      name: String,
      lines: Int,
      timeout: Period
    ): Map[Int, HttpLogResponse] = {
      val futures = brokers.map(b =>
        b.id -> scheduler.requestBrokerLog(b, name, lines, Duration(timeout.ms(), TimeUnit.MILLISECONDS))
      ).toMap

      futures
        .mapValues(f => Try(Await.result(f, Duration.Inf)))
        .mapValues({
          case Success(r) => HttpLogResponse("ok", r)
          case Failure(e: TimeoutException) => HttpLogResponse("timeout", "")
          case Failure(e) => HttpLogResponse("failure", e.getMessage)
        })
    }

    @POST
    @Path("clone")
    @Produces(Array(MediaType.APPLICATION_JSON))
    def cloneBroker(
      @BothParam("broker") expr: String,
      @BothParam("source") sourceBrokerId: String
    ): Response = {
      val newBrokers = Expr.expandBrokers(cluster, expr)
      val existingBrokers = newBrokers.filter(b => cluster.getBroker(b) != null)
      if (existingBrokers.nonEmpty) {
        return Status.BadRequest(s"broker(s) ${existingBrokers.mkString(",")} already exist.")
      }
      val sourceBroker = expandBrokers(sourceBrokerId)
      val addedBrokers = sourceBroker.flatMap({
        case b if b.size > 1 => Failure(new IllegalArgumentException("source must be 1 broker"))
        case b if b.isEmpty => Failure(new IllegalArgumentException("source does not exist"))
        case b => Success(b.head)
      }).map(b => cloneBrokerImpl(b, newBrokers))

      addedBrokers match {
        case Success(b) => Response.ok(BrokerStatusResponse(b)).build()
        case Failure(f) => Status.BadRequest(f.getMessage)
      }
    }

    def cloneBrokerImpl(source: Broker, newIds: Seq[Int]) = {
      val newBrokers = newIds.map(source.clone)
      newBrokers.foreach(cluster.addBroker)
      cluster.save()

      newBrokers
    }
  }

}