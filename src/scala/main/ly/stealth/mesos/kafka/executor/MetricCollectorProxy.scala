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

import com.yammer.metrics.Metrics
import com.yammer.metrics.core._
import com.yammer.metrics.util.DeathRattleExceptionHandler
import java.util
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import ly.stealth.mesos.kafka.Broker
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

class MetricCollectorProxy(loader: ClassLoader, server: AnyRef, send: Broker.Metrics => Unit) {
  private[this] val executorService = Executors.newScheduledThreadPool(1)
  private[this] val cls = loader.loadClass(classOf[MetricCollectorProxy.MetricsCollector].getName)
  private[this] val ctor = cls.getConstructor(classOf[AnyRef])
  private[this] val collector = ctor.newInstance(server)
  private[this] val collectorRunMethod = cls.getMethod("run")
  private[this] var collectorSchedule: ScheduledFuture[_] = _

  def start(): Unit = {
    collectorSchedule = executorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = collect()
    }, 60, 60, TimeUnit.SECONDS)
    collect()
  }

  private def collect() = {
    val metrics = collectorRunMethod.invoke(collector)
    send(Broker.Metrics(metrics.asInstanceOf[util.Map[String, Number]].toMap, System.currentTimeMillis()))
  }

  def shutdown(): Unit = {
    Option(collectorSchedule).foreach(_.cancel(false))
  }
}

object MetricCollectorProxy {
  // WARNING: This class must be loaded into the same classloader that Kafka is running in.
  // MetricCollectorProxy handles this.
  private class MetricsCollector(server: AnyRef) extends MetricProcessor[mutable.Map[String, Number]] {
    if (server.getClass.getClassLoader.ne(this.getClass.getClassLoader)) {
      throw new RuntimeException("Collector loaded into wrong ClassLoader.")
    }

    import org.apache.kafka.common.metrics.{Metrics => ApacheMetrics}

    private def getMetricsRegistry = Metrics.defaultRegistry()
    private val logger = Logger.getLogger("MetricCollectorProxy")

    private def installCollectors() = {
      val c = Metrics.newCounter(new MetricName("vm", "threads", "unhandledThreadDeaths"))
      val handler = new DeathRattleExceptionHandler(c)
      Thread.setDefaultUncaughtExceptionHandler(handler)
    }
    installCollectors()

    private def initKafkaServerMetricsAccessor() = {
      try {
        val field = server.getClass.getDeclaredField("metrics")
        field.setAccessible(true)
        () => Some(field.get(server).asInstanceOf[ApacheMetrics])
      } catch {
        case NonFatal(e) => () => None
      }
    }
    private val scopeLevelMetricExcludes = Set("FetcherLagMetrics", "FetcherStats")
    private val apacheMetricsIncludes = Set("byte-rate", "throttle-time")
    private val apacheMetricsAccessor = initKafkaServerMetricsAccessor()

    private def buildName(name: MetricName, prefix: String = ""): String = {
      if (name.hasScope) {
        s"${name.getGroup},${name.getType},${name.getName}$prefix,${name.getScope}"
      }
      else {
        s"${name.getGroup},${name.getType},${name.getName}$prefix"
      }
    }

    private def addMetric(name: MetricName, value: Number, metrics: mutable.Map[String, Number], prefix: String = ""): Unit = {
      if (name.hasScope && scopeLevelMetricExcludes.contains(name.getType)) {
        return
      }
      val displayName = buildName(name, prefix)
      metrics(displayName) = value
    }

    private def collectJvmMetrics: Map[String, Number] = {
      val vm = VirtualMachineMetrics.getInstance()
      val baseVmMetrics = Map[String, Number](
        "vm,memory,totalInit" -> vm.totalInit(),
        "vm,memory,totalUsed" -> vm.totalUsed(),
        "vm,memory,totalMax" -> vm.totalMax(),
        "vm,memory,totalCommitted" -> vm.totalCommitted(),

        "vm,memory,heapInit" -> vm.heapInit(),
        "vm,memory,heapUsed" -> vm.heapUsed(),
        "vm,memory,heapMax" -> vm.heapMax(),
        "vm,memory,heapCommitted" -> vm.heapCommitted(),
        "vm,memory,heapUsage" -> vm.heapUsage(),
        "vm,memory,nonHeapUsage" -> vm.nonHeapUsage(),

        "vm,threads,daemon" -> vm.daemonThreadCount(),
        "vm,threads,total" -> vm.threadCount(),
        "vm,uptime,total" -> vm.uptime()
      )

      val bps = vm.getBufferPoolStats
      val bufferPoolMetrics = if (!bps.isEmpty) {
        def getBufferStats(bufferType: String) =
          if (bps.containsKey(bufferType))
            Map[String, Number](
              s"vm,buffers,count,$bufferType" -> bps.get(bufferType).getCount,
              s"vm,buffers,memoryUsed,$bufferType" -> bps.get(bufferType).getMemoryUsed,
              s"vm,buffers,totalCapacity,$bufferType" -> bps.get(bufferType).getTotalCapacity
            )
          else
            Map[String, Number]()
        getBufferStats("direct") ++ getBufferStats("mapped")
      } else {
        Map[String, Number]()
      }

      val gcMetrics = vm.garbageCollectors().flatMap({
        case (key, stats) => Map[String, Number](
          s"vm,gc,runs,$key" -> stats.getRuns,
          s"vm,gc,ms,$key" -> stats.getTime(TimeUnit.MILLISECONDS)
        )
      })

      baseVmMetrics ++ bufferPoolMetrics ++ gcMetrics
    }

    private def collectApacheMetrics: Map[String, Number] = {
      val allApacheMetrics = apacheMetricsAccessor()
      val metrics = allApacheMetrics.map(
        m => m.metrics().filterKeys(k => apacheMetricsIncludes.contains(k.name()))
      )
      metrics.map(_.map({
        case (name, metric) =>
          val metricName =
            if (name.tags().containsKey("client-id")) {
              val userId =
                if (name.tags.containsKey("user-id"))
                  name.tags.get("user-id")
                else
                  null
              var metricSuffix = name.tags.get("client-id")
              if (userId != null) {
                metricSuffix += "." + userId
              }
              s"kafka.metrics,ClientMetrics,${name.name()},${name.group()},$metricSuffix"
            } else {
              s"kafka.metrics,${name.name()},${name.group()}"
            }

          metricName -> metric.value().asInstanceOf[Number]
      }).toMap).getOrElse(Map())
    }

    // WARNING: This is java map in order to allow it to marshall between class loaders.
    // DO NOT change it to a scala Map.
    def run(): util.Map[String, Number] = {
      try {
        val metrics = new mutable.HashMap[String, Number]()
        for ((metricName, metric) <- getMetricsRegistry.allMetrics()) {
          try {
            metric.processWith(this, metricName, metrics)
          } catch {
            case NonFatal(_) =>
          }
        }

        (metrics ++ collectJvmMetrics ++ collectApacheMetrics).toMap.asJava
      }
      catch {
        case NonFatal(e) =>
          logger.error("Error collecting metrics", e)
          null
      }
    }

    override def processGauge(name: MetricName, gauge: Gauge[_], context: mutable.Map[String, Number]): Unit = {
      val value = gauge.value()
      if (value.isInstanceOf[Integer] || value.isInstanceOf[Long] || value.isInstanceOf[Double]) {
        addMetric(name, value.asInstanceOf[Number], context)
      }
    }

    override def processMeter(name: MetricName, meter: Metered, context: mutable.Map[String, Number]): Unit =
      addMetric(name, meter.oneMinuteRate(), context)

    override def processHistogram(name: MetricName, histogram: Histogram, context: mutable.Map[String, Number]): Unit = {
      addMetric(name, histogram.mean(), context)
      addMetric(name, histogram.getSnapshot.get99thPercentile(), context, "_p99")
    }

    override def processTimer(name: MetricName, timer: Timer, context: mutable.Map[String, Number]): Unit = {
      addMetric(name, timer.oneMinuteRate(), context)
    }

    override def processCounter(name: MetricName, counter: Counter, context: mutable.Map[String, Number]): Unit =
      addMetric(name, counter.count(), context)
  }
}

