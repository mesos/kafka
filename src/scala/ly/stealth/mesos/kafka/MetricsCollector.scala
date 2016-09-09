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

import java.util.concurrent.TimeUnit

import com.yammer.metrics.Metrics
import com.yammer.metrics.core._
import com.yammer.metrics.reporting.AbstractPollingReporter
import com.yammer.metrics.util.DeathRattleExceptionHandler
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.NonFatal

class MetricsCollector(send: Broker.Metrics => Unit)
  extends AbstractPollingReporter(Metrics.defaultRegistry(), "kafka-metrics")
    with MetricProcessor[mutable.Map[String, Number]] {

  private def installCollectors() = {
    val c = Metrics.newCounter(new MetricName("vm", "threads", "unhandledThreadDeaths"))
    val handler = new DeathRattleExceptionHandler(c)
    Thread.setDefaultUncaughtExceptionHandler(handler)
  }
  installCollectors()

  private val logger = Logger.getLogger(classOf[MetricsCollector])
  private val scopeLevelMetrics = Set("BrokerTopicMetrics", "RequestMetrics", "Log", "Partition")

  private def buildName(name: MetricName, prefix: String = ""): String = {
    if (name.hasScope) {
      s"${name.getGroup},${name.getType},${name.getName}$prefix,${name.getScope}"
    }
    else {
      s"${name.getGroup},${name.getType},${name.getName}$prefix"
    }
  }

  private def addMetric(name: MetricName, value: Number, metrics: mutable.Map[String, Number], prefix: String = ""): Unit = {
    if (name.hasScope && !scopeLevelMetrics.contains(name.getType)) {
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

  override def run(): Unit = {
    try {
      val metrics = new mutable.HashMap[String, Number]()
      for ((metricName, metric) <- getMetricsRegistry.allMetrics()) {
        try {
          metric.processWith(this, metricName, metrics)
        } catch {
          case NonFatal(e) => logger.error("Error processing metrics:", e)
        }
      }
      send(new Broker.Metrics(metrics ++ collectJvmMetrics, System.currentTimeMillis()))
    }
    catch {
      case NonFatal(e) => logger.error("Error collecting metrics", e)
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
