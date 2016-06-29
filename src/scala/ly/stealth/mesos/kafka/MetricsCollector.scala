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

import com.yammer.metrics.Metrics
import com.yammer.metrics.core._
import com.yammer.metrics.reporting.AbstractPollingReporter
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.collection.mutable

class MetricsCollector(send: Broker.Metrics => Unit)
  extends AbstractPollingReporter(Metrics.defaultRegistry(), "kafka-metrics")
    with MetricProcessor[mutable.Map[String, Number]] {

  private val logger = Logger.getLogger(classOf[MetricsCollector])
  private val scopeLevelMetrics = Set("BrokerTopicMetrics", "RequestMetrics", "Log", "Partition")

  private def buildName(name: MetricName): String = {
    if (name.hasScope) {
      s"${name.getGroup},${name.getType},${name.getName},${name.getScope}"
    }
    else {
      s"${name.getGroup},${name.getType},${name.getName}"
    }
  }

  private def addMetric(name: MetricName, value: Number, metrics: mutable.Map[String, Number]): Unit = {
    if (name.hasScope && !scopeLevelMetrics.contains(name.getType)) {
      return
    }
    val displayName = buildName(name)
    metrics(displayName) = value
  }

  override def run(): Unit = {
    try {
      val metrics = new mutable.HashMap[String, Number]()
      for (entry <- getMetricsRegistry.groupedMetrics.entrySet()) {
        for (subEntry <- entry.getValue.entrySet()) {
          val metric = subEntry.getValue
          if (metric != null) {
            try {
              metric.processWith(this, subEntry.getKey, metrics)
            } catch {
              case e: Throwable =>
                logger.error("Error processing metrics:", e)
            }
          }
        }
      }
      send(new Broker.Metrics(metrics, System.currentTimeMillis()))
    }
    catch {
      case e: Throwable =>
        logger.error("Error collecting metrics", e)
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

  override def processHistogram(name: MetricName, histogram: Histogram, context: mutable.Map[String, Number]): Unit =
    addMetric(name, histogram.mean(), context)

  override def processTimer(name: MetricName, timer: Timer, context: mutable.Map[String, Number]): Unit =
    addMetric(name, timer.oneMinuteRate(), context)

  override def processCounter(name: MetricName, counter: Counter, context: mutable.Map[String, Number]): Unit =
    addMetric(name, counter.count(), context)
}
