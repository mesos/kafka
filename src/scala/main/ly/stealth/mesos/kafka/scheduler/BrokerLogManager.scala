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
package ly.stealth.mesos.kafka.scheduler

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeoutException}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Success

trait BrokerLogManagerComponent {
  val brokerLogManager: BrokerLogManager

  trait BrokerLogManager {
    def initLogRequest(timeout: Duration): (Long, Future[String])
    def putLog(requestId: Long, content: String): Unit
  }
}

trait BrokerLogManagerComponentImpl extends BrokerLogManagerComponent {
  val brokerLogManager = new BrokerLogManagerImpl

  class BrokerLogManagerImpl extends BrokerLogManager {
    private[this] val pendingLogs = TrieMap[Long, Promise[String]]()
    private[this] val scheduler: ScheduledExecutorService  = Executors.newScheduledThreadPool(1);

    def putLog(requestId: Long, content: String): Unit =
      pendingLogs.get(requestId).foreach { log => log.complete(Success(content)) }

    private def scheduleTimeout(promise: Promise[String], timeout: Duration): ScheduledFuture[_] = {
      scheduler.schedule(new Runnable {
        override def run() = promise.tryFailure(new TimeoutException())
      }, timeout.length, timeout.unit)
    }

    def initLogRequest(timeout: Duration): (Long, Future[String]) = {
      val requestId = System.currentTimeMillis()
      val promise = Promise[String]()

      pendingLogs.put(requestId, promise)

      val timer = scheduleTimeout(promise, timeout)
      promise.future.onComplete { _ =>
        timer.cancel(false) // false: probably too late to interrupt the failing promise anyway
        pendingLogs.remove(requestId)
      }

      (requestId, promise.future)
    }
  }
}

