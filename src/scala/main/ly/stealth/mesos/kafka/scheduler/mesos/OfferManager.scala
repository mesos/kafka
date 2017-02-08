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
package ly.stealth.mesos.kafka.scheduler.mesos

import java.util.Date
import ly.stealth.mesos.kafka.Broker
import org.apache.log4j.Logger
import org.apache.mesos.Protos.{Filters, Offer, OfferID}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

abstract class OfferResult {
  def offer: Offer
}
object OfferResult {
  abstract class Decline extends OfferResult {
    def reason: String
    def duration: Int
  }

  case class DeclineBroker(
    offer: Offer,
    broker: Broker,
    reason: String,
    duration: Int = 5
  ) extends Decline
  case class NoWork(offer: Offer) extends Decline {
    val reason = "no work to do"
    val duration: Int = 60 * 60
  }
  case class Accept(offer: Offer, broker: Broker) extends OfferResult

  def neverMatch(offer: Offer, broker: Broker, reason: String) =
    DeclineBroker(offer, broker, reason, 60 * 60)
  def eventuallyMatch(offer: Offer, broker: Broker, reason: String, howLong: Int) =
    DeclineBroker(offer, broker, reason, howLong)
}

object OfferManager {
  def otherTasksAttributes(name: String, brokers: Iterable[Broker]): Iterable[String] = {
    def value(task: Broker.Task, name: String): Option[String] = {
      if (name == "hostname") return Some(task.hostname)
      task.attributes.get(name)
    }

    brokers.flatMap(b => Option(b.task).flatMap(t => value(t, name)))
  }
}

trait OfferManagerComponent {
  val offerManager: OfferManager

  trait OfferManager {
    def enableOfferSuppression()
    def tryAcceptOffer(
      offer: Offer,
      brokers: Iterable[Broker]
    ): Either[OfferResult.Accept, Seq[OfferResult.Decline]]
    def pauseOrResumeOffers(forceRevive: Boolean = false): Unit
    def declineOffer(offer: OfferID): Unit
    def declineOffer(offer: OfferID, filters: Filters): Unit
  }
}

trait OfferManagerComponentImpl extends OfferManagerComponent {
  this: ClusterComponent with SchedulerDriverComponent =>

  val offerManager: OfferManager = new OfferManagerImpl

  class OfferManagerImpl extends OfferManager {
    private[this] var offersAreSuppressed: Boolean = false
    private[this] val logger = Logger.getLogger("OfferManager")
    private[this] var canSuppressOffers = true

    def enableOfferSuppression(): Unit = canSuppressOffers = true

    def tryAcceptOffer(
      offer: Offer,
      brokers: Iterable[Broker]
    ): Either[OfferResult.Accept, Seq[OfferResult.Decline]] = {
      val now = new Date()
      val declines = mutable.Buffer[OfferResult.Decline]()

      for (broker <- brokers.filter(_.shouldStart(offer.getHostname))) {
        broker.matches(offer, now, name => OfferManager.otherTasksAttributes(name, brokers)) match {
          case accept: OfferResult.Accept => return Left(accept)
          case reason: OfferResult.Decline => declines.append(reason)
        }
      }

      // if we got here we're declining the offer,
      // if there's no reason it just meant we had nothing to do
      if (declines.isEmpty)
        declines.append(OfferResult.NoWork(offer))

      val maxDuration = declines.map(_.duration).max.toDouble
      val jitter = (maxDuration / 3) * (Random.nextDouble() - .5)
      val fb = Filters.newBuilder().setRefuseSeconds(maxDuration + jitter)
      declineOffer(offer.getId, fb.build())
      Right(declines)
    }

    def declineOffer(offer: OfferID): Unit = Driver.call(_.declineOffer(offer))
    def declineOffer(offer: OfferID, filters: Filters): Unit =
      Driver.call(_.declineOffer(offer, filters))

    def pauseOrResumeOffers(forceRevive: Boolean = false): Unit = {
      if (forceRevive) {
        driver.reviveOffers()
        logger.info("Re-requesting previously suppressed offers.")
        this.offersAreSuppressed = false
        return
      }

      val clusterIsSteadyState = cluster.getBrokers.asScala.forall(_.isSteadyState)
      // If all brokers are steady state we can request mesos to stop sending offers.
      if (!this.offersAreSuppressed && clusterIsSteadyState) {
        if (canSuppressOffers) {
          // Our version of mesos supports suppressOffers, so use it.
          Driver.call(_.suppressOffers())
          logger.info("Cluster is now stable, offers are suppressed")
          this.offersAreSuppressed = true
        }
        else {
          // No support for suppress offers, noop it.
          this.offersAreSuppressed = true
        }
      }
      // Else, if offers are suppressed, and we are no longer steady-state, resume offers.
      else if (!clusterIsSteadyState && offersAreSuppressed) {
        Driver.call(_.reviveOffers())
        logger.info("Cluster is no longer stable, resuming offers.")
        this.offersAreSuppressed = false
      }
    }
  }

}