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

import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.StatusType
import net.elodina.mesos.util.{Constraint, Strings}
import scala.collection.JavaConverters._
import scala.collection.mutable

class StringMap(value: String) extends mutable.HashMap[String, String] {
  this ++= Strings.parseMap(value).asScala
}

class ConstraintMap(value: String) extends mutable.HashMap[String, Constraint] {
  this ++= Strings.parseMap(value).asScala.mapValues(new Constraint(_))
}

object Status {
  class BadRequest(reason: String) extends StatusType {
    override def getStatusCode: Int = Response.Status.BAD_REQUEST.getStatusCode
    override def getReasonPhrase: String = reason
    override def getFamily: Response.Status.Family = Response.Status.BAD_REQUEST.getFamily
  }
  object BadRequest {
    def apply(reason: String) = Response.status(new BadRequest(reason)).build()
  }
}