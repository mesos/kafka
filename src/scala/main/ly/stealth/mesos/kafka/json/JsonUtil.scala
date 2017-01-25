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
package ly.stealth.mesos.kafka.json

import com.fasterxml.jackson.annotation.JsonInclude
import java.text.SimpleDateFormat
import java.util.TimeZone
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(KafkaObjectModel)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private[this] def dateFormat = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("UTC-0"))
    format
  }
  mapper.setDateFormat(dateFormat)

  def toJson(value: Any): String = mapper.writeValueAsString(value)
  def toJsonBytes(value: Any): Array[Byte] = mapper.writeValueAsBytes(value)
  def fromJson[T](str: String)(implicit m: Manifest[T]): T = mapper.readValue[T](str)
  def fromJson[T](bytes: Array[Byte])(implicit m: Manifest[T]) = mapper.readValue[T](bytes)
}
