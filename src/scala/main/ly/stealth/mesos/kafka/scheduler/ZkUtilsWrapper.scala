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

import ly.stealth.mesos.kafka.Config
import ly.stealth.mesos.kafka.interface.{AdminUtilsProxy, ZkUtilsProxy}
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

abstract class LazyWrapper[I](fact: (String) => I) {
  private def getInstance() = {
    fact(Config.zk)
  }

  private var instance: Option[I] = None

  def apply(): I = instance match {
    case Some(i) => i
    case None =>
      instance = Some(getInstance())
      instance.get
  }
  def reset() = instance = None
}


object ZkUtilsWrapper extends LazyWrapper(ZkUtilsProxy.apply) {}

object AdminUtilsWrapper extends LazyWrapper(AdminUtilsProxy.apply) { }