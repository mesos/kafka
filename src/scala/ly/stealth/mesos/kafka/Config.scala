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

import java.io.{FileInputStream, File}
import java.util.Properties
import java.net.URI
import ly.stealth.mesos.kafka.Util.{BindAddress, Period}

object Config {
  val DEFAULT_FILE = new File("kafka-mesos.properties")

  var debug: Boolean = false
  var storage: String = "file:kafka-mesos.json"

  var master: String = null
  var principal: String = null
  var secret: String = null
  var user: String = null

  var frameworkName: String = "kafka"
  var frameworkRole: String = "*"
  var frameworkTimeout: Period = new Period("30d")

  var jre: File = null
  var log: File = null
  var api: String = null
  var bindAddress: BindAddress = null
  var zk: String = null

  def apiPort: Int = {
    val port = new URI(api).getPort
    if (port == -1) 80 else port
  }

  def replaceApiPort(port: Int): Unit = {
    val prev: URI = new URI(api)
    api = "" + new URI(
      prev.getScheme, prev.getUserInfo,
      prev.getHost, port,
      prev.getPath, prev.getQuery, prev.getFragment
    )
  }

  private[kafka] def load(file: File): Unit = {
    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    if (props.containsKey("debug")) debug = java.lang.Boolean.valueOf(props.getProperty("debug"))
    if (props.containsKey("storage")) storage = props.getProperty("storage")

    if (props.containsKey("master")) master = props.getProperty("master")
    if (props.containsKey("user")) user = props.getProperty("user")
    if (props.containsKey("principal")) principal = props.getProperty("principal")
    if (props.containsKey("secret")) secret = props.getProperty("secret")

    if (props.containsKey("framework-name")) frameworkName = props.getProperty("framework-name")
    if (props.containsKey("framework-role")) frameworkRole = props.getProperty("framework-role")
    if (props.containsKey("framework-timeout")) frameworkTimeout = new Period(props.getProperty("framework-timeout"))

    if (props.containsKey("jre")) jre = new File(props.getProperty("jre"))
    if (props.containsKey("log")) log = new File(props.getProperty("log"))
    if (props.containsKey("api")) api = props.getProperty("api")
    if (props.containsKey("bind-address")) bindAddress = new BindAddress(props.getProperty("bind-address"))
    if (props.containsKey("zk")) zk = props.getProperty("zk")
  }

  override def toString: String = {
    s"""
      |debug: $debug, storage: $storage
      |mesos: master=$master, user=${if (user == null || user.isEmpty) "<default>" else user}, principal=${if (principal != null) principal else "<none>"}, secret=${if (secret != null) "*****" else "<none>"}
      |framework: name=$frameworkName, role=$frameworkRole, timeout=$frameworkTimeout
      |api: $api, bind-address: ${if (bindAddress != null) bindAddress else "<all>"}, zk: $zk, jre: ${if (jre == null) "<none>" else jre}
    """.stripMargin.trim
  }
}
