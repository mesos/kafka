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
import ly.stealth.mesos.kafka.Util.Period

object Config {
  var debug: Boolean = false
  var clusterStorage: String = "file:kafka-mesos.json"

  var mesosConnect: String = null
  var mesosUser: String = null
  var mesosFrameworkTimeout: Period = new Period("1d")

  var kafkaZkConnect: String = null
  var schedulerUrl: String = null

  def schedulerPort: Int = new URI(schedulerUrl).getPort
  load()

  private[kafka] def load(): Unit = {
    val configPath = System.getProperty("config")
    val file = new File(if (configPath != null) configPath else "kafka-mesos.properties")
    if (!file.exists()) return

    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    debug = java.lang.Boolean.valueOf(props.getProperty("debug"))
    if (props.contains("cluster-storage")) clusterStorage = props.getProperty("cluster-storage")

    mesosConnect = props.getProperty("mesos-connect")
    mesosUser = props.getProperty("mesos-user")
    if (props.contains("mesos-framework-timeout")) mesosFrameworkTimeout = new Period(props.getProperty("mesos-framework-timeout"))

    kafkaZkConnect = props.getProperty("kafka-zk-connect")
    schedulerUrl = props.getProperty("scheduler-url")
  }

  override def toString: String = {
    s"""
      |debug: $debug, cluster-storage: $clusterStorage
      |mesos: connect=$mesosConnect, user=${if (mesosUser == null) "<system>" else mesosUser}, framework-timeout=$mesosFrameworkTimeout
      |kafka-zk-connect: $kafkaZkConnect, scheduler-url: $schedulerUrl
    """.stripMargin.trim
  }
}
