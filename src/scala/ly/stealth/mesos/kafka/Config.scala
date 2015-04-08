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

object Config {
  var debug: Boolean = false
  var clusterStorage: String = null

  var frameworkId: String = null
  var mesosUser: String = null

  var masterConnect: String = null
  var kafkaZkConnect: String = null
  var schedulerUrl: String = null

  var failoverTimeout: Int = 60

  def schedulerPort: Int = new URI(schedulerUrl).getPort
  load()

  private[kafka] def load(): Unit = {
    val configPath = System.getProperty("config")
    val file = new File(if (configPath != null) configPath else "kafka-mesos.properties")
    if (!file.exists()) throw new IllegalStateException("File " + file + " not found")

    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    debug = java.lang.Boolean.valueOf(props.getProperty("debug"))
    clusterStorage = props.getProperty("clusterStorage", "file:kafka-mesos.json")

    frameworkId = props.getProperty("framework.id")
    mesosUser = props.getProperty("mesos.user")

    masterConnect = props.getProperty("master.connect")
    kafkaZkConnect = props.getProperty("kafka.zk.connect")
    schedulerUrl = props.getProperty("scheduler.url")

    failoverTimeout = Integer.parseInt(props.getProperty("failoverTimeout"))
  }
}
