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

object Config {
  var debug: Boolean = false
  var mesosUser: String = null

  var masterHost: String =  null
  var masterPort: Int = 5050
  var zkPort: Int = 2181
  
  var schedulerHost: String = null
  var schedulerPort: Int = 7000

  var failoverTimeout: Int = 60

  def masterUrl: String = masterHost + ":" + masterPort
  def zkUrl: String = masterHost + ":" + zkPort
  def schedulerUrl: String = "http://" + schedulerHost + ":" + schedulerPort

  load()

  private def load(): Unit = {
    val configPath = System.getProperty("config")
    val file = new File(if (configPath != null) configPath else "kafka-mesos.properties")
    if (!file.exists()) throw new IllegalStateException("File " + file + " not found")

    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)
    
    props.load(stream)
    stream.close()
    
    debug = java.lang.Boolean.valueOf(props.getProperty("debug"))
    mesosUser = props.getProperty("mesos.user")

    masterHost = props.getProperty("master.host")
    masterPort = Integer.parseInt(props.getProperty("master.port"))
    zkPort = Integer.parseInt(props.getProperty("master.zk.port"))
    
    schedulerHost = props.getProperty("scheduler.host")
    schedulerPort = Integer.parseInt(props.getProperty("scheduler.port"))
    
    failoverTimeout = Integer.parseInt(props.getProperty("failoverTimeout"))
  }
}

