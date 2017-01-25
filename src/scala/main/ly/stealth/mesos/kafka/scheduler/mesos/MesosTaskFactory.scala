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

import com.google.protobuf.ByteString
import ly.stealth.mesos.kafka._
import ly.stealth.mesos.kafka.executor.LaunchConfig
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.KafkaDistributionComponent
import net.elodina.mesos.util.Version
import org.apache.mesos.Protos.Environment.Variable
import org.apache.mesos.Protos._
import scala.collection.mutable

trait MesosTaskFactoryComponent {
  val taskFactory: MesosTaskFactory

  trait MesosTaskFactory {
    def newTask(broker: Broker, offer: Offer, reservation: Broker.Reservation): TaskInfo
  }
}

trait MesosTaskFactoryComponentImpl extends MesosTaskFactoryComponent {
  this: KafkaDistributionComponent =>

  val taskFactory: MesosTaskFactory = new MesosTaskFactoryImpl

  class MesosTaskFactoryImpl extends MesosTaskFactory {
    private[kafka] def newExecutor(broker: Broker): ExecutorInfo = {
      val distInfo = kafkaDistribution.distInfo
      var cmd = "java -cp " + distInfo.jar.getName
      cmd += " -Xmx" + broker.heap + "m"
      if (broker.jvmOptions != null) cmd += " " + broker.jvmOptions.replace("$id", broker.id)

      if (Config.debug) cmd += " -Ddebug"
      cmd += " ly.stealth.mesos.kafka.executor.Executor"

      val commandBuilder = CommandInfo.newBuilder
      if (Config.jre != null) {
        commandBuilder
          .addUris(CommandInfo.URI.newBuilder().setValue(Config.api + "/jre/" + Config.jre.getName))
        cmd = "jre/bin/" + cmd
      }

      val env = new mutable.HashMap[String, String]()
      env("MESOS_SYSLOG_TAG") = Config.frameworkName + "-" + broker.id
      if (broker.syslog) env("MESOS_SYSLOG") = "true"

      val envBuilder = Environment.newBuilder()
      for ((name, value) <- env)
        envBuilder.addVariables(Variable.newBuilder().setName(name).setValue(value))
      commandBuilder.setEnvironment(envBuilder)

      commandBuilder
        .addUris(CommandInfo.URI.newBuilder()
          .setValue(Config.api + "/jar/" + distInfo.jar.getName).setExtract(false))
        .addUris(CommandInfo.URI.newBuilder()
          .setValue(Config.api + "/kafka/" + distInfo.kafkaDist.getName))
        .setValue(cmd)

      ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder.setValue(Broker.nextExecutorId(broker)))
        .setCommand(commandBuilder)
        .setName("broker-" + broker.id)
        .build()
    }

    def newTask(broker: Broker, offer: Offer, reservation: Broker.Reservation): TaskInfo = {
      val taskData = {
        var defaults: Map[String, String] = Map(
          "broker.id" -> broker.id,
          "port" -> ("" + reservation.port),
          "log.dirs" -> "kafka-logs",
          "log.retention.bytes" -> ("" + 10l * 1024 * 1024 * 1024),

          "zookeeper.connect" -> Config.zk,
          "host.name" -> offer.getHostname
        )

        if (kafkaDistribution.distInfo.kafkaVersion.compareTo(new Version("0.9")) >= 0)
          defaults += ("listeners" -> s"PLAINTEXT://:${ reservation.port }")

        if (reservation.volume != null)
          defaults += ("log.dirs" -> "data/kafka-logs")

        val launchConfig = LaunchConfig(
          broker.id,
          broker.options,
          broker.syslog,
          broker.log4jOptions,
          broker.bindAddress,
          defaults)
        ByteString.copyFrom(JsonUtil.toJsonBytes(launchConfig))
      }

      val taskBuilder: TaskInfo.Builder = TaskInfo.newBuilder
        .setName(Config.frameworkName + "-" + broker.id)
        .setTaskId(TaskID.newBuilder.setValue(Broker.nextTaskId(broker)).build)
        .setSlaveId(offer.getSlaveId)
        .setData(taskData)
        .setExecutor(newExecutor(broker))

      taskBuilder.addAllResources(reservation.toResources)
      taskBuilder.build
    }
  }
}