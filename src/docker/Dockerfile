# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:14.04

RUN \
  if [ -f "/tmp/apt-proxy" ]; then \
    APT_PROXY=$(cat "/tmp/apt-proxy"); \
    echo "Using apt-proxy: $APT_PROXY"; \
    echo "Acquire::http::Proxy \"$APT_PROXY\";" > /etc/apt/apt.conf.d/90-apt-proxy.conf ; \
  fi

# Add mesos repo
RUN \
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF && \
  echo deb http://repos.mesosphere.io/ubuntu trusty main > /etc/apt/sources.list.d/mesosphere.list

# Install deps
RUN \
  apt-get update && \
  apt-get install -qy \
    git vim zip mc curl \
    openjdk-7-jdk mesos libc6

# Add kafka-mesos & kafka
COPY .docker/kafka* /opt/kafka-mesos/

WORKDIR /opt/kafka-mesos
EXPOSE 7000

ENTRYPOINT ["/usr/bin/java", "-jar", "/opt/kafka-mesos/kafka-mesos-0.9.5.0.jar", "scheduler"]
