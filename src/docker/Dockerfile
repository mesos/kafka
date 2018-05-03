FROM ubuntu:16.04

# Add mesos repo
RUN \
  apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF && \
  echo deb http://repos.mesosphere.io/ubuntu trusty main > /etc/apt/sources.list.d/mesosphere.list

# Install deps
RUN \
  apt-get update && \
  apt-get install -qy \
    git zip mc curl \
    openjdk-8-jdk mesos libc6

# Add kafka-mesos & kafka
COPY .docker/kafka* /opt/kafka-mesos/
COPY ./docker-entrypoint.sh /docker-entrypoint.sh

WORKDIR /opt/kafka-mesos
EXPOSE 7000

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["scheduler"]
