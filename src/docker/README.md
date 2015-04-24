# Intro
This folder provide a tool for building docker images to be reused later.
Image fill have following pre-installed components:
- openjdk-jdk;
- mesos native libs;

`/opt/kafka-mesos` folder will contain following content:
- kafka*.tgz - kafka distro;
- kafka-mesos*.jar - kafka-mesos distro;
- kafka-mesos.sh - shell script to run kafka-mesos;

No default configuration file for kafka-mesos is provided. All configuration params should be
specified via cli options.

# Building image
Building docker image is done by `build-image.sh` script. Please refer to help via `./build-image.sh -h`.

Example:
```
# ./build-image.sh -t <username>/kafka-mesos
```

Note: `build-image.sh` does not pushes the image. Push should be done manually after testing.

# Running image
Running image using docker. Required networking params should be provided. Image has no entry point,
so ./kafka-mesos.sh should be specified explicitly.

Example:
```
# docker run -it -p 7000:7000 --add-host=master:192.168.3.5 <username>/kafka-mesos ./kafka-mesos.sh scheduler \
--mesos-connect=master:5050 --kafka-zk-connect=master:2181 --scheduler-url=http://<accessible-ip>:7000 --cluster-storage=zk:/kafka-mesos
```

Note: if you want to inspect image content you can it without specifying `./kafka-mesos.sh` entry point.
In that case you will got shell access.