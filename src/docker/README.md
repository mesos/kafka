# Running image
sudo docker run -it yohan\test ./kafka-mesos.sh add 0

sudo docker run -it -p 7000:7000 --expose 7000 -h scheduler \
-v /home/yohan/workspace/project/kafka-mesos/kafka-mesos.properties:/opt/kafka-mesos/kafka-mesos.properties \
--add-host=master:192.168.3.5 yohan/kafka-mesos