# SPA-Assignment2

Anaconda 
export PATH="/usr/local/anaconda3/bin:$PATH"
python -m pip install kafka-python

Start Zookeper

zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

Kafka start

kafka-server-start /usr/local/etc/kafka/server.properties


Create Topic

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

Listen to topic 
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
