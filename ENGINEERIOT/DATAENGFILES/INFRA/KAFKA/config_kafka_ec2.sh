sudo su
mkdir /opt/kafka_cluster
cd /opt/kafka_cluster
wget https://dlcdn.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
tar -xzf kafka_2.13-3.3.1.tgz
cd kafka_2.13-3.3.1/    
sudo yum -y install java-11

cd /opt/kafka_cluster/kafka_2.13-3.3.1/

bin/zookeeper-server-start.sh config/zookeeper.properties

# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092