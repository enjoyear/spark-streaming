## Setup Kafka Listeners for External and Internal clients

Start an EC2 instance and whitelist TCP ports used below, e.g. 9092 and 4001

Follow the steps below for the setup
```bash
## Execute on EC2
yes y | sudo yum install wget
yes y | sudo yum install java-11-openjdk-devel
yes y | sudo yum install scala
wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
tar -xvf kafka_2.13-2.8.0.tgz
cd kafka_2.13-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
# Produce messages
# localhost can be replaced with private ip
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
echo 'a b cd' | kafkacat -P -b localhost:9092 -t quickstart-events



## apply the settings below to config/server.properties
## It would also work if 0.0.0.0 is changed to the private IP 10.1.0.94, but not 127.0.0.1
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:4001
inter.broker.listener.name=INTERNAL
## The port number is used by Kafka to match and tell which advertised listener should be returned as a response
## If internal is INTERNAL://abc:9092, the host name abc will be returned if client connects through 9092
## If external is external://xyz:4041, the host name xyz will be returned if client connects through 4041
advertised.listeners=INTERNAL://ip-10-1-0-94.ec2.internal:9092,EXTERNAL://ec2-3-91-72-157.compute-1.amazonaws.com:4001
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT



## Execute locally
export EC2HostName=ec2-3-91-72-157.compute-1.amazonaws.com
export EC2KafkaPort=4001
# Test connection
telnet ${EC2HostName} ${EC2KafkaPort}
## -L: see the metadata for the listener to which you connected.
## Note that
## - 9092 will response with internal hostname
## - 4001 will response with external hostname
kafkacat -b ${EC2HostName}:${EC2KafkaPort} -L ## You will see the advertised hostname-port returned 
#Consume
kafkacat -b ${EC2HostName}:${EC2KafkaPort} -L -C -t quickstart-events
```

## References
https://aws.amazon.com/blogs/big-data/how-goldman-sachs-builds-cross-account-connectivity-to-their-amazon-msk-clusters-with-aws-privatelink/
https://rmoff.net/2018/08/02/kafka-listeners-explained/
https://cwiki.apache.org/confluence/display/KAFKA/KIP-103%3A+Separation+of+Internal+and+External+traffic
