# Setup Kafka Listeners for External and Internal clients

Start EC2 instances in a subnet with security group like below
![image](images/Kafka%20subnet%20security%20group.png)

Follow the steps below to set up a Kafka cluster with two brokers(broker-0 and broker-1)

## Setup Kafka Brokers on EC2
1. Download and install packages on both EC2 instances
```bash
yes y | sudo yum install wget
yes y | sudo yum install java-11-openjdk-devel
yes y | sudo yum install telnet
yes y | sudo yum install bind-utils  #For nslookup and dig
# yes y | sudo yum install scala
wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz
tar -xvf kafka_2.13-2.8.0.tgz
```

2. Start a single node zookeeper cluster on broker-0
```bash
cd kafka_2.13-2.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```

3. Start a terminal to start Kafka broker-0

Apply the settings below in config/server.properties for broker-0 
```bash
## No changes needed 
broker.id=0

## Listeners are the interfaces that Kafka binds/listens to and can handle traffic on those ports. For the bootstrap
## connection, the advertised listener of the SAME name will be advertised to the client. This advertised listener can
## be leveraged to change the hostname or port. When a listener is not advertised, current broker won't be consumable if
## a client tries to consume/produce using such listener. For example, if you define an external listener with 
## 0.0.0.0:9900 and a corresponding advertised listener with hostname:4000, your bootstrap connection needs to go through
## 9000 but later traffic will go through 4000. Either use a load balancer to change the traffic from 4000 back to 9000,
## or define a new listener, without an advertised listener, listening at 0.0.0.0:4000.
## If not use a load balancer, setup for this scenario will be like
### listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9900,EXTERNAL2://0.0.0.0:4000
### inter.broker.listener.name=INTERNAL
### advertised.listeners=INTERNAL://10.1.0.94:9092,EXTERNAL://ec2-54-87-254-253.compute-1.amazonaws.com:4000
### listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,EXTERNAL2:PLAINTEXT
## Constrains:
## * Listener names must be identical across all brokers
## * Each listener also must have a different port
## It would also work if 0.0.0.0 is changed to the private IP 10.1.0.94, but not 127.0.0.1
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:4000
inter.broker.listener.name=INTERNAL

## Advertised listeners are how clients can connect
## The matching between listener and advertised listener is done through the listner name

## An alternative way to the INTERNAL listener below is INTERNAL://ip-10-1-0-94.ec2.internal:9092, in this case
## - ip-10-1-0-94.ec2.internal is the broker-0's hostname
## - An A-Record for ip-10-1-0-94.ec2.internal to 10.1.0.94 needs to be added in the DNS
advertised.listeners=INTERNAL://10.1.0.94:9092,EXTERNAL://ec2-54-87-254-253.compute-1.amazonaws.com:4000
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

# Connect to the zookeeper running locally
zookeeper.connect=localhost:2181

# start the broker
bin/kafka-server-start.sh config/server.properties
```

4. Start a terminal to start Kafka broker-1

Apply the settings below in config/server.properties for broker-1
```bash
# Use a different broker id
broker.id=1

## Configure the listeners
## Listener names must be identical across all brokers
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:4001
inter.broker.listener.name=INTERNAL
advertised.listeners=INTERNAL://10.1.0.76:9092,EXTERNAL://ec2-54-173-92-245.compute-1.amazonaws.com:4001
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

# Connect to the zookeeper running on broker-0
zookeeper.connect=10.1.0.94:2181
```

5. Start a terminal to start a producer on broker-0
```bash
## SSH into broker0

# Delete the topic if exists
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic quickstart-events

# localhost can be replaced with private ip
# Create the topic
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 \
--partitions 3 --replication-factor 1

# Produce messages using internal port
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
# Option 2
# echo 'a b cd' | kafkacat -P -b localhost:9092 -t quickstart-events
```

6. Start a terminal to start a consumer on broker-1
```bash
## SSH into broker1

# Consume using the internal port
bin/kafka-console-consumer.sh --topic quickstart-events --bootstrap-server localhost:9092 \
--from-beginning

# Alternatively, you can consume using the external port
bin/kafka-console-consumer.sh --topic quickstart-events --bootstrap-server localhost:4001 \
--from-beginning
```

## Actions on your local machine

### Check the listener metadata
```bash
export Broker0=ec2-54-87-254-253.compute-1.amazonaws.com
export Broker1=ec2-54-173-92-245.compute-1.amazonaws.com
export InternalPort=9092
export Broker0ExternalPort=4000
export Broker1ExternalPort=4001

# Test connection
telnet ${Broker0} ${Broker0ExternalPort}
telnet ${Broker0} ${InternalPort}
telnet ${Broker1} ${Broker1ExternalPort}
telnet ${Broker1} ${InternalPort}

## -L: see the metadata for the listener to which you connected.
## Note that
## - 9092 will response with INTERNAL hostname
## - 4000 and 4001 will response with EXTERNAL hostname
## Listener names must be identical across all brokers, otherwise you will get a partial list of the brokers
## For example, in broker0 you declared listener EXTERNAL and in broker1 you declared listener EXTERNAL2
## When you try to get the metadata using port 4000, you will only see the brokers with EXTERNAL declared
## When you try to get the metadata using port 4001, you will only see the brokers with EXTERNAL2 declared
kafkacat -b ${Broker0}:${Broker0ExternalPort} -L ## You will see the advertised hostname-port returned
kafkacat -b ${Broker1}:${Broker1ExternalPort} -L ## You will see the advertised hostname-port returned

## If you use the internal port, the internal listeners will be returned
## So kafka response by matching port numbers
kafkacat -b ${Broker0}:${InternalPort} -L
kafkacat -b ${Broker1}:${InternalPort} -L
```

#### Example of bootstrapping with broker0 EXTERNAL port
![image](images/External%20client%20bootstrap%20with%20broker0%20external%20ip.png)

#### Example of bootstrapping with broker1 EXTERNAL port
![image](images/External%20client%20bootstrap%20with%20broker1%20external%20ip.png)

#### Example of bootstrapping with broker0 INTERNAL port
![image](images/External%20client%20bootstrap%20with%20broker0%20internal%20ip.png)


### Consume locally
```bash
## The data consumed will be partial if listener names are not identical across all brokers
## You can change EXTERNAL to EXTERNAL2 in broker-1, then see the different results when running the two commands below
kafkacat -b ${Broker0}:${Broker0ExternalPort} -L -C -t quickstart-events
kafkacat -b ${Broker1}:${Broker1ExternalPort} -L -C -t quickstart-events
kafkacat -b ${Broker0}:${Broker0ExternalPort},${Broker1}:${Broker1ExternalPort} -L -C -t quickstart-events

## Consume using the INTERNAL listeners won't work
kafkacat -b ${Broker0}:${InternalPort} -L -C -t quickstart-events ## This won't work
kafkacat -b ${Broker1}:${InternalPort} -L -C -t quickstart-events ## This won't work
```

## Other commands
```bash
./zookeeper-shell.sh ${Broker0}:2181 get /brokers/ids/0

## For MSK, get zookeeper node through
aws kafka describe-cluster --cluster-arn <>

## Customize MSK listeners
./kafka-configs.sh \
  --bootstrap-server "b-1.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9092" \
  --alter \
  --entity-type brokers \
  --entity-name 1 \
  --add-config listeners=[CLIENT://0.0.0.0:9092,CLIENT_SECURE://0.0.0.0:9094,REPLICATION://0.0.0.0:9093,REPLICATION_SECURE://0.0.0.0:9095,EXTERNAL://0.0.0.0:9098],listener.security.protocol.map=[CLIENT:PLAINTEXT,CLIENT_SECURE:SSL,REPLICATION:PLAINTEXT,REPLICATION_SECURE:SSL,EXTERNAL:PLAINTEXT],advertised.listeners=[CLIENT://b-1.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9092,CLIENT_SECURE://b-1.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9094,REPLICATION://b-1-internal.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9093,REPLICATION_SECURE://b-1-internal.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9095,EXTERNAL://b-1-internal.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:4000]

## Read MSK customization
./kafka-configs.sh \
  --bootstrap-server "b-1.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:9092" \
  --describe \
  --entity-type brokers \
  --entity-name 1
  
./zookeeper-shell.sh z-1.chen-guo-msk.hcao8v.c6.kafka.us-east-1.amazonaws.com:2181 get /brokers/ids/1

```

## References
- https://aws.amazon.com/blogs/big-data/how-goldman-sachs-builds-cross-account-connectivity-to-their-amazon-msk-clusters-with-aws-privatelink/
- https://rmoff.net/2018/08/02/kafka-listeners-explained/
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-103%3A+Separation+of+Internal+and+External+traffic

