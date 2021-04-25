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

## It would also work if 0.0.0.0 is changed to the private IP 10.1.0.94, but not 127.0.0.1
## Listener names must be identical across all brokers
## Each listener also must have a different port
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:4000
inter.broker.listener.name=INTERNAL

## The port number is used by Kafka to match and tell which advertised listener should be returned as a response
## If internal is INTERNAL://abc:9092, the host name abc will be returned if client connects through 9092
## If external is EXTERNAL://xyz:4000, the host name xyz will be returned if client connects through 4000

## Alternatively INTERNAL://ip-10-1-0-94.ec2.internal:9092 can be used, in this case
## - ip-10-1-0-94.ec2.internal is the broker-0's hostname
## - An A-Record for ip-10-1-0-94.ec2.internal to 10.1.0.94 needs to be added in the DNS
advertised.listeners=INTERNAL://10.1.0.94:9092,EXTERNAL://ec2-54-162-247-190.compute-1.amazonaws.com:4000
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
advertised.listeners=INTERNAL://10.1.0.76:9092,EXTERNAL://ec2-54-162-193-150.compute-1.amazonaws.com:4001
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
export Broker0=ec2-54-162-247-190.compute-1.amazonaws.com
export Broker1=ec2-54-162-193-150.compute-1.amazonaws.com
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


## References
- https://aws.amazon.com/blogs/big-data/how-goldman-sachs-builds-cross-account-connectivity-to-their-amazon-msk-clusters-with-aws-privatelink/
- https://rmoff.net/2018/08/02/kafka-listeners-explained/
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-103%3A+Separation+of+Internal+and+External+traffic

