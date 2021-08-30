#!/usr/bin/env python3.7

import argparse
import json
from multiprocessing import Process

from bootstrap_servers import *
from confluent_kafka import Consumer, TopicPartition


class KafkaConsumerAssign(KafkaClientBase):

    def __init__(self, bootstrap_servers: str, consumer_group_id: str, cert_path: str, key_password: str):
        super(KafkaConsumerAssign, self).__init__(bootstrap_servers, cert_path, key_password)
        self._consumer_group = f"kss_test_group_{consumer_group_id}"
        config = {
            'bootstrap.servers': f"{self._bootstrap_servers}",
            'group.id': f"{self._consumer_group}",
            # 'auto.offset.reset': 'earliest',
        }
        consumer_config = {**config, **self._ssl_config}
        print(f"Using consumer config:\n{json.dumps(consumer_config, sort_keys=True, indent=2)}")
        self._consumer = Consumer(consumer_config)

    def consume(self, topic):
        print(self._consumer.get_watermark_offsets(TopicPartition("AnotherTestTopic", 0)))
        print(self._consumer.get_watermark_offsets(TopicPartition("AnotherTestTopic", 1)))
        print(self._consumer.get_watermark_offsets(TopicPartition("AnotherTestTopic", 2)))

        # self._consumer.subscribe([topic])
        self._consumer.assign([TopicPartition("AnotherTestTopic", 8, 663)])

        batch_size = 2
        while True:
            # By default, timeout is set to -1, which means the request will block, if the number of available messages
            # is less than the number requested.
            print(f"Requesting {batch_size} messages ...")
            msgs = self._consumer.consume(batch_size)
            print(f"Size of msgs: {len(msgs)}")

            for msg in msgs:
                if msg.error():
                    raise Exception("Consumer error: {}".format(msg.error()))

                msg_value = msg.value().decode('utf-8')
                print(f"Message received within {self._consumer_group}. "
                      f"{msg.topic()}-{msg.partition()}-{msg.offset()}: {msg.key()}->{msg_value}")


class KafkaConsumerAssignWrapper:
    def __init__(self, bootstrap_servers: str, consumer_group_id: str, cert_path: str, key_password: str):
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group_id = consumer_group_id
        self._cert_path = cert_path
        self._key_password = key_password

    def start(self, topic):
        KafkaConsumerAssign(self._bootstrap_servers, self._consumer_group_id, self._cert_path,
                            self._key_password).consume(
            topic)


def main(bootstrap_servers, group_id, cert_path, key_password):
    consumer = KafkaConsumerAssignWrapper(bootstrap_servers, group_id, cert_path, key_password)
    jobs = []
    jobs.append(Process(target=consumer.start, args=("AnotherTestTopic",)))

    for job in jobs:
        job.start()

    for job in jobs:
        job.join()


if __name__ == '__main__':
    """
    Example: 
    conda activate airflow
    ./kafka_consumer_assign.py --bootstrap-servers xxx.kafka.us-east-1.amazonaws.com:9094 \
        --group-id 1 --cert-path /tmp/kafka-local-credentials --key-password password
    """
    parser = argparse.ArgumentParser(description='Run the Kafka consumer')
    parser.add_argument("-b", "--bootstrap-servers", type=str,
                        help='The bootstrap servers and port number of format <broker_url>:<port number 9094>')
    parser.add_argument("--group-id", type=str,
                        help="The consumer group ID. It should be an integer.")
    parser.add_argument("--cert-path", type=str,
                        help="The path to the client's certificate and key file for the TLS connection with MSK")
    parser.add_argument("--key-password", type=str, help="The key store's key password")
    args = parser.parse_args()
    main(bootstrap_servers=args.bootstrap_servers, group_id=args.group_id, cert_path=args.cert_path,
         key_password=args.key_password)
