#!/usr/bin/env python3.7

import argparse
import json
from multiprocessing import Process

from confluent_kafka import Consumer

from bootstrap_servers import *


class KafkaConsumerSubscribe(KafkaClientBase):

    def __init__(self, bootstrap_servers: str, consumer_group_id: str, cert_path: str, key_password: str):
        super(KafkaConsumerSubscribe, self).__init__(bootstrap_servers, cert_path, key_password)
        self._consumer_group = f"kss_test_group_{consumer_group_id}"
        config = {
            'bootstrap.servers': f"{self._bootstrap_servers}",
            'group.id': f"{self._consumer_group}",
            # 'auto.offset.reset': 'earliest',
        }
        consumer_config = {**config, **self._ssl_config}
        print(f"Using consumer config:\n{json.dumps(consumer_config, sort_keys=True, indent=2)}")
        self._consumer = Consumer(consumer_config)
        # Determine how often the metrics will be logged by setting the number of messages consumed for each report
        self._metric_report_interval = 10

    def consume(self, topic):
        self._consumer.subscribe([topic])
        count = 0
        tic = None

        while True:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue

            if tic is None:
                tic = time.process_time()

            if msg.error():
                raise Exception("Consumer error: {}".format(msg.error()))
            count += 1

            msg_value = msg.value().decode('utf-8')

            if count % self._metric_report_interval == 0:
                msg_json = json.loads(msg_value)
                latency = datetime.datetime.now() - datetime.datetime.strptime(msg_json[-1]['ts'],
                                                                               "%Y-%m-%d %H:%M:%S.%f")
                print(
                    f"{self._consumer_group} metric for every {self._metric_report_interval} messages"
                    f"({self._metric_report_interval * sys.getsizeof(msg_value) / 1024 / 1024}MB):\n"
                    f"Latency: {latency}.\n"
                    f"Time to consume: {time.process_time() - tic}")

            value = f"(truncated){msg_value[:200]}" if random.randint(1, 100) == 1 else "value skipped"
            print(f"{count} messages received within {self._consumer_group}. "
                  f"{msg.topic()}-{msg.partition()}-{msg.offset()}: {msg.key()}->{value}")


class KafkaConsumerSubscribeWrapper:
    def __init__(self, bootstrap_servers: str, consumer_group_id: str, cert_path: str, key_password: str):
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group_id = consumer_group_id
        self._cert_path = cert_path
        self._key_password = key_password

    def start(self, topic):
        KafkaConsumerSubscribe(self._bootstrap_servers, self._consumer_group_id, self._cert_path,
                               self._key_password).consume(
            topic)


def main(bootstrap_servers, group_id, cert_path, key_password):
    consumer = KafkaConsumerSubscribeWrapper(bootstrap_servers, group_id, cert_path, key_password)
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
    ./kafka_consumer_subscribe.py --bootstrap-servers xxx.kafka.us-east-1.amazonaws.com:9094 \
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
