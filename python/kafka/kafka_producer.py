#!/usr/bin/env python3.7

import argparse
import datetime
import json
import sys
import time
from multiprocessing import Process

from confluent_kafka import Producer

from bootstrap_servers import *


class KafkaProducer(KafkaClientBase):
    PAYLOAD_ELEMENT_COUNT = 500  # Number of elements in the payload list

    def __init__(self, bootstrap_servers: str, sleep_time: float, cert_path: str, key_password: str):
        super(KafkaProducer, self).__init__(bootstrap_servers, cert_path, key_password)
        # payload size in MB
        self.MESSAGE_SIZE = sys.getsizeof(json.dumps([KafkaProducer.get_data()] * self.PAYLOAD_ELEMENT_COUNT)
                                          .encode("utf-8")) / 1024 / 1024
        self._sleep_time = sleep_time
        config = {
            'bootstrap.servers': f"{self._bootstrap_servers}",
        }
        producer_config = {**config, **self._ssl_config}
        print(f"Using producer config:\n{json.dumps(producer_config, sort_keys=True, indent=2)}")
        self._producer = Producer(producer_config)
        self._cpu_tic = None
        self._num_message = 0

    def produce(self, topic):
        wall_clock_begin = time.time()
        wall_clock_last = wall_clock_begin
        self._cpu_tic = time.process_time()

        while True:
            # produce is an asynchronous operation, the `callback` (alias `on_delivery`) will be called from poll()
            # when the message has been successfully delivered or permanently fails delivery.
            self._producer.poll(0)

            time.sleep(self._sleep_time)
            # Trigger any available delivery report callbacks from previous produce() calls
            # print(f"Number of events processed (callbacks served): {p.poll(0)}")

            payload = [self.get_data()] * (self.PAYLOAD_ELEMENT_COUNT - 1)
            # Append one more json separately to get an accurate "ts" for the last json.
            # Total JSON is 500 in the payload. The size of easy payload is 500KB.
            payload.append(self.get_data())

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.
            self._producer.produce(topic, json.dumps(payload).encode("utf-8"), callback=KafkaProducer.delivery_report)
            self._num_message += 1
            wall_clock_current = time.time()
            elapsed_seconds = wall_clock_current - wall_clock_begin
            if wall_clock_current - wall_clock_last > 1:
                print(
                    f"Total messages sent {self._num_message}. "
                    f"Current rate (#msg/s): {self._num_message / elapsed_seconds}/s "
                    f"Current throughput: {self._num_message * self.MESSAGE_SIZE / elapsed_seconds}MB/s")
                wall_clock_last = time.time()

    def __del__(self):
        if self._cpu_tic is None:
            print("Nothing to flush. Exiting directly...")
            return

        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
        print(f">>>> Flushing messages in the producer queue...")
        self._producer.flush()

        print(
            f">>>>>>  Total messages sent: {self._num_message}. Total size: {self._num_message * self.MESSAGE_SIZE}MB. "
            f"Total CPU time {time.process_time() - self._cpu_tic}  <<<<<<")

    @staticmethod
    def delivery_report(err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print(f'Message delivered {msg}')

    @staticmethod
    def get_data():
        """
        :return: the payload element that uses current time to build "ts"
        """
        return {
            "int0": 0,
            "int1": 1,
            "int1-": -1,
            "int8": 255,
            "int8-": -255,
            "int16": 256,
            "int16-": -256,
            "int32": 65536,
            "int32-": -65536,
            "nil": None,
            "true": True,
            "false": False,
            "float": 0.5,
            "float-": -0.5,
            "string0": "",
            "string1": "A",
            "string4": "foobarbaz",
            "string8": "Omnes viae Romam ducunt.",
            "string16": "L’homme n’est qu’un roseau, le plus faible de la nature ; mais c’est un roseau pensant. Il ne faut pas que l’univers entier s’arme pour l’écraser : une vapeur, une goutte d’eau, suffit pour le tuer. Mais, quand l’univers l’écraserait, l’homme serait encore plus noble que ce qui le tue, puisqu’il sait qu’il meurt, et l’avantage que l’univers a sur lui, l’univers n’en sait rien. Toute notre dignité consiste donc en la pensée. C’est de là qu’il faut nous relever et non de l’espace et de la durée, que nous ne saurions remplir. Travaillons donc à bien penser : voilà le principe de la morale.",
            "array0": [],
            "array1": [
                "foo"
            ],
            "array8": [
                1,
                2,
                4,
                8,
                16,
                32,
                64,
                128,
                256,
                512,
                1024,
                2048,
                4096,
                8192,
                16384,
                32768,
                65536,
                131072,
                262144,
                524288,
                1048576
            ],
            "map0": {},
            "map1": {
                "foo": "bar"
            },
            "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        }


class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers: str, sleep_time: float, cert_path: str, key_password: str):
        self._bootstrap_servers = bootstrap_servers
        self._sleep_time = sleep_time
        self._cert_path = cert_path
        self._key_password = key_password

    def start(self, topic):
        KafkaProducer(self._bootstrap_servers, self._sleep_time, self._cert_path, self._key_password).produce(topic)


def main(sleep_time, bootstrap_servers: str, cert_path: str, key_password: str):
    producer = KafkaProducerWrapper(bootstrap_servers, sleep_time, cert_path, key_password)
    jobs = []
    jobs.append(Process(target=producer.start, args=("AnotherTestTopic",)))

    for job in jobs:
        job.start()

    for job in jobs:
        job.join()


if __name__ == '__main__':
    """
    Example: 
    conda activate airflow
    ./kafka_producer.py -s 0.5 --bootstrap-servers xxx.kafka.us-east-1.amazonaws.com:9094 \
        --group-id 1 --cert-path /tmp/kafka-local-credentials --key-password password
    """
    parser = argparse.ArgumentParser(description='Run the Kafka producer with controllable event emitting rate')
    parser.add_argument("-s", "--sleep-time", "-f", "--frequency", type=float,
                        help='Sleep time in seconds between each message sent, '
                             'or the frequency in seconds that how often a message will be sent')
    parser.add_argument("-b", "--bootstrap-servers", type=str,
                        help='The bootstrap servers and port number of format <broker_url>:<port number 9094>')
    parser.add_argument("--cert-path", type=str,
                        help="The path to the client's certificate and key file for the TLS connection with MSK")
    parser.add_argument("--key-password", type=str, help="The key store's key password")
    args = parser.parse_args()
    main(sleep_time=args.sleep_time, bootstrap_servers=args.bootstrap_servers, cert_path=args.cert_path,
         key_password=args.key_password)
