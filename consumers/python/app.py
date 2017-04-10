#!/usr/bin/env python

import threading, logging, time

## pip install kafka-python
from kafka import KafkaConsumer

class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        consumer.subscribe(['sample'])

        for message in consumer:
            print ('Python consumer: {}'.format(message.value))


def main():
    threads = [
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(30)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )

    main()
