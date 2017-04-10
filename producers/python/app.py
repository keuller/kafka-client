#!/usr/bin/env python

import threading, logging, time

## pip install kafka-python
from kafka import KafkaProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        count = 1
        while count <= 3:
            producer.send('sample', 'Message {} from python.'.format(count))
            time.sleep(2)
            count = count + 1


def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(8)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )

    main()
