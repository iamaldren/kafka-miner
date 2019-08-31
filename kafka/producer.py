import threading
import time
import json

from kafka import KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        x = '{"name": "aldren", "position": "developer"}'
        y = json.loads(x)

        while not self.stop_event.is_set():
            producer.send('testnew', y)
            time.sleep(1)

        producer.close()
