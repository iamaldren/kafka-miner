import threading, logging, time, json
import multiprocessing

from kafka import KafkaConsumer, KafkaProducer


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
            producer.send('test', y)
            time.sleep(1)

        producer.close()


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='latest',
                                 consumer_timeout_ms=1000,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['test'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
