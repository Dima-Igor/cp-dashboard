import json
import socket
import utils
import sqlite3
import pika
import asyncio

class Client:
    def __init__(self, cfg):
        self.host = cfg["host"]
        self.port = int(cfg["port"])
        self.user = cfg["user"]
        self.password = cfg["password"]

        self.output_queue = cfg["statistic_input"]
        self.input_queue = cfg["statistic_output"]

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host, port=self.port, credentials=pika.PlainCredentials(self.user, self.password))
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(self.output_queue)
        self.channel.queue_declare(self.input_queue)

    def send_message(self, message):
        self.channel.basic_publish(
            '', routing_key=self.output_queue, body=json.dumps(message))


    def run_server(self):
        self.channel.basic_consume(
            queue=self.input_queue, on_message_callback=self.handle_message, auto_ack=False
        )

        # print(f"Worker is waiting for messages on {self.host}:{self.port}")
        self.channel.start_consuming()


    def handle_message(self, ch, method, properties, body):
        print(body)



def main():
    with open("config.json", "r") as f:
        cfg = json.load(f)

    client = Client(cfg)

    client.run_server()

    # asyncio.run(client.run_server())

    '''
        handle: string,
        sub_time: int
        verdict: string
        problem_rating: int
    '''


    array =  [
        {'handle' : "dimasidorenko", 'problem_id' : 32, 'sub_time': 50, 'verdict' : 'OK', "problem_rating" : 500 }, 
        {'handle' : "Skeef79", 'problem_id' : 57, 'sub_time': 50, 'verdict' : 'OK', "problem_rating" : 500 }]

    client.send_message(array)


self._insert(table="statistic", fields={
            "assignment_id": assignment_id})

