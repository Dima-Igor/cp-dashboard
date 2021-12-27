import json
import pika

class RabbitMQScheduler:
    with open("config.json", "r") as f:
        cfg = json.load(f)

    def __init__(self, cfg):
        self.host = cfg["host"]
        self.port = int(cfg["port"])
        self.user = cfg["user"]
        self.password = cfg["password"]

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host, port=self.port, credentials=pika.PlainCredentials(self.user, self.password))
        )

        # Очередь куда мы кладем задачу
        self.task_queue = cfg["task_queue"]
        self.channel = self.connection.channel()
        self.channel.queue_declare(self.task_queue)

    # queue_output - название очереди (string), task = задача в формате dictionary
    def send_task(self, task):
        self.channel.basic_publish(
            exchange = '', routing_key=self.task_queue, body=json.dumps(task))
        


array =  [
    {'handle' : "dimasidorenko", 'problem_id' : 32, 'sub_time': 50, 'verdict' : 'OK', "problem_rating" : 500 }, 
    {'handle' : "Skeef79", 'problem_id' : 57, 'sub_time': 50, 'verdict' : 'OK', "problem_rating" : 500 }]


sample_task = {
    'assignment_id' : 10, 
    'submissions' : array,
}

#scheduler = RabbitMQScheduler(cfg)
#scheduler.send_task(sample_task)











