import json
import pika
import db_interaction

class Worker:
    def __init__(self, cfg, db):
        self.host_mq = cfg["host"]
        self.port_mq = int(cfg["port"])
        self.user_mq = cfg["user"]
        self.password_mq = cfg["password"]

        self.db = db

        self.input_queue = cfg["task_queue"]

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host_mq, port=self.port_mq, credentials=pika.PlainCredentials(self.user_mq, self.password_mq))
        )

        self.channel = self.connection.channel()

    def send_message(self, message):
        self.channel.basic_publish(
            '', routing_key=self.output_queue, body=json.dumps(message))


    # get dict, return dict
    def make_statistic(self, submissions):
        user_task_history = {} 

        for sub in submissions:
            # print(sub)
            handle = sub['handle']
            if handle not in user_task_history:
                user_task_history[handle] = []
            
            user_task_history[handle].append(sub)

        stat = []

        for user, task_history in user_task_history.items():
            solved = set() 
            all_task = set()
            sub_count = 0
            average_solved = 0

            #make history for each user
            for task in task_history:
                task_id = task['problem_id']
                all_task.add(task_id)
                if (task['verdict'] == 'OK' and task_id not in solved):
                    solved.add(task_id)
                    average_solved += task['problem_rating']  

            res = average_solved // len(solved) if len(solved) != 0 else 0

            user_stat = {
                'handle' : user, 
                'solved_count' : len(solved), 
                'unsolved_count' : len(all_task) - len(solved), 
                'submissions': len(task_history), 
                'average_solved' : res
            }

            stat.append(user_stat)

        return stat


    def handle_message(self, ch, method, properties, body):
        message = json.loads(body)

        #print(message)

        stat = self.make_statistic(message['submissions'])
        
        for row in stat:
            self.db.insert(message['assignment_id'], row)

        # stat['assignment_id'] = message['assignment_id']
        
        print("statistic which was written in database: ", stat, end='\n\n')
        self.channel.basic_ack(method.delivery_tag)


    def run_worker(self):
        self.channel.basic_consume(
            queue=self.input_queue, on_message_callback=self.handle_message, auto_ack=False
        )

        print(f"Worker is waiting for messages on {self.host_mq}:{self.port_mq}")
        self.channel.start_consuming()


with open("config.json", "r") as f:
    cfg = json.load(f)

with open("db_config.json", "r") as f:
    db_cfg = json.load(f)  


db = db_interaction.PgDatabase(db_cfg)

worker = Worker(cfg, db)
worker.run_worker()

# db._insert(table="statistic", fields={
#             "assignment_id": 5})

# conn = db.conn
# conn.cursor

# sql_string = "INSERT INTO statistic (assignment_id) VALUES (5) ON CONFLICT DO NOTHING"

# with conn.cursor() as cursor:
#     cursor.execute(sql_string)

#     conn.commit()
#     # result = cursor.fetchall()
#     # print(result)
