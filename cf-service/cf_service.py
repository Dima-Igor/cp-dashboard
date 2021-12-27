from cf_api import CodeforcesAPI
import asyncio
import websockets
import time
import datetime
import json
import grpc
import submission_pb2
import submission_pb2_grpc
from concurrent import futures

class CodeforcesService(submission_pb2_grpc.CodeforcesService):
    def __init__(self):
        self.cfApi = CodeforcesAPI()
        with open("users.json") as f:
            self.users = json.load(f)

    def get_recent(self):
        submissions = []
        for handle in self.users:
            status, result = self.cfApi.get_user_submissions(handle=handle, count=100)
            #TODO handle request fail 
            submissions+=result

        submissions = (sorted(submissions, key = lambda item: item["sub_time"]))[::-1]
        return submissions[:min(len(submissions),100)] #get last 100 items

    def get_slice(self, time_type):
        start_time = datetime.datetime.fromtimestamp(time.time())
        end_time = start_time
        if time_type == "day":
            end_time -= datetime.timedelta(days=1)
        elif time_type == "week":
            end_time -= datetime.timedelta(days=7)
        elif time_type == "month":
            end_time -= datetime.timedelta(days=30)
        
        start_time = start_time.timestamp()
        end_time = end_time.timestamp()

        submissions = []

        for handle in self.users:
            done = False
            from_problem = 1
            while not done:
                status, result = self.cfApi.get_user_submissions(handle=handle, count=100, from_problem=from_problem)
                #TODO handle request fail
                for submission in result:
                    if submission["sub_time"] < end_time:
                        done = True
                        break
                    else:
                        submissions.append(submission)
                if not done:
                    from_problem+=100
          
        return submissions

    def GetSubmissions(self, request, context):
        if request.query_type == "recent":
            submissions = self.get_recent()
            print(submissions)

        elif request.query_type == "period":
            if request.time_type not in ["day", "week", "period"]:
                yield submission_pb2.SubmissionReply(
                    status = "Incorrect time_type"
                )
            submissions = self.get_slice(request.time_type)
        else:
            yield submission_pb2.SubmissionReply(
                status = "Incorrent query_type"
            )

        print(submissions)
        for submission in submissions:
            yield submission_pb2.SubmissionReply(
                handle = submission["handle"],
                contest_id = submission["contest_id"],
                problem_index = submission["problem_index"],
                sub_time =  submission["sub_time"],
                verdict = submission["verdict"],
                status = "OK"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submission_pb2_grpc.add_CodeforcesServiceServicer_to_server(CodeforcesService(), server)
    
    addr = "localhost:8090"
    print(f"Codeforces service is listening on {addr}")
    server.add_insecure_port(addr)
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
