import asyncio
import websockets
import json
import grpc
import submission_pb2_grpc
import submission_pb2

addr = "localhost:8090"

with grpc.insecure_channel(addr) as channel:
    stub = submission_pb2_grpc.CodeforcesServiceStub(channel)
    counter = 0
    for result in stub.GetSubmissions(submission_pb2.SubmissionRequest(query_type ="period", time_type = "week")):
        print(result)
        counter+=1
    
    print(f"Got {counter} messages")


