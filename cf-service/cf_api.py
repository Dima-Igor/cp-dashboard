from asyncio.windows_events import NULL
from re import sub
import aiohttp
import asyncio
import requests
from collections import namedtuple
import time

class CodeforcesAPI:
    def __init__(self):
        pass
    
    def api_response(self, url, params=None):
        try:
            tries = 0
            while tries < 5:
                tries += 1
                resp = requests.get(url)
                response = {}
                if resp.status_code == 503:
                    response['status'] = "FAILED"
                    response['comment'] = "limit exceeded"
                else:
                    response = resp.json()

                if response['status'] == 'FAILED' and 'limit exceeded' in response['comment'].lower():
                    time.sleep(1)
                else:
                    return response
            return response
        except Exception as e:
            return None

    def get_user_submissions(self, handle, count = 100, from_problem = 1):
        url = f"https://codeforces.com/api/user.status?handle={handle}"
        if count:
            url += f"&from={from_problem}&count={count}"

        response = self.api_response(url)
        if not response:
            return [False, "CF API Error"]
        if response['status'] != 'OK':
            return [False, response['comment']]
        try:
            data = []
            for x in response['result']:
                y = x['problem']
                rating = -1
                if 'rating' in y:
                    rating = y['rating']
                if 'verdict' not in x or x['verdict'] == 'TESTING':
                    continue
                
                problem = {}
                problem["handle"] = handle
                problem["problem_rating"] = rating
                problem["contest_id"] = y["contestId"]
                problem["problem_index"] = y["index"]
                problem["sub_time"] = x["creationTimeSeconds"]
                problem["verdict"] = x["verdict"]

                data.append(problem)
            return [True, data]
        except Exception as e:
            return [False, str(e)]

    async def get_recent_submissions(self, handle, timestamp = None):
        submissions = []
        if not timestamp:
            return self.get_user_submissions(handle)
        else:
            while True:
                status, submissions_slice = self.get_user_submissions(handle)
                if not status:
                    return status, submissions_slice
                done = False
                for submission in submissions_slice:
                    if len(submissions)==100:
                        done = True
                        break
                    if submission.sub_time > timestamp:
                        submission.append(submission)
                    else:
                        done = True
                        break
                if done:
                    break
                
            return [True, submissions]