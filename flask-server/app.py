from flask import Flask, render_template, url_for, request, redirect
from random import randint
import requests
import time

app = Flask(__name__)

submissions = set()

class Submission:
    def __init__(self, handle, problem, verdict):
        self.handle = handle
        self.problem = problem
        self.verdict = verdict

@app.route('/', methods = ['POST', 'GET'])
def index():
    return render_template('index.html', submissions = submissions)

@app.route('/statistics', methods = ['POST','GET'])
def stats():
    if request.method == 'POST':
        data = [Submission("skeef79", "1562A", "OK"),
        Submission("dimasidorenko", "1562B","TL")]

        submissions.add(data[0])
        submissions.add(data[1])

        return redirect('/')

@app.route('/update', methods = ['POST'])
def update_table():
    if request.method == 'POST':
        submissions.add(Submission("kek","lol","jopa")) 
        return redirect('/')

if __name__ == '__main__':
    app.run(debug = True)
