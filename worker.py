import requests
from celery import Celery

app = Celery('tasks', broker='amqp://guest@localhost//')

@app.task
def send(client_url):
    requests.get(client_url, timeout=0.001)
