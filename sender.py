import pika
import json
for i in range(1000):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='dispatcher_queue')
    data = {
        "mobile": '09212799520',
        "service_id": 1,
        "FetchDate": 't',
        "clientURL": 'http://127.0.0.1/time_delay.php',
        "user_id": 1,
        "text": "Hello world"
    }
    j_data = json.dumps(data)
    channel.basic_publish(exchange='',
                          routing_key='dispatcher_queue',
                          body=j_data)

    print(" [x] Sent 'Hello World! %s'" % i)
    connection.close()
