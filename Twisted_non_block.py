import pika
import datetime
import eventlet
import json
import MySQLdb
from pika.adapters import twisted_connection
from twisted.internet import defer, reactor, protocol, task
from eventlet.green import urllib2
from eventlet.timeout import Timeout


@defer.inlineCallbacks
def run(connection):
    channel = yield connection.channel()

    exchange = yield channel.exchange_declare(exchange='topic_link', type='topic')

    queue = yield channel.queue_declare(queue='dispatcher_queue', auto_delete=False, exclusive=False)

    yield channel.queue_bind(exchange='topic_link', queue='dispatcher_queue', routing_key='hello.world')

    yield channel.basic_qos(prefetch_count=1)

    queue_object, consumer_tag = yield channel.basic_consume(queue='dispatcher_queue', no_ack=False)

    l = task.LoopingCall(read, queue_object)

    l.start(0.01)


@defer.inlineCallbacks
def read(queue_object):
    ch, method, properties, body = yield queue_object.get()

    if body:
        data = json.loads(body.decode("utf-8"))
        # requests.get(data['clientURL'], timeout=0.01, verify=False)
        urls = [data['clientURL']]
        print(data)
        # print('message=%r send in %s' % (body, start_date))

        pool = eventlet.GreenPool()
        db = MySQLdb.connect("localhost", "root", "13610522", "mmp_hamrah")
        cursor = db.cursor()
        result = cursor.execute(
            '''INSERT INTO inbound (user_id, service_id, message, status, description, created_at, updated_at)  VALUES (%s,%s,%s,%s,%s,%s,%s);''',
            (data['user_id'], data['service_id'], data['text'], 200, 'recieve message sucessfull', datetime.datetime.now(), datetime.datetime.now()))
        db.commit()
        db.close()
        for url, length in pool.imap(fetch, urls):
            if (not length):
                print "%s: timeout!" % (url)
            else:
                print "%s: %s" % (url, length)
    yield ch.basic_ack(delivery_tag=method.delivery_tag)


def fetch(url):
    response = bytearray()
    with Timeout(0.01, False):
        response = urllib2.urlopen(url).read()

    return url, len(response)


parameters = pika.ConnectionParameters()
cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, parameters)
d = cc.connectTCP('localhost', 5672)
d.addCallback(lambda protocol: protocol.ready)
d.addCallback(run)
reactor.run()
