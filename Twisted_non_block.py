#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-

import pika
import datetime
import eventlet
import json
import MySQLdb
from pika.adapters import twisted_connection
from twisted.internet import defer, reactor, protocol, task
from eventlet.green import urllib2
from eventlet.timeout import Timeout


def fetch(url):
	response = bytearray()
	with Timeout(2, False):
		try:
			response = urllib2.urlopen(url).read()
		except urllib2.HTTPError, e:
			print 'HTTP Error: %s, url: %s' % (str(e.code), url)
		except urllib2.URLError, e:
			print 'URL Error : %s, url: %s' % (str(e.reason), url)
		except urllib2.URLError, e:
			print 'URL Error : %s, url: %s' % (str(e.reason), url)
		except Exception:
			print 'Generic Exception %s' % url
	return url, len(response)


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
def error(result):
	print 'error'


@defer.inlineCallbacks
def read(queue_object):
	ch, method, properties, body = yield queue_object.get()

	if body:
		data = json.loads(body)
		urls = [data['clientURL']]
		text = data['text']
		pool = eventlet.GreenPool()
		db = MySQLdb.connect("127.0.0.1", "root", "13610522", "mmp_hamrah")
		db.set_character_set('utf8')
		cursor = db.cursor()
		cursor.execute('SET NAMES utf8;')
		cursor.execute('SET CHARACTER SET utf8;')
		cursor.execute('SET CHARACTER SET utf8;')
		result = cursor.execute(
			'''INSERT INTO inbound (user_id, service_id, message, status, description, created_at, updated_at)  VALUES (%s,%s,%s,%s,%s,%s,%s);''',
			(data['user_id'], data['service_id'], text, 200, 'recieve message sucessfull', datetime.datetime.now(), datetime.datetime.now()))
		db.commit()
		db.close()
		for url, length in pool.imap(fetch, urls):
			if not length:
				print "%s: timeout! FetchDate: %s" % (url, datetime.datetime.now())
			else:
				print "fetch_date : %s, date_response: %s,url: %s " % (data['FetchDate'],datetime.datetime.now(),url)
	yield ch.basic_ack(delivery_tag=method.delivery_tag)


parameters = pika.ConnectionParameters()
cc = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, parameters)
d = cc.connectTCP('localhost', 5672)
d.addCallback(lambda protocol: protocol.ready)
d.addCallback(run)
reactor.run()
