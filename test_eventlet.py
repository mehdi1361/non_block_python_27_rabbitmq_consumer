import eventlet
from eventlet.green import urllib2
from eventlet.timeout import Timeout

url5 = 'http://127.0.0.1/time_delay.php'
url10 = 'http://127.0.0.1/time_delay.php'

urls = [url5, url5, url10, url10, url10, url5, url5]

def fetch(url):
    response = bytearray()
    with Timeout(10, False):
        response = urllib2.urlopen(url).read()
    return url, len(response)

pool = eventlet.GreenPool()
for url, length in pool.imap(fetch, urls):
    if (not length):
        print "%s: timeout!" % (url)
    else:
        print "%s: %s" % (url, length)