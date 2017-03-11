from multiprocessing import Queue
import threading
import requests
from bs4 import BeautifulSoup
import pymysql
import time
import getpass

key_queue = Queue()
url_queue = Queue()
host = 'http://en.wikipedia.org/wiki/'
key = 'Film'
# key = input('Start with:')
key_queue.put(key)
url_queue.put(key)
s = requests.session()
threads_url = []
threads_crawl = []
lock = threading.Lock()
user = input('SQL USER:')
passwd = getpass.getpass('SQL PASSWORD:')
conn = pymysql.connect(host='localhost',
                       user='%s' % user,
                       passwd='%s' % passwd,
                       db='wikipedia',
                       charset='utf8')
cur = conn.cursor()


class ThreadURL(threading.Thread):
    def __init__(self, key_queue, url_queue):
        threading.Thread.__init__(self)
        self.key_queue = key_queue
        self.url_queue = url_queue
        self.lock = threading.Lock()

    def run(self):
        while True:
            try:
                global host
                key = self.url_queue.get()
                link = host + key
                link_requests = s.get(link)
                link_requests.encoding = 'UTF-8'
                soup_link = BeautifulSoup(link_requests.text, 'html.parser')
                for item in (soup_link.find_all('div', id='mw-content-text')):
                    for a in item.find_all('a'):
                        temp_key = a.get('href')
                        try:
                            temp_key = temp_key.split('/wiki/')[1]
                        except:
                            continue
                        try:
                            temp_key = temp_key.split('#')[1]
                            continue
                        except:
                            pass
                        if temp_key != '' and len(temp_key) < 255:
                            temp_key = temp_key.replace('\"', '')
                            if lock.acquire():
                                cur.execute('SELECT COUNT(*) FROM wikipedia WHERE English = "' + temp_key + '"')
                                lock.release()
                            for r in cur:
                                if r[0] != 0:
                                    break
                                else:
                                    if lock.acquire():
                                        cur.execute('INSERT INTO wikipedia (English) VALUES ("' + temp_key + '")')
                                        conn.commit()
                                        lock.release()
                if lock.acquire():
                    cur.execute('SELECT * FROM wikipedia')
                    for item in cur:
                        if item[2] is None:
                            key_queue.put(item[1])
                            url_queue.put(item[1])
                    lock.release()
            except:
                print('URL', key)
                continue


class ThreadCrawl(threading.Thread):
    def __init__(self, key_queue):
        threading.Thread.__init__(self)
        self.key_queue = key_queue
        self.lock = threading.Lock()

    def run(self):
        while True:
            try:
                global host
                if Queue.empty():
                    time.sleep(10)
                    continue
                key = self.key_queue.get()
                link = host + key
                link_requests = s.get(link)
                link_requests.encoding = 'UTF-8'
                soup_link = BeautifulSoup(link_requests.text, 'html.parser')
                test = 1
                for item in (soup_link.find_all('a', class_='interlanguage-link-target')):
                    item = item.get('title')
                    devided = item.split(' – ')
                    expression = devided[0]
                    method = devided[1]
                    if method == 'Chinese':
                        if lock.acquire():
                            cur.execute('UPDATE wikipedia SET Chinese = "' + expression + '" WHERE English = "' + key +'"')
                            conn.commit()
                            lock.release()
                            test = 0
                if lock.acquire():
                    if test == 1:
                        cur.execute('UPDATE wikipedia SET Chinese = "None" WHERE English = "' + key +'"')
                        conn.commit()
                    lock.release()
            except:
                print(key)
                continue


def main():
    for i in range(4):
        threads_url.append(ThreadURL(key_queue, url_queue))
        threads_url[i].start()
    time.sleep(10)
    for i in range(200):
        threads_crawl.append(ThreadCrawl(key_queue))
        threads_crawl[i].start()


if __name__ == '__main__':
    main()
