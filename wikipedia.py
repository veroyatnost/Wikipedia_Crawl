from multiprocessing import Queue
import threading
import requests
from bs4 import BeautifulSoup
import pymysql
import time
import getpass

user = input('SQL USER:')
passwd = getpass.getpass('SQL PASSWORD:')
num_threadurl = int(input('Number of URL Threads:'))
num_threadcrawl = int(input('Number of Crawl Threads:'))
conn = pymysql.connect(host='localhost',
                       user='%s' % user,
                       passwd='%s' % passwd,
                       db='wikipedia',
                       charset='utf8mb4')
cur = conn.cursor()
key_queue = Queue()
url_queue = Queue()
host = 'http://en.wikipedia.org/wiki/'
s = requests.session()
threads_url = []
threads_crawl = []
lock = threading.Lock()
block_list = ['Main_Page', 'Main_page', 'main_page', '#', ':']

if lock.acquire():
    cur.execute('SELECT * FROM English WHERE Flag = "0"')
    for item in cur:
        key_queue.put(item[1])
    if key_queue.empty():
        key = input('Start with:')
        key_queue.put(key)
        url_queue.put(key)
    else:
        url_queue.put(item[1])
    lock.release()

class ThreadURL(threading.Thread):
    def __init__(self, key_queue, url_queue):
        threading.Thread.__init__(self)
        self.key_queue = key_queue
        self.url_queue = url_queue

    def run(self):
        while True:
            try:
                global block_list, host
                key = self.url_queue.get()
                link = host + key
                link_requests = s.get(link)
                link_requests.encoding = 'UTF-8'
                soup_link = BeautifulSoup(link_requests.text, 'lxml')
                for item in (soup_link.find_all('div', id='mw-content-text')):
                    for a in item.find_all('a'):
                        temp_key = a.get('href')
                        try:
                            temp_key = temp_key.split('/wiki/')[1]
                            flag = 0
                            for block in block_list:
                                if temp_key.find(block) != -1:
                                    flag = 1
                                    break
                            if flag == 1:
                                continue
                        except:
                            continue
                        if temp_key != '' and len(temp_key) < 255:
                            temp_key = temp_key.replace('\"', ' ')
                            if lock.acquire():
                                cur.execute('SELECT COUNT(*) FROM English WHERE English = "' + temp_key + '"')
                                lock.release()
                            for item in cur:
                                if item[0] == 0:
                                    if lock.acquire():
                                        cur.execute('INSERT INTO English (English) VALUES ("' + temp_key + '")')
                                        conn.commit()
                                        self.key_queue.put(temp_key)
                                        self.url_queue.put(temp_key)
                                        lock.release()
                                else:
                                    break
            except:
                print('URL', key)
                continue


class ThreadCrawl(threading.Thread):
    def __init__(self, key_queue):
        threading.Thread.__init__(self)
        self.key_queue = key_queue

    def run(self):
        while True:
            try:
                global host
                if self.key_queue.empty():
                    time.sleep(5)
                    continue
                key = self.key_queue.get()
                link = host + key
                link_requests = s.get(link)
                link_requests.encoding = 'UTF-8'
                soup_link = BeautifulSoup(link_requests.text, 'lxml')
                if lock.acquire():
                    cur.execute('SELECT id FROM English WHERE English = "' + key + '"')
                    for item in cur:
                        id_e = str(item[0])
                    lock.release()
                for item in (soup_link.find_all('a', class_='interlanguage-link-target')):
                    item = item.get('title')
                    devided = item.rfind(' – ')
                    if devided == -1:
                        continue
                    expression = item[:devided].replace('\"', ' ')
                    method = item[devided + 3:]
                    if method != 'English' and len(expression) < 255:
                        if lock.acquire():
                            cur.execute('INSERT INTO `' + method + '` VALUES ("' + expression + '" ,"' + id_e + '")')
                            conn.commit()
                            lock.release()
                if lock.acquire():
                    cur.execute('UPDATE English SET Flag = 1 WHERE id = ' + id_e)
                    conn.commit()
                    lock.release()
                print (id_e, key)
            except:
                print ('INSERT INTO `' + method + '` VALUES ("' + expression + '" ,"' + id_e + '")')
                self.key_queue.put(key)
                continue


def main():
    for i in range(num_threadurl):
        threads_url.append(ThreadURL(key_queue, url_queue))
        threads_url[i].start()
    time.sleep(10)
    for i in range(num_threadcrawl):
        threads_crawl.append(ThreadCrawl(key_queue))
        threads_crawl[i].start()


if __name__ == '__main__':
    main()