import multiprocessing
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import pymongo
import time

Client = pymongo.MongoClient('localhost',27017)
db = Client.wikipedia
BlockList = ['Main_Page', 'Main_page', 'main_page', '#', ':']
Process = []
lock = multiprocessing.Lock()
KeyQueue = multiprocessing.Queue()
if KeyQueue.empty():
    Start = ['en', 'Film', 'en']
    KeyQueue.put(Start)


class ProcessCrawl(multiprocessing.Process):
    def __init__(self, KeyQueue):
        multiprocessing.Process.__init__(self)
        self.KeyQueue = KeyQueue

    
    def run(self):
        while True:
            try:
                global BlockList
                if self.KeyQueue.empty():
                    time.sleep(30)
                    continue
                Key = self.KeyQueue.get()
                print (Key)
                HrefLanguage = Key[0]
                Keyword = Key[1]
                KeyLanguage = Key[2]
                Url = 'http://' + HrefLanguage + '.wikipedia.org/wiki/' + Keyword
                LinkRequests = requests.session().get(Url)
                LinkRequests.encoding = 'UTF-8'
                self.GetKeyword(LinkRequests, Keyword, KeyLanguage)
                self.GetText(LinkRequests, HrefLanguage, Keyword, KeyLanguage)
                print (db.Keyword.find({}).count(), db.Text.find({}).count(), self.KeyQueue.qsize())
            except:
                print (HrefLanguage, Keyword, KeyLanguage)
                self.terminate()

                
    def FormatKeyword(self, Keyword):
        Keyword = Keyword.replace('\"', '').replace('.', '-').replace('\000', '_')
        return Keyword
    

    def GetKeyword(self, LinkRequests, Keyword, KeyLanguage):
        TempKeyword = self.FormatKeyword(Keyword)
        if db.Keyword.find({KeyLanguage + '.Keyword':TempKeyword}).count() == 0:
            AOnly = SoupStrainer('a', class_='interlanguage-link-target')
            Soup = BeautifulSoup(LinkRequests.text, 'lxml', parse_only=AOnly)
            Post = {KeyLanguage:{'Keyword':Keyword}}
            for item in Soup.find_all('a'):
                raw = item.get('title')
                while True:
                    devided = raw.rfind(' – ')
                    if devided != -1:
                        break
                TempKeyword = raw[:devided]
                TempKeyLanguage = item.get('lang')
                TempHrefLanguage = item.get('hreflang')
                self.KeyQueue.put([TempHrefLanguage, TempKeyword, TempKeyLanguage])
                TempKeyword = self.FormatKeyword(TempKeyword)
                Post[TempKeyLanguage] = {'Keyword':TempKeyword}
            if lock.acquire():
                db.Keyword.insert(Post)
                lock.release()


    def GetText(self, LinkRequests, HrefLanguage, Keyword, KeyLanguage):
        DivOnly = SoupStrainer('div', id='mw-content-text')
        Soup = BeautifulSoup(LinkRequests.text, 'lxml', parse_only=DivOnly)
        Text = str(Soup.get_text)
        TempKeyword = self.FormatKeyword(Keyword)
        db.Text.insert({TempKeyword:Text})
        _id = db.Text.find_one({TempKeyword:Text})['_id']
        db.Keyword.update_one({KeyLanguage:{'Keyword':Keyword}},{'$set':{KeyLanguage:{'Keyword':Keyword,'_id':_id}}})
        for item in Soup.find_all('a'):
            Keyword = item.get('href')
            try:
                Keyword = Keyword.split('/wiki/')[1]
                flag = 0
                for Block in BlockList:
                    if Keyword.find(Block) != -1:
                        flag = 1
                if flag == 1:
                    continue
            except:
                continue
            if Keyword != '':
                TempKeyword = self.FormatKeyword(Keyword)
                if db.Keyword.find({KeyLanguage + '.Keyword':TempKeyword}).count():
                    break
                self.KeyQueue.put([HrefLanguage, Keyword, KeyLanguage])


def main():
    for i in range(1):
        Process.append(ProcessCrawl(KeyQueue))
        Process[i].start()
        time.sleep(1)


if __name__ == '__main__':
    main()