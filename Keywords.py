

###################### wikipedia words from database #######################
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import Counter
import time

class add_wikiwords(object):
    def __init__(self):
        self.auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        self.cluster = Cluster(['128.0.0.1'],auth_provider=self.auth_provider)
        self.session = self.cluster.connect('CustomerData')
    
    def newengwords(self):
        #start_time = time.time()
        wikilist=[]
        rows = self.session.execute('SELECT * FROM keywords;')
        for wiki in rows:
            wikilist.append(wiki.word)
        #print(wikilist)
        #print(len(wikilist))
        #print("--- %s seconds ---" % (time.time() - start_time))
        return wikilist + ['data','teams','cannot','applicable','trianed','step','svg','officers','reference']
    
    def shutdown(self):
        self.session.shutdown()
        return "connection closed successfully"



