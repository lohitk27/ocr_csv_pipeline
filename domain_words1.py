
import re 
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import Counter
import glob
class add_domainwords(object):
    
    def __init__(self):
        self.auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        self.cluster = Cluster(['128.0.0.1'],auth_provider=self.auth_provider)
        self.session = self.cluster.connect('CustomerData')
        self.domain_text =[]
    def singlecolumn(self,table_name):
        auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        cluster = Cluster(['128.0.0.1'],auth_provider=auth_provider)
        session = cluster.connect('CustomerData')
        self.contentslist = []
        row2='SELECT * FROM  ' + table_name + ';'
        row1 = session.execute(row2)
        for columns in row1:
            self.contentslist.append(columns[0])
        return self.contentslist
    
    def text_append(self):
        domain_files = glob.glob('./DomainWords/*.txt')
        # iterate over the list getting each file 
        for fle in domain_files:
            # open the file and then call .read() to get the text 
            with open(fle) as f:
                text_domain = f.read()
                self.domain_text.append(text_domain)
        return self.domain_text      

    
    def threecolumn(self,col_name1,col_name2,col_name3,table_name):
        auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        cluster = Cluster(['128.0.0.1'],auth_provider=auth_provider)
        session = cluster.connect('CustomerData')
        self.contentslist=[]
        row4='SELECT ' + col_name1 + ',' + col_name2 + ',' + col_name3 +  ' FROM ' + table_name + ';'
        row3 = self.session.execute(row4)
        for logdesc_row in row3:
            self.contentslist.append(logdesc_row[0])
            self.contentslist.append(logdesc_row[1])
            self.contentslist.append(logdesc_row[2])
        return self.contentslist
    
    def twocolumn(self,col_name1,col_name2,table_name):
        auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        cluster = Cluster(['128.0.0.1'],auth_provider=auth_provider)
        session = cluster.connect('CustomerData')
        self.countrylist=[]
        row6='SELECT ' + col_name1 + ',' + col_name2 + ' FROM ' + table_name + ';'
        row5 = self.session.execute(row6)
        for countrycent_row in row5:
            self.countrylist.append(countrycent_row[0])
            self.countrylist.append(countrycent_row[1])
        return self.countrylist
    
    def get_DBdata(self):
        namescatlist = self.singlecolumn('namescategory')
        projectscatlist =self.singlecolumn('projectscategory')
        companylist = self.singlecolumn('loggingcompany')
        country_list = self.twocolumn('country_name','region_name','country_centroid')
        self.domainwordslist = namescatlist+projectscatlist+companylist+country_list
        return self.domainwordslist
    
    def wordscleaning(self,lists):
        #lists = self.domainwordslist
        ################# removing special characters###################
        domainlist_special = []
        for k in lists:
            k1 = re.sub(r'[-?~|$|.|!()0-9+_*/=]',r' ',k) 
            domainlist_special.append(k1)
            #print("done")
        ##################### split words based on spaces######################
        domainlist_spaces=[]
        domainlist_spaces=[j.split() for j in domainlist_special]
        ####################### final output as a single list##################
        domainwords_finallist = []
        domainwords_finallist=[p for q in domainlist_spaces for p in q ]
        #print(domainwords_finallist)
        #################### removing duplicates###############################
        domainwords_finallist=list(set(domainwords_finallist))
        #print(domainwords_finallist)
        #wordcount=Counter(domainwords_finallist)
        #print(wordcount)
        return domainwords_finallist + self.domain_text

