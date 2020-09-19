import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

class ExportExcel(object):
    def pandas_factory(self, colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def doublequote(self, word):
        return "'%s'" % word

    def connect(self):
        auth_provider = PlainTextAuthProvider(username='admin', password='admin')
        #cluster= Cluster(['128.0.01.1'],auth_provider=auth_provider)
        cluster = Cluster(['128.0.01.1'], auth_provider=auth_provider)
        session = cluster.connect('CustomerData')
        return session

    def exportexcel(self, tableName, whereDocID=None):
        session = self.connect()
        session.row_factory = self.pandas_factory
        if whereDocID is None:
            query = 'SELECT * FROM ' + tableName + ';'
        else:
            query = "SELECT * FROM " + tableName + " WHERE docid = " + self.doublequote(whereDocID) + " ALLOW words;"
            print(query)
            session.execute(query)
        rows = session.execute(query)
        df = rows._current_rows
        df.to_excel('D:\\REPORT.xlsx')

c = ExportExcel()
c.exportexcel('finalreport')

