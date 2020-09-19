# -*- coding: utf-8 -*-

from picklist_search import Trie, Node
from cassandra import Cassandra
import pandas as pd
import uuid

db = Cassandra()
trie = Trie()
class metadata_extraction:
    def __init__(self):
        self.db = Cassandra()
        self.trie = Trie()

    def extract_AttributePicklist(self,dataFrame,tablename,col="CorrectedText"):
        rows = self.db.getDataFromTable(tablename)
        attributeList=[]
        for playlist_row in rows:
            attributeList.append(playlist_row[0].lower())
        print(attributeList)
        for i in attributeList:
            self.trie.add(i)
        attributeResults =[]
        df_metadata = pd.DataFrame(columns=["Image_name","roinumber","DocID","Docname","PageNumber"])
        for index,row in dataFrame.iterrows():
        	word_found = self.trie.has_word(str(row[col]).lower())
        	if word_found:
        		Image_name = row['Image_name']
        		ROI_width  = row['ROI_width']
        		ROI_height = row['ROI_height']
        		roinumber   = row['roinumber']
        		DocID		= row['DocID']
        		Docname     =row['Docname']
        		PageNumber  =row['PageNumber']
        		insertQuery = "INSERT INTO NLPTable (uid, docid, docname, pagenumber, roinumber, roiheight, roiwidth, content ) VALUES  (?, ?, ?, ?, ?, ?, ?, ?)" 
        		session = db.connect()
        		prepared = session.prepare(insertQuery)
        		session.execute(prepared, (uuid.uuid1(),DocID,Docname, PageNumber,roinumber,ROI_height,ROI_width, ' '.join(word_found)))
        		data = [{'Image_name': Image_name, 'ROI_width': ROI_width, 'ROI_height': ROI_height, 'content': word_found, 'roinumber':roinumber, 'DocID':DocID, 'Docname':Docname, 'PageNumber':PageNumber}]	
        		df_metadata = df_metadata.append(data, ignore_index=False)
        return df_metadata



filepath = "/home/Ocr_Text.csv"
dataFrame = pd.read_csv(filepath)
m= metadata_extraction()
content  = m.extract_AttributePicklist(dataFrame,'Content')
