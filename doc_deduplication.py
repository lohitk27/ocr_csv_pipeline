# -*- coding: utf-8 -*-

import pandas as pd


class DocumentDeDuplication(object):
    def __init__(self):
        pass
    
    def fun_split(sen1,sen2):
        sen1 = sen1.split('|')
        sen2 = sen2.split('|')
    
        sen1 = sorted(sen1)
        sen2 = sorted(sen2)
    
        if sen1 == sen2:
            return 1
        else:
            return 0
    
    def deDuplicate(self):
        data = pd.read_csv('D:/doc_.csv',encoding='latin')

        attributes= data.columns 
        
        mandatory_names = ['Data Type','Category','Content''Latitude','Longitude']
        mandatory_projects = ['Data Type','Category','Content','DocTitle','Country (s)']
        mandatory_files = ['Data Type','Category','Content','Process Detail','DocLineName']
        
        multiple_values = ['Category','Content','Process Detail']
        columns = ['FileName','isDuplicate', 'ParentFileName']
        resultsDF = pd.DataFrame(columns=columns)
        
        for i in range(0,data.shape[0]-1):
            for j in range(i+1,data.shape[0]):
                data1 = data.iloc[i]
                data2 = data.iloc[j]
                #print("file1:", data1.loc["FileName"])
                #print("file2:", data2.loc["FileName"])
                count = 0
                check = 0

                for k in range(0,len(data1)):
                    if (data1.loc['Data Type']=="str" and data2.loc['Data Type']=="int"):
                        
                        if attributes[k] in mandatory_names:
                            check = check + 1
                            if attributes[k] in multiple_values:                    
                                value = self.fun_split(str(data1[k]),str(data2[k]))
                                if(value==1):
                                    count = count+1
                            else:
                                if (type(data1[k]) == float or type(data2[k]) ==float):   
                                    count = count+1
                                #if (type(data1[k]) == float and type(data2[k]) ==float):
                                    #count = count-1
                                if(data1[k] == data2[k]):
                                    count = count + 1
                    if (data1.loc['Data Type']=="PDF" and data2.loc['Data Type']=="docx"):
                        if attributes[k] in mandatory_projects:
                            check = check + 1
                            if attributes[k] in multiple_values:                    
                                value = self.fun_split(str(data1[k]),str(data2[k]))
                                if(value==1):
                                    count = count+1
                            else:
                                if (type(data1[k]) == float or type(data2[k]) ==float):   
                                    count = count+1
                                #if (type(data1[k]) == float and type(data2[k]) ==float):
                                    #count = count-1
                                if(data1[k] == data2[k]):
                                    count = count + 1
                    if (data1.loc['Data Type']=="Projects" and data2.loc['Data Type']=="Projects"):
                        if attributes[k] in mandatory_files:
                            check = check + 1
                            if attributes[k] in multiple_values:                    
                                value = self.fun_split(str(data1[k]),str(data2[k]))
                                if(value==1):
                                    count = count+1
                            else:
                                if (type(data1[k]) == float or type(data2[k]) ==float):   
                                    count = count+1
                                #if (type(data1[k]) == float and type(data2[k]) ==float):
                                    #count = count-1
                                if(data1[k] == data2[k]):
                                    count = count + 1
        
                if count == check:
                    if(int(data1.loc['Line Number']) > int(data2.loc['Line Number'])):
                        
                        resultsDF = resultsDF.append({'FileName':data1.loc['FileName'],
                                              'isDuplicate':'no',
                                              'ParentFileName':data1.loc['FileName']}, ignore_index=True)
                        
                        resultsDF = resultsDF.append({'FileName':data2.loc['FileName'],
                                              'isDuplicate':'yes',
                                              'ParentFileName':data1.loc['FileName']}, ignore_index=True)
                        
                    else:
                        
                        resultsDF = resultsDF.append({'FileName':data2.loc['FileName'],
                                              'isDuplicate':'no',
                                              'ParentFileName':data2.loc['FileName']}, ignore_index=True)
                        
                        resultsDF = resultsDF.append({'FileName':data1.loc['FileName'],
                                              'isDuplicate':'yes',
                                              'ParentFileName':data2.loc['FileName']}, ignore_index=True)
        
            resultsDF = resultsDF.append({'FileName':data1.loc['FileName'],'isDuplicate':'no','ParentFileName':data1.loc['FileName']}, ignore_index=True)
        
        resultsDF.to_csv('D:/deDuplicationResults.csv')