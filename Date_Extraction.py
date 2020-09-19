# -*- coding: utf-8 -*-

import dateutil.parser as dateParser
import pandas as pd

class DateExtraction(object):
    def __init__(self, data=None):
        pass


    def ValidateDate(self,dateTxt):
        try:
            return dateParser.parse(dateTxt).date()
        except:
            pass
    
    
# This function can extract following formats
# 23-12-1985 , 23rd Dec, 2004  ,21 December 2004 , March 25, 2014 ,  23/09/1892, 05-mar-1967, December 2020  
    def extract_DatesFromDocument(self,dataFrame, col="corrected_text"):
        dates = []
        #patterns = ["\d{1,2}-\d{1,2}-\d{2,4}","\d{1,2}\s?(?:st|nd|rd|th)?\s+[a-zA-Z]+,?\s*\d{2,4}","[a-zA-Z]+\s*\d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4}","\d{1,2}\/\d{1,2}\/\d{2,4}","\d{1,2}-[a-zA-Z]+-\d{2,4}","[a-zA-Z]+\s+\d{2,4}"]
        mon_pat = "[J|j|Fe|fe|M|m|A|a|Se|se|Oc|oc|No|no|De|de]"
        patterns = ["\d{1,2}-\d{1,2}-\d{2,4}",
                    "\d{1,2}\/\d{1,2}\/\d{2,4}",
                    "\d{1,2}\s?(?:st|nd|rd|th)?\s+"+mon_pat+"\w+,?\s*\d{2,4}",
                    mon_pat+"\w+\s*\d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4}",     
                    "\d{1,2}-"+mon_pat+"\w+-\d{2,4}",
                    mon_pat+"\w+\s+\d{2,4}"]
        #patterns = ["\d{1,2}-\d{1,2}-\d{2,4}","\d{1,2}\s?(?:st|nd|rd|th)?\s+[J|j|F|f|M|m|A|a|S|s|O|o|N|n|D|d]\w+,?\s*\d{2,4}"]

        for i in range(0,len(patterns)):
            #print(pat)
            result = dataFrame[col].str.findall(patterns[i])#r"\d{1,2}-\d{1,2}-\d{2,4}")#|\d{1,2}\s?(?:st|nd|rd|th)?\s+[a-zA-Z]+,?\s*\d{2,4}|[a-zA-Z]+\s*\d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4}|\d{1,2}\/\d{1,2}\/\d{2,4}|\d{1,2}-[a-zA-Z]+-\d{2,4}|[a-zA-Z]+\s+\d{2,4}")
            result = result.apply(lambda x: pd.Series(x) if x else pd.Series()).dropna()
            extracted_dates = result.values.flatten()
            #print(extracted_dates)
            #print(extracted_dates)
            for dateTxt in extracted_dates:
                if dateTxt.find("at") == -1 and dateTxt.find("and") == -1:
                    #print(dateTxt)
                    validDate = self.ValidateDate(dateTxt)
                    if validDate != None:
                        if validDate.year < 2017 and validDate.year > 1600:
                            dates.append("%s-%s-%s"%(validDate.day,validDate.month,validDate.year))
        return dates

#import pandas as pd
#df = pd.DataFrame([{"ocr_output":"12th apr, 1923"}])   
#df = pd.read_csv(path)    
#d = DateExtraction()
#d.extract_DatesFromDocument(df,"ocr_output")

'''
import os
path  = ("/home/")
d = DateExtraction()

count = 1
_, _, files = next(os.walk(path))
for file in files:
    filePath = path+"/"+file
    if os.path.getsize(filePath) > 10:
        df = pd.read_csv(filePath,error_bad_lines = False)
    
        if df.empty:
            continue
        print("Date###########",d.extract_DatesFromDocument(df,"ocr_output"))
    count +=1
    
    if count > 30:
        break

'''


