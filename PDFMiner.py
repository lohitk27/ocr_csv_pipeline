# -*- coding: utf-8 -*

import os
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
# From PDFInterpreter import both PDFResourceManager and PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
#from pdfminer.pdfdevice import PDFDevice
# Import this to raise exception whenever text extraction from PDF is not allowed
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.layout import LAParams, LTTextBox, LTTextLine, LTFigure
from pdfminer.converter import PDFPageAggregator

import pandas as pd

from cassandra import Cassandra
from ResultGeneration import Results_generator

#os.chdir("/home/")
#x = PdfReader('NcolasStudy.pdf')

class PDFProcessor(object):
    
    def __init__(self):
        self.isTextAvailable = False
        self.db = Cassandra()
        self.imagePages = []
        self.totalPages = 0
        self.Results = Results_generator()
        
    
    def GetData(self, input_file_path,output_path, toCsv = True):
        
        password = ""
        extracted_text = ""
        
        fileName = input_file_path.split('/')[-1]
        # Open and read the pdf file in binary mode
        fp = open(input_file_path, "rb")
        
        # Create parser object to parse the pdf content
        parser = PDFParser(fp)
        
        # Store the parsed content in PDFDocument object
        document = PDFDocument(parser, password)
        
        # Check if document is extractable, if not abort
        if not document.is_extractable:
        	raise PDFTextExtractionNotAllowed
        	
        # Create PDFResourceManager object that stores shared resources such as fonts or images
        rsrcmgr = PDFResourceManager()
        
        # set parameters for analysis
        laparams = LAParams()
        
        # Create a PDFDevice object which translates interpreted information into desired format
        # Device needs to be connected to resource manager to store shared resources
        # device = PDFDevice(rsrcmgr)
        # Extract the decive to page aggregator to get LT object elements
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        
        # Create interpreter object to process page content from PDFDocument
        # Interpreter needs to be connected to resource manager for shared resources and device 
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        
        
        df = pd.DataFrame()
        pageNo = 0
        pageText = ""
        # Ok now that we have everything to process a pdf document, lets process it page by page
        for page in PDFPage.create_pages(document):
        # As the interpreter processes the page stored in PDFDocument object
            interpreter.process_page(page)
        # The device renders the layout from interpreter
            layout = device.get_result()
        	# Out of the many LT objects within layout, we are interested in LTTextBox and LTTextLine
            for lt_obj in layout:
                if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                    if not toCsv:
                        extracted_text += lt_obj.get_text()
                    else:
                        txt = lt_obj.get_text().encode('utf-8').strip().decode('utf-8') #windows-1252')
                        pageText += txt
                            
                        #row = pd.Series([pageNo,0,lt_obj.bbox[0],lt_obj.bbox[1],lt_obj.bbox[2],lt_obj.bbox[3], txt])
                        
                        
                        row = pd.Series([fileName.split('.')[0],pageNo,0,output_path,0,0,page.mediabox[3],page.mediabox[2],13, (lt_obj.bbox[3] - lt_obj.bbox[1]), (lt_obj.bbox[2]-lt_obj.bbox[0]),0,0,(page.mediabox[3]-lt_obj.bbox[3]),lt_obj.bbox[3],0,0,False,False, txt,3])
                        df = df.append(row,ignore_index=True)
                        
            if pageText:
                self.isTextAvailable = True
            else:
                self.imagePages.append(pageNo)
                
            pageText = ""
            pageNo += 1  
            print(pageNo)
            
        self.totalPages = pageNo
        
        			
        #close the pdf file
        fp.close()
        
        if toCsv:
            if not df.empty:
                df.columns = ['docname','pagenumber','line_number','pagelocation','font_size','line_height','line_width','line_x','line_y','table_column_number','table_row_number','underline','color','ocr_output','pipelinename']      

            return df
        else:
            return extracted_text



        
    def ExtractTextToTxt(self, input_file_path, output_path):
        file_extn = input_file_path.split(".")[-1]
        if file_extn == "pdf":
            text = PDFProcessor.GetData(self,input_file_path,output_path,False)
            fileName = input_file_path.split('/')[-1]
            output_file_path = output_path +"/"+ fileName.split('.')[0]+'.txt'

            with open(output_file_path, "wb") as fp:
                fp.write(text.encode("utf-8"))

        
        
    def ExtractTextToCsv(self, input_file_path, output_path):
        
        print("###################PDFMiner###############")
        self.imagePages = []
        self.totalPages = 0
        file_extn = input_file_path.split(".")[-1]
        if file_extn == "pdf":
            df = PDFProcessor.GetData(self,input_file_path,output_path)
            
            if self.isTextAvailable == False:
                return self.GetEmptyDF(),self.imagePages
                
            fileName = input_file_path.split('/')[-1]
            output_file_path = output_path +"/"+ fileName.split('.')[0]+'.csv'
            print("#############",self.isTextAvailable)
            
            pagename = ['NA']*len(df)
            docname = fileName.split(".")[0]
            docname = [docname]*len(df)
            csvPath = [output_file_path]*len(df)
            orientation = ['NA']*len(df)
            outputDataFrame =self.Results.csv_generator2(csvPath,docname,df['pagenumber'],df['pagelocation'][0],df['ocr_output'],0,df['line_x'],df['line_y'],df['line_height'],df['line_width'],df['font_size'],pagename,df['underline'],df['table_row_number'],df['table_column_number'],df['color'])

            
            
            #fileName = input_file_path.split('/')[-1]
            #output_file_path = output_path +"/"+ fileName.split('.')[0]+'.csv'
        
            outputDataFrame.to_csv(output_file_path, index = False)
            
            # Insert df to table.
            #self.db.InsertttoROITable(outputDataFrame)
            
            return outputDataFrame,self.imagePages
        else:
            return pd.DataFrame(), self.imagePages
            
            
        
    def ExtractText(self,input_file_path, output_path):
        self.isTextAvailable = False
        self.imagePages = []
        self.ExtractTextToCsv(input_file_path, output_path)
        return self.isTextAvailable,self.imagePages
        
    def GetEmptyDF(self):
        df = pd.DataFramedf = pd.DataFrame( columns =
            ['ocr_output', 
            'line_number',
            'line_x', 
            'line_y',
            'line_height',
            'line_width',
            'font_size',
            'docname',
            'pagenumber',
            'pagelocation',
            'Image_name',
            'table_row_number',
            'table_column_number',
            'color'
            ])
        return df


        
#pdfMiner = PDFProcessor()








