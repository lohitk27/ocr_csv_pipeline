import subprocess
import os,sys
from subprocess import check_output
import PyPDF2
import pandas as pd
from cassandra_dao import Cassandra

class pdf_img(object):
    def _init_(self):
        self.db = Cassandra()


        
    def run_onefolder(self,var):

        ImagePath = subprocess.check_output(['bash','./pdf.sh',var])
        print (ImagePath)
        df= pd.DataFrame()
        ImagePath1 = ImagePath.rstrip().decode("utf-8")
        #print(ImagePath1)
        cwd = os.getcwd()
        #print(cwd)
        folderpath = cwd + '/DocPngs/' + ImagePath1
        #print(folderpath)


  
        df['pagelocation'] =[folderpath]
        #print(df)

        return df

    def  returndf(self,docpath):
        self.db = Cassandra()
        pdfobj = open(docpath , "rb")
        pdf = PyPDF2.PdfFileReader(pdfobj)
        npage = pdf.getNumPages()
        doclocation, docname = os.path.split(docpath)
        #print(doclocation)
        docimage = []
        docimagepage1 = []
        docimagelocation = []

        docid = docname[:-4]
        print(docid)
        for docimagepage in range(npage):
            if npage < 10:
                docimage1 = str(-(docimagepage+1)) + '.png'
                docimage.append(docimage1)
            if npage < 100 and npage>=10:
                if docimagepage<9:
                    docimage1 = '-' + '0' + str(docimagepage+1) + '.png'
                    docimage.append(docimage1)
                if docimagepage<100 and docimagepage>=9:
                    docimage1 = '-' + str(docimagepage+1) + '.png'
                    docimage.append(docimage1)
            if npage < 1000 and npage >=99:
                if docimagepage<9:
                    docimage1 = '-' + '00' + str(docimagepage+1) + '.png'
                    docimage.append(docimage1)
                if docimagepage<99 and docimagepage>=9:
                    docimage1 = '-' + '0' + str(docimagepage+1) + '.png'
                    docimage.append(docimage1)
                if docimagepage < 1000 and docimagepage >=99:
                    docimage1 = '-'  + str(docimagepage+1) + '.png'
                    docimage.append(docimage1)

            #docimage1 = str(-(docimagepage+1)) + '.png'
            #print("**")
            #print(docimage1)
            #docimage.append(docimage1)
            docimageloc = docid  +'/'+docimage1
            cwd = os.getcwd()
            docimagelocation1 =  cwd  +'/DocPngs/'+ docimageloc
            #print(docimagelocation1)
            docimagelocation.append(docimagelocation1)
            docimagepage1.append(docimagepage+1)
            #db.InsertttoPdfDoctoImage(docid,docname,docimagepage,docimagelocation,doclocation)

        #print(docimage)
        #print(docimagepage1)
        df = pd.DataFrame({'docid':docid,'docname':docname,'docimagepage':docimagepage1,'docimagelocation':docimagelocation,'doclocation':doclocation})
        #print(df)
        #print('**')
        df.to_csv('test.csv',index = True)
        #print('**')
        self.db.InsertttoDocAdmin(df)
        #print(df)
        
        
            

 



        


'''
s = pdf_img()
p = s.run_onefolder('/home/LNTIES/20145794/MntPnt/TimeStudy/16-Nov/Reports_109/D_H012951033.pdf')
print(p)
q = s.returndf('/home/LNTIES/20145794/MntPnt/TimeStudy/16-Nov/Reports_109/D_H012951033.pdf')

'''