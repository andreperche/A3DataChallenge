import ftplib
import os

inicialYear = 2010
finalYear = 2010
dirName = "data"
localPath = "Z:/Documentos/projetos_python/a3datachallenge/data"

ftp = ftplib.FTP("ftp.mtps.gov.br")
ftp.login("anonymous","")

data = []
ftp.cwd("pdet/microdados/RAIS")

listing = []
folderList = ftp.nlst() 
folderList.remove("Layouts")



#Creating folder to storage all data
try:
    os.mkdir(dirName)
    print("Diretório ", dirName, " criado.")
except FileExistsError:
    print("Diretório ", dirName, "já existe.")    


for f in folderList:        
    #Accessing each year folder (2010,2011,2012 ...)
    if(inicialYear <= int(f) <= finalYear):
        
        #Creating local dir
        try:
            subDirName = dirName +"/"+ f
            os.mkdir(subDirName)
            print("!---Diretório ", subDirName, " criado.")
        except FileExistsError:
            print("!---Diretório ", subDirName, " já existe!")

        print("+---Downloading Year: " + f)
        fileList = []            
        fileSizeList = []
        fileSizeList = ftp.nlst()            
            
        ftp.cwd(f)
        ftp.retrlines("LIST", fileList.append)            

        # Downloading each file within year folder
        for index in range(len(fileList)):                
            wordsFiles = fileList[index].split(None, 8)
            filename = wordsFiles[-1].lstrip()
                
                #wordsFileSize = fileSizeList[index].split(None, 8)
                #fileSize = wordsFileSize[-2].lstrip()                

                #print("|---Downloading File: ", filename, " Size: ", fileSize)                
                #local_filename = os.path.join(r+localPath, filename)
                #lf = open(local_filename, "wb")
                #ftp.retrbinary("RETR " + filename, lf.write, 8*1024)                
                #lf.close
'''
local_filename = os.path.join(r"Z:\Documentos\projetos_python\a3datachallenge", filename)
lf = open(local_filename, "wb")
ftp.retrbinary("RETR" + filename, lf.write, 8*1024)
lf.close
'''
