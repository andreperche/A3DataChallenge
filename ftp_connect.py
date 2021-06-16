import ftplib
from genericpath import getsize
from json.decoder import JSONDecodeError
import os
import py7zr
import json

inicialYear = 2019
finalYear = 2019
dirName = "data"
versionControl = {}
dados = [{}]
versionControlFile = "versionControl.json"
versionUpdateControlFile = "versionUpdateControl.json"
versionControlFileEmpty = "false"
#localPath = "Z:/Documentos/projetos_python/a3datachallenge/data"

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
    print("!---Directory ", dirName, " created.")
except FileExistsError:
    print("!---Directory ", dirName, " already exists!")    

os.chdir(dirName + "/")

if os.path.isfile(versionControlFile):            
        a_file = open(versionControlFile,"r")            
        content = a_file.read()        
        versionControl = json.loads(content)

        print(versionControl["dados"][0]["files"])
        #if os.path.isfile(versionUpdateControlFile):                      
        #    with open(versionUpdateControlFile) as file:            
        #        versionUpdateControlJson = json.load(file)            
        #        versionControlFileEmpty = "true"                         
else:
    open(versionControlFile, "a")

for year in folderList:        
    #Accessing each year folder (2010,2011,2012 ...)
    if(inicialYear <= int(year) <= finalYear):                  
        
        #Creating local dir
        try:
            #subDirName = dirName +"/"+ f + "/"        
            subDirName = year + "/"           
            os.mkdir(subDirName)
            print("!---Directory ", subDirName, " created.")
        except FileExistsError:
            print("!---Directory ", subDirName, " already exists!")        
        
        
        fileList = []            
        fileSizeList = []

        #Variables used for JSON, version control
        yearList = [] 
        fileInfoList = []               

        ftp.cwd(year)        
            
        ftp.retrlines("LIST", fileList.append)                    
        
        # Downloading/Extracting each file within year folder
        print("+---Downloading Year: " + year)
        for index in range(len(fileList)):
            
            # Retrieving file details
            wordsFiles = fileList[index].split(None, 8)
            filename = wordsFiles[-1].lstrip()
            fileDate = wordsFiles[1].lstrip()
            fileHour = wordsFiles[2].lstrip()
            fileSize = ftp.size(filename)
            pathFile = subDirName + filename
                        
            #Checking if file already exists and is updated        
            '''                            
            if os.path.isfile(pathFile):                                
                print("!---File already exists!")
                print("|---Extracting file")
                archive = py7zr.SevenZipFile(pathFile, mode='r')
                archive.extractall(subDirName)
                archive.close()                                    
                for i in versionControl["dados"] :
                    if i["year"] == year:
                        for j in i["files"]:
                            if j["filename"] == filename and j["size"] != fileSize:                            
                                print("update JSON")                            
                            else:   
                                fileInfoList.append({"filename":filename, "size":fileSize, "date":fileDate, "hour":fileHour})                            
            else:
                print("|---Downloading/Extracting File: ", filename)              
                lf = open(pathFile, "wb")
                ftp.retrbinary("RETR " + filename, lf.write, 8*1024)                
                lf.close                
            #local_filename = os.path.join(subDirName, filename)
            #print(subDirName)                            
            '''         
                #try:
                #    print("|---Extracting File: ", filename) 
                #    os.chdir(subDirName)
                #    print("DIRETORIO__:", os.path.curdir)
                #    archive = py7zr.SevenZipFile(local_filename, mode='r')
                #    archive.extractall(subDirName)
                #    archive.close()                    
                #except FileNotFoundError:
                #    print("!---File Not Found")        
        
        # Implementing or updating file Version Control
        
        #if versionControlFileEmpty == "true":
        dados.append({"year":year, "files": fileInfoList})     
        #print (dados)
        
        #Go back to previous folder    
        ftp.cwd("../")
            #wordsFileSize = fileSizeList[index].split(None, 8)
            #fileSize = wordsFileSize[-2].lstrip()                

#Remove first blank item
dados.pop(0)            



versionControl = {"dados": dados}
a_file = open(versionControlFile,"w")       
json.dump(versionControl,a_file)
a_file.close()

#a_file = open(versionControlFile,"r")
#output = a_file.read()
#print(output)
#Remove first blank item
#versionControl.pop(0)
#print(versionControl)
                        
                
'''
local_filename = os.path.join(r"Z:\Documentos\projetos_python\a3datachallenge", filename)
lf = open(local_filename, "wb")
ftp.retrbinary("RETR" + filename, lf.write, 8*1024)
lf.close
'''
