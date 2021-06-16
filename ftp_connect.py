import ftplib
from genericpath import getsize
from json.decoder import JSONDecodeError
import os
import py7zr
import json
from flatten_dict import flatten
from flatten_dict import unflatten
import time

inicialYear = 2010
finalYear = 2010
dirName = "data"
versionControl = {}
dados = [{}]
versionControlFile = "versionControl.json"
versionControlFileEmpty = False
versionControlFileUpdate = False
notExtracted = False
ftpDir = "pdet/microdados/RAIS" 

ftp = ftplib.FTP("ftp.mtps.gov.br")
ftp.login()
ftp.cwd(ftpDir)

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
        try:
            versionControl = json.loads(content)
        except JSONDecodeError:
            versionControlFileEmpty = True

        #print(versionControl["dados"][0]["files"])
        #if os.path.isfile(versionUpdateControlFile):                      
        #    with open(versionUpdateControlFile) as file:            
        #        versionUpdateControlJson = json.load(file)            
        #        versionControlFileEmpty = "true"                         
else:
    open(versionControlFile, "a")
    versionControlFileEmpty = True

for year in folderList:        
    #Accessing each year folder (2010,2011,2012 ...)
    if(inicialYear <= int(year) <= finalYear):                  
        
        #Creating local dir
        try:                
            subDirName = year + "/"           
            os.mkdir(subDirName)
            print("!---Directory ", subDirName, " created.")
        except FileExistsError:
            print("!---Directory ", subDirName, " already exists!")        
        
        ftp.login
        fileList = []                            
        fileInfoList = [] 
        ftp.login("anonymous","")
        ftp.cwd(ftpDir)               
        ftp.cwd(year)        
            
        ftp.retrlines("LIST", fileList.append)                    
        totalFiles = len(fileList)
        # Close connection because of timeout
        ftp.close
        # Downloading/Extracting each file within year folder
        print("+---Downloading Year: " + year)        
        for index in range(totalFiles):
            
            # Retrieving file details
            wordsFiles = fileList[index].split(None, 8)
            filename = wordsFiles[-1].lstrip()
            fileDate = wordsFiles[1].lstrip()
            fileHour = wordsFiles[2].lstrip()
            fileSize = ftp.size(filename)
            updateFile = False
            fileFound = False
            pathFile = subDirName + filename

            fileInfoList.append({"filename":filename, "size":fileSize, "date":fileDate, "hour":fileHour}) 

            # Check if versionControlFile is empty and if file already exists and is updated
            if not versionControlFileEmpty:                
                for i in versionControl["dados"] :
                    if i["year"] == year:
                        for j in i["files"]:
                            if j["filename"] == filename:
                                fileFound = True
                                if j["size"] != fileSize:                            
                                    updateFile = True
                                    versionControlFileUpdate = True                                                                        

            # Cheking and Updating                                                                     
            if os.path.isfile(pathFile):                                
                print("!---File already exists! (", (index+1),"/",totalFiles,")")                
                notExtracted = True                
                if not fileFound:
                    versionControlFileUpdate = True
                fileFound =  True                  
                                                            
            if updateFile or not fileFound:
                print(" ---Downloading File(",(index+1),"/",totalFiles,"): ", filename)              
                lf = open(pathFile, "wb")
                ftp.login()
                ftp.cwd(ftpDir)
                ftp.retrbinary("RETR " + filename, lf.write)                
                ftp.close
                lf.close      
                notExtracted = True                                                                                        
        
        dados.append({"year":year, "files": fileInfoList})     

        #Extract Data
        if notExtracted:
            
            os.chdir(subDirName)
            files = os.listdir()
            totalFiles = len(files)
            print("+---Extracting files")             
            for f in files:
                if "7z" in f: 
                    print(" -------File: ", f)
                    os.system("py7zr x " + f)
                    os.remove(f)
        #Go back to previous folder    
        os.chdir("../")                    

#Remove first blank item
dados.pop(0)
if not versionControlFileEmpty:
    aux = versionControl["dados"]
    dados.append(aux)
    print("!---Version Control Updated.")
if versionControlFileEmpty or versionControlFileUpdate:
    versionControl = {"dados": dados}
    a_file = open(versionControlFile,"w")       
    json.dump(versionControl,a_file)
    a_file.close()


                        
            