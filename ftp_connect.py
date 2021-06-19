import ftplib
from genericpath import getsize
from json.decoder import JSONDecodeError
import os
import json
from flatten_dict import flatten
from flatten_dict import unflatten
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pyarrow as pa
import glob

inicialYear = 2010
finalYear = 2019
dirName = "data"
versionControl = {}
dados = [{}]
# The versionControl is only used after first load server, so in future updates it won't need to load all files again, only the updated ones.
versionControlFile = "versionControl.json"
versionControlFileEmpty = False
versionControlFileUpdate = False
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
            #print(versionControl)
        except JSONDecodeError:
            versionControlFileEmpty = True                        
else:
    open(versionControlFile, "a")
    versionControlFileEmpty = True

for year in folderList:        
    ## Accessing each year folder (2010,2011,2012 ...)
    if(inicialYear <= int(year) <= finalYear):                  
        
        ## Creating local dir
        try:                
            subDirName = year + "/"           
            os.mkdir(subDirName)
            print("!---Directory ", subDirName, " created.")
        except FileExistsError:
            print("!---Directory ", subDirName, " already exists!")        
        
                
        fileList = []                            
        fileInfoList = []               
        ftp.cwd(year)        
            
        ftp.retrlines("LIST", fileList.append)                    
        totalFiles = len(fileList)

        ## Downloading/Extracting each file within year folder
        print("+---Downloading Year: " + year)        
        for index in range(totalFiles):
            
            ## Retrieving file details
            wordsFiles = fileList[index].split(None, 8)
            filename = wordsFiles[-1].lstrip()
            fileDate = wordsFiles[1].lstrip()
            fileHour = wordsFiles[2].lstrip()
            fileSize = ftp.size(filename)
            updateFile = False
            fileFound = False
            pathFile = subDirName + filename

            fileInfoList.append({"filename":filename, "size":fileSize, "date":fileDate, "hour":fileHour}) 

            ## Check if versionControlFile is empty and if file already exists and is updated
            try:
                if not versionControlFileEmpty:                
                    for i in versionControl["dados"] :
                        #print(i)
                        if i["year"] == year:
                            for j in i["files"]:
                                if j["filename"] == filename:
                                    fileFound = True
                                    if j["size"] != fileSize:                            
                                        updateFile = True
                                        versionControlFileUpdate = True                                                                        
            except:
                print("!---Error reading versionControl File!")

            ## Checking and Updating                                                                     
            if len(glob.glob(subDirName+filename)) > 0:                
                if os.stat(subDirName+filename).st_size != fileSize:
                    updateFile = True            
            if len(glob.glob(subDirName+filename)) > 0 or len(glob.glob(subDirName+filename[:-3]+".txt")) > 0 or len(glob.glob(subDirName+filename[:-3]+".parquet")) > 0:                                
                print("!---File already exists! (", (index+1),"/",totalFiles,")")                                                
                if not fileFound:
                    versionControlFileUpdate = True
                fileFound =  True                  
                                                            
            if updateFile or not fileFound:
                print(" ---Downloading File(",(index+1),"/",totalFiles,"): ", filename)              
                lf = open(pathFile, "wb")
                ftp.retrbinary("RETR " + filename, lf.write)
                lf.close                                                                                                             
        
        dados.append({"year":year, "files": fileInfoList})     

        ## Extracting 7z Data
        os.chdir(subDirName)
        if len(glob.glob("*.7z")) > 0:                                             
            files = glob.glob("*.7z")
            totalFiles = len(files)
            print("+---Extracting files")             
            for f in files:                 
                print(" -------File: ", f)                
                try:
                    os.system("py7zr x " + f)
                    os.remove(f)            
                except:
                    print(" -------File: ", f, " was not removed.")
        ## Parse to parquet file        
        if len(glob.glob("*.txt")) > 0:             
            files = glob.glob("*.txt") 
            print("+---Parsing parquet files") 
            for f in files: 
                print(" -------File: ", f)      
                arrow_table = pc.read_csv(f[:-4]+".txt", convert_options=pa.csv.ConvertOptions(include_columns=[ 'Qtd Hora Contr'                                                                                                        
                                                                                                                ,'Escolaridade após 2005'                                                                                                        
                                                                                                                ,'CNAE 2.0 Classe'
                                                                                                                ,'Sexo Trabalhador'
                                                                                                                ,'Vl Remun Média Nom'
                                            ]) ,parse_options=pc.ParseOptions(delimiter=";"),read_options=pc.ReadOptions(encoding='cp1252'))
                parquet_file = f[:-4] + ".parquet"
                pq.write_table(arrow_table, parquet_file)  
                try:
                    os.remove(f)
                except:
                    print(" -------File: ", f, " was not removed.")
        ## Go back to previous folder    
        os.chdir("../")
        try:
            ftp.cwd("../")
        except:
            ftp.login()
            ftp.cwd(ftpDir)                                          
ftp.quit()

## Remove first blank item
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


                        
            