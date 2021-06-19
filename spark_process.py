import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.functions import lit, col, regexp_replace, when
from pyspark.sql.types import StructType,StructField, StringType, ShortType
from pyspark.sql import SparkSession

import os
import glob

##Start Spark Session
spark = SparkSession.builder.appName("ProcessamentoA3Challenge") \
         .getOrCreate()


## Server Folder Structure
dirName = "data/"
dirDF = "dirDF/"

try:
    os.mkdir(dirName+"/"+dirDF)
    print("!---Directory ", dirDF, " created.")
except FileExistsError:
    print("!---Directory ", dirDF, " already exists!")  

## Creates Empty RDD
empty = spark.sparkContext.emptyRDD()

## Create Default Schema
schema = StructType([
              StructField('qtd_hora_contr',           ShortType(),True),                            
              StructField('escolaridade',             ShortType(),True),                                      
              StructField('cnae_2_classe',            ShortType(),True),              
              StructField('sexo_trabalhador',         ShortType(),True),
              StructField('vl_remun_media_nom',       StringType(),True),                            
              StructField('regiao',                   StringType(),True),                
              StructField('sexo',                     StringType(),True),              
              StructField('cnae',                     StringType(),True),
              StructField('vl_remun_media_dec',       ShortType(),True),              
              StructField('ano',                      StringType(),True)
              
          ])

## Dictionary for managing Brazil Regions
regioes = {'DF':'Centro-Oeste'
            ,'GO':'Centro-Oeste'
            ,'MS':'Centro-Oeste'
            ,'MT':'Centro-Oeste'
            ,'AL':'Nordeste'
            ,'BA':'Nordeste'
            ,'CE':'Nordeste'
            ,'MA':'Nordeste'
            ,'PB':'Nordeste'
            ,'PE':'Nordeste'
            ,'PI':'Nordeste'
            ,'RN':'Nordeste'
            ,'SE':'Nordeste'
            ,'AC':'Norte'
            ,'AM':'Norte'
            ,'AP':'Norte'
            ,'PA':'Norte'
            ,'RO':'Norte'
            ,'RR':'Norte'
            ,'TO':'Norte'
            ,'ES':'Sudeste'
            ,'MG':'Sudeste'
            ,'RJ':'Sudeste'
            ,'SP':'Sudeste'
            ,'PR':'SUL'
            ,'RS':'SUL'
            ,'SC':'SUL'}

os.chdir(dirName)
folders = os.listdir()
for fo in folders:
    if os.path.isdir(fo) and fo != dirDF[:5]:
        edf = spark.createDataFrame(empty,schema)
        os.chdir(fo)
        print("+---Year: ",fo)
        pf = []
        files = glob.glob('*.parquet')        
        for f in files:
            if(f.upper()[:4] != "ESTB" and f.upper()[5:10] != "ESTAB"): 
                print("+---File: ",f)                
                if int(fo) < 2018:
                    regiao = regioes[f[:2]].upper()
                else:
                    regiao = f.replace('RAIS_VINC_PUB_','').replace('.parquet','')
                if regiao == 'SP' or regiao == 'MG_ES_RJ':
                    regiao = 'SUDESTE' 
                ## Loading Parquet and Renaming Columns
                arquivo = pq.read_table(f,columns=['Qtd Hora Contr'                                                                                                        
                                                   ,'Escolaridade após 2005'                                                                                                        
                                                   ,'CNAE 2.0 Classe'
                                                   ,'Sexo Trabalhador'
                                                   ,'Vl Remun Média Nom'                                                    
                                        ]).rename_columns([ 'qtd_hora_contr'                                                                                                                                                    
                                                            ,'escolaridade'                                                                                                                                                                       
                                                            ,'cnae_2_classe'          
                                                            ,'sexo_trabalhador'            
                                                            ,'vl_remun_media_nom'
                                        ])
                pq.write_table(arquivo,"../"+dirDF+"x"+f)                                                                             
                sdf = spark.read.parquet(dirName+dirDF+"x"+f)
                df = sdf.withColumn("regiao",lit(regiao))\
                        .withColumn("sexo", when(col('sexo_trabalhador')==1,'M').otherwise('F'))\
                        .withColumn("cnae",col("cnae_2_classe").cast(StringType()))\
                        .withColumn("vl_remun_media_dec",regexp_replace(col('vl_remun_media_nom'),',','.').cast('int'))\
                        .withColumn("ano",lit(fo))
                                              
                edf = edf.union(df) 
        print("!-----Processing ", fo)            
        result = edf.groupBy("ano","cnae")\
            .count()\
            .filter(col("cnae").like("62%"))

        ## Recording Result in DataBase 
        print("!-----Writing Database: ", result.count(), " records")
        mode = "append"
        url = "jdbc:postgresql://localhost:5432/postgres"
        tableName = "rais_q3"
        properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
        result.write.jdbc(url=url, table=tableName, mode=mode, properties=properties)            
    os.chdir("../")


