# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 19:52:26 2021

@author: Perche
"""

## Install Dependecies
#pip install pyspark
#pip install pandas

from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.functions import lit, col, regexp_replace, when
from pyspark.sql.types import StructType,StructField, StringType, ShortType
from pyspark.sql import SparkSession

import os
import glob
# print('Job start: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

##################################  #SETUP ####################################

# ##Set Spark parameters
# """ !?WTF?! Once the physical memory is exhausted, swap will be used instead. 
#             If you have enough swap configured, you will be able to make use of 16 GB. 
#             But your process will most likely slow down to a crawl when it starts swapping.
# """
# conf = SparkConf().setMaster("local").setAppName("My application").set("spark.executor.memory", "16g")
# sc = SparkContext(conf)


##Start Spark Session
spark = SparkSession.builder.appName("ProcessamentoA3Challenge") \
         .getOrCreate()
#         # .master("local[1]")



dirName = "data"
dirSummary = "summary"

## Creates Empty RDD
empty = spark.sparkContext.emptyRDD()

## Create Default Schema
schema = StructType([
              StructField('qtd_hora_contr',           ShortType(),True),
              StructField('idade',                    ShortType(),True),
              StructField('faixa_tempo_emprego',      ShortType(),True),
              StructField('escolaridade',             ShortType(),True),
              StructField('faixa_etaria',             ShortType(),True),
              StructField('vinculo_ativo_12',         ShortType(),True),
              StructField('cbo_ocupacao_2002',        ShortType(),True),
              StructField('cnae_2_classe',            ShortType(),True),
              StructField('sexo_trabalhador',         ShortType(),True),
              StructField('vl_remun_media_nom',       StringType(),True),
              StructField('tipo_vinculo',             ShortType(),True), 
              StructField('regiao',                   StringType(),True),  
              StructField('sexo',                     StringType(),True),  
              StructField('vl_remun_media_dec',       ShortType(),True)
          ])

edf = spark.createDataFrame(empty,schema)

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
            ,'PR':'Sul'
            ,'RS':'Sul'
            ,'SC':'Sul'}

print(regioes['DF'])

folders = os.listdir(dirName)
os.chdir(dirName)
for fo in folders:
    if os.path.isdir(fo) and fo != 'summary':
        os.chdir(fo)
        print("+---Year: ",fo)
        pf = []
        files = glob.glob('*.parquet')        
        for f in files:
            ## Rename Parquet Columns
            if int(fo) < 2018:
                regiao = regioes[f[:2]].upper()
            else:
                regiao = f.replace('RAIS_VINC_PUB_','').replace('.parquet','')
                if regiao == 'SP' or regiao == 'MG_ES_RJ':
                   regiao = 'SUDESTE'
            arquivo = pq.read_table(f,columns=[ 'Qtd Hora Contr'.encode('cp1252')
                                            ,'Idade'.encode('cp1252')
                                            ,'Faixa Tempo Emprego'.encode('cp1252')
                                            ,'Escolaridade após 2005'.encode('cp1252')
                                            ,'Faixa Etária'.encode('cp1252')
                                            ,'Vínculo Ativo 31/12'.encode('cp1252')
                                            ,'CNAE 2.0 Classe'.encode('cp1252')
                                            ,'Sexo Trabalhador'.encode('cp1252')
                                            ,'Vl Remun Média Nom'.encode('cp1252')
                                            ,'Tipo Vínculo'.encode('cp1252')
                                        ]).rename_columns([ 'qtd_hora_contr',           
                                                            'idade',                    
                                                            'faixa_tempo_emprego',      
                                                            'escolaridade',             
                                                            'faixa_etaria',             
                                                            'vinculo_ativo_12',              
                                                            'cnae_2_classe',            
                                                            'sexo_trabalhador',            
                                                            'vl_remun_media_nom',       
                                                            'tipo_vinculo'
                                        ])
            pq.write_table(arquivo,f)                                                                             
            sdf = spark.read.parquet(f)
            df = sdf.withColumn("regiao",lit(regiao))\
                    .withColumn("sexo", when(col('sexo_trabalhador')==1,'M').otherwise('F'))\
                    .withColumn("vl_remun_media_dec",regexp_replace(col('vl_remun_media_nom'),',','.').cast('int'))
            edf = edf.union(df) 
            
edf.groupBy("regiao","sexo")
    .sum(1)
    .avg("vl_remun_media_dec")
    .show(false)

print(df.printSchema())
print(df.show(5))

##Import packages
#print('Imports packages: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

############################ #Loading and Framming ############################

