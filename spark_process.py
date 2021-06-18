# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 19:52:26 2021

@author: Perche
"""

## Install Dependecies
#pip install pyspark
#pip install pandas

#Import pacjages 
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import column
from pyarrow.parquet import ParquetFile
import pyspark as ps
from pyspark.sql.types import StructType,StructField, StringType, ShortType
from pyspark.sql import SparkSession

##Start Spark Session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


print('Job start: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


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
              StructField('regiao',                   StringType(),True),
              StructField('vl_remun_media_nom',       StringType(),True),
              StructField('tipo_vinculo',             ShortType(),True)   
          ])

sdf = spark.createDataFrame(empty,schema)
sdf2 = spark.read.parquet('data/summary/*.parquet')

print(sdf2)



#spark.close()