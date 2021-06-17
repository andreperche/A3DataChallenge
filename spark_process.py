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

print('Job start: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


## Start Spark Session
spark = SparkSession.builder.appName("ProcessamentoA3Challenge") \
        .getOrCreate()
        # .master("local[1]")
       

## Loading Parquets 
print('Imports parquets: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
df = spark.read.parquet("2010/")
df.printSchema()

print('Imports Ok: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


