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
import pyarrow.parquet  
import glob 

print('Job start: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


## Start Spark Session
#spark = SparkSession.builder.appName("ProcessamentoA3Challenge") \
#        .getOrCreate()
        # .master("local[1]")
       

## Loading Parquets 
print('Imports parquets: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

files = glob.glob('2010/*.parquet')

# df = 0

# for f in files:
#     if df == 0:
#         df = spark.read.parquet(f)
#     else:
#         df.write.mode('append').parquet(f)
        

print(pyarrow.parquet.read_metadata('2010/PI2010.parquet'))
print(pyarrow.parquet.ParquetFile.schema('2010/PI2010.parquet'))


#df = spark.read.parquet('2010/PI2010.parquet')
#df.printSchema()
#df.empty()

print('Imports Ok: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))



#spark.close()