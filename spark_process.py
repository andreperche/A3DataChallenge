# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 19:52:26 2021

@author: Perche
"""

## Install Dependecies
#pip install pyspark
#pip install pandas

from datetime import datetime
print('Job start: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

##################################  #SETUP ####################################

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
##Set Spark parameters
""" !?WTF?! Once the physical memory is exhausted, swap will be used instead. 
            If you have enough swap configured, you will be able to make use of 16 GB. 
            But your process will most likely slow down to a crawl when it starts swapping.
"""
conf = SparkConf().setMaster("local").setAppName("My application").set("spark.executor.memory", "16g")
sc = SparkContext(conf)


##Start Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


##Import packages
print('Imports packages: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import upper
import pandas as pd
import glob

############################ #Loading and Framming ############################

#Path and file type definitions
path = r'C:\Users\Rafa\Documents\GitHub\A3DataChallenge\data\2019'
all_files = glob.glob(path + '/*.txt')
li = []

##Read all files in folder using pandas 
for filename in all_files:
    print('Start reading: '+filename)
    df = pd.read_csv(filename, index_col=None, header=0)
    print('file reading ok: ' +'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
#append files a list
    li.append(df)

#Concat all datasets into a single one
p_df = pd.concat(li, axis=0, ignore_index=True)
print('Concat ok: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")) )

#Convert Pandas dataFrame into SparkDataframe
df = spark.createDataFrame(p_df)
print('dataframe ok: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")) )

############################ #Process and Cleaning ############################
##Clean Headers names
#df.columns.foldLeft(df){(newdf, colname) => newdf.withColumnRenamed(colname, colname.replace(" ", "_").replace(".", "_"))}.show

##Replace special characters from data
#df['column_name'].str.encode('ascii', 'ignore').str.decode('ascii')

#df.show(5)
df.printSchema()


########################### #Storing Data (Parquet) ###########################


#clear dataFrame before write on parquet
# dfclear

print('Job finished: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


