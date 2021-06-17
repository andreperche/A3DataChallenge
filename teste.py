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

# from pyspark.conf import SparkConf
# from pyspark.context import SparkContext
# ##Set Spark parameters
# """ !?WTF?! Once the physical memory is exhausted, swap will be used instead. 
#             If you have enough swap configured, you will be able to make use of 16 GB. 
#             But your process will most likely slow down to a crawl when it starts swapping.
# """
# conf = SparkConf().setMaster("local").setAppName("My application").set("spark.executor.memory", "16g")
# sc = SparkContext(conf)


##Start Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ProcessamentoA3Challenge") \
        .getOrCreate()
        # .master("local[1]")
       

##Import packages
print('Imports packages: '+'{}'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

############################ #Loading and Framming ############################

df = spark.read.options(header='True',inferSchema='True',delimiter=';').format("csv").load(r"data/2019/RAIS_VINC_PUB_CENTRO_OESTE.txt")
df.printSchema()