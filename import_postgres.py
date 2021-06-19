
#import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


dirSummary = "data/summary"

#conf = SparkConf()  # create the configuration
#conf.set("spark.jars", "Z:/Documentos/projetos_python/a3datachallenge/code/jdbc/postgresql-42.2.22.jar")

spark = SparkSession.builder.appName('A3Challenge').getOrCreate ()
    #.config("spark.jars","Z:/Documentos/projetos_python/a3datachallenge/code/jdbc/postgresql-42.2.22.jar")\
    
print("!------Creating DataFrame")
df = spark.read.parquet("data/summary/2010.parquet")
            #.load(dirSummary)\
           #.write.format("postgres")\
           # .option("host","localhost")\
           # .option("partitions", 4) \
           # .option("table","rais")\
           # .option("user","postgres")\
            #.option("database","postgres")\
            #.option("schema","public")\
            #.load()

print("Total row: ",df.count())
print(df.crosstab("vl_remun","sexo"))

#print("!------Database Writing")




#df2 = df.withColumnRenamed('Qtd Hora Contr',"qtd_hora")\
#    .withColumnRenamed('Faixa Tempo Emprego',"faixa_emprego")\
#    .withColumnRenamed('Escolaridade após 2005',"escolaridade")\
#    .withColumnRenamed('Faixa Etária',"faixa_etaria")\
    #.withColumnRenamed('Vínculo Ativo 31/12',"vinculo_ativo")\
    #.withColumnRenamed('CNAE 2.0 Classe',"cnae")\
    #.withColumnRenamed('Sexo Trabalhador',"sexo")\
    #.withColumnRenamed('Vl Remun Média Nom',"vl_remun")\
    #.withColumnRenamed('Tipo Vínculo',"tipo_vinculo")

#df2.printSchema()
#df2.count()

#mode = "overwrite"
#url = "jdbc:postgresql://localhost:5432/postgres"
#properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
#df2.write.jdbc(url=url, table="rais_dados", mode=mode, properties=properties)