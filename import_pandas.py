#import pandas as  pd
import pyarrow.parquet as pq
# import pyarrow.dataset as ds
import pyarrow
import os
import glob


dirName = "data"
folders = os.listdir(dirName)
os.chdir(dirName)
for fo in folders:
    if os.path.isdir(fo) and fo != 'summary':
        os.chdir(fo)
        pf = []
        files = glob.glob('*.parquet')
        for f in files:
            # Columns select and filter
            print(f)
            pf.append( pq.read_table(f
                                ,columns=[ 'Qtd Hora Contr'.encode('cp1252')
                                            ,'Idade'.encode('cp1252')
                                            ,'Faixa Tempo Emprego'.encode('cp1252')
                                            ,'Escolaridade após 2005'.encode('cp1252')
                                            ,'Faixa Etária'.encode('cp1252')
                                            ,'Vínculo Ativo 31/12'.encode('cp1252')
                                            ,'CBO Ocupação 2002'.encode('cp1252')
                                            ,'CNAE 2.0 Classe'.encode('cp1252')
                                            ,'Sexo Trabalhador'.encode('cp1252')
                                            ,'Vl Remun Média Nom'.encode('cp1252')
                                            ,'Tipo Vínculo'.encode('cp1252')]
            #                    ,filters=[('CNAE 2.0 Classe', '=', 62091)]
            
                            ))
        arrow_table = pyarrow.concat_tables(pf)
        pq.write_table(arrow_table,'../summary/'+fo+'.parquet')
        os.chdir("../")

#pandas = pat.to_pandas()
#print(pandas.head(25))


