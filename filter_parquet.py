#import pandas as  pd
import pyarrow.parquet as pq
# import pyarrow.dataset as ds
import pyarrow
import os
import glob


dirName = "data"
dirSummary = "summary"

#Creating folder to storage all data
try:
    os.mkdir(dirName+"/"+dirSummary)
    print("!---Directory ", dirSummary, " created.")
except FileExistsError:
    print("!---Directory ", dirSummary, " already exists!")  

folders = os.listdir(dirName)
os.chdir(dirName)
for fo in folders:
    if os.path.isdir(fo) and fo != 'summary':
        os.chdir(fo)
        print("+---Year: ",fo)
        pf = []
        files = glob.glob('*.parquet')        
        for f in files:
            # Columns select and filter            
            if(f.upper()[:4] != "ESTB" and f.upper()[5:10] != "ESTAB"): 
                print("|---File: ", f)                       
                table = pq.read_table(f
                            ,columns=[ 'Qtd Hora Contr'
                                                ,'Idade'
                                                ,'Faixa Tempo Emprego'
                                                ,'Escolaridade após 2005'
                                                ,'Faixa Etária'
                                                ,'Vínculo Ativo 31/12'                                                
                                                ,'CNAE 2.0 Classe'
                                                ,'Sexo Trabalhador'
                                                ,'Vl Remun Média Nom'
                                                ,'Tipo Vínculo'
                                            ]                                
                                ) 
                new_table = table.rename_columns(['qtd_hora','idade','faixa_tempo','escolaridade','faixa_etaria','vinculo','cnae','sexo','vl_remun','tipo_vinculo'])
                pf.append(new_table)
                
        arrow_table = pyarrow.concat_tables(pf)
        pq.write_table(arrow_table,'../summary/'+fo+'.parquet')
        os.chdir("../")

# All years in one parquet file
'''
os.chdir(dirSummary)   
pf = []
files = glob.glob('*.parquet')        
for f in files:
    if(f != "summary.parquet"):

        table = pq.read_table(f
                            ,columns=[ 'Qtd Hora Contr'
                                                ,'Idade'
                                                ,'Faixa Tempo Emprego'
                                                ,'Escolaridade após 2005'
                                                ,'Faixa Etária'
                                                ,'Vínculo Ativo 31/12'                                                
                                                ,'CNAE 2.0 Classe'
                                                ,'Sexo Trabalhador'
                                                ,'Vl Remun Média Nom'
                                                ,'Tipo Vínculo'
                                            ]                                
                                ) 
        new_table = table.rename_columns(['qtd_hora','idade','faixa_tempo','escolaridade','faixa_etaria','vinculo','cnae','sexo','vl_remun','tipo_vinculo'])
        pf.append(new_table)
arrow_table = pyarrow.concat_tables(pf)
pq.write_table(arrow_table,'../summary/summary.parquet')         
'''
#pandas = pat.to_pandas()
#print(pandas.head(25))


