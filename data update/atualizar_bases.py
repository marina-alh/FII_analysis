#from urllib.request import urlopen
#from urllib.error import HTTPError
#from urllib.error import URLError
#from bs4 import BeautifulSoup
#from subprocess import check_output
#from PIL import Image
#import re
#import datetime, time
#import random
#import os, shutil

##################################################################################
### Main
##################################################################################
def main():
    
    print("\n*** Atualização dos arquivos basicos das fontes!")
    # No primeiro uso, baixar também a partir de 1986
#    atualizar_b3_series_historicas(2019) # baixar para o ano atual. A partir de 01/01, baixar do ano anterior também, que pode não estar completo.
    atualizar_b3_series_historicas(2020) # baixar para o ano atual. A partir de 01/01, baixar do ano anterior também, que pode não estar completo.
    # atualizar_b3_titulos_negociaveis() # lista de papéis negociáveis
    # atualizar_quandl_titulos_negociaveis()

    print("\n*** Atualização concluída!")

    return

##################################################################################
### atualizar_b3_series_historicas
##################################################################################
def atualizar_b3_series_historicas(ano):
    
    import zipfile, os
    
    print('Baixando arquivo de cotações históricas...')

    path = 'fontes/b3/series_historicas/'
    os.makedirs(path, exist_ok=True)
    url = 'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A'+str(ano)+'.ZIP'
    downdload_com_status(url,path+'COTAHIST_A'+str(ano)+'.ZIP');
        
    print('Descompactando arquivo de cotações históricas...')
    with zipfile.ZipFile(path+'COTAHIST_A'+str(ano)+'.ZIP','r') as zip_ref:
        zip_ref.extractall(path)    

    return

##################################################################################
### atualizar_b3_titulos_negociaveis
##################################################################################
def atualizar_b3_titulos_negociaveis():
    
    import zipfile, os
    
    print('Baixando arquivo de titulos negociaveis...')

    path = 'fontes/b3/titulos_negociaveis/'
    os.makedirs(path, exist_ok=True)
    url = 'http://bvmf.bmfbovespa.com.br/suplemento/ExecutaAcaoDownload.asp?server=L&arquivo=Titulos_Negociaveis.zip'
    downdload_com_status(url,path+'Titulos_Negociaveis.zip');

    print('Baixando documento que descreve a formatação do arquivo de titulos negociaveis...')
    url = 'http://bvmf.bmfbovespa.com.br/suplemento/doc/Titulos_Negociaveis.PDF'
    downdload_com_status(url,path+'Titulos_Negociaveis.pdf');
        
    print('Descompactando arquivo de titulos negociaveis...')
    with zipfile.ZipFile(path+'Titulos_Negociaveis.zip','r') as zip_ref:
        zip_ref.extractall(path)    

    return

##################################################################################
### atualizar_quandl_titulos_negociaveis
##################################################################################
def atualizar_quandl_titulos_negociaveis():
    
    import zipfile, os
    
    print('Baixando arquivo de titulos negociaveis da base Quandl...')

    path = 'fontes/quandl/titulos_negociaveis/'
    os.makedirs(path, exist_ok=True)
    url = 'https://s3.amazonaws.com/quandl-production-static/end_of_day_us_stocks/ticker_list.csv'
    downdload_com_status(url,path+'ticker_list.csv');

    return
        
def downdload_com_status(url,filename):
    
    import requests
    from tqdm import tqdm
    
    r = requests.get(url, stream=True)
    block_size = 1024
    wrote = 0 
    with open(filename, 'wb') as f:
        for data in tqdm(r.iter_content(block_size), desc='   Download status', unit=' KB', unit_scale=True):
            wrote = wrote  + len(data)
            f.write(data)
        f.close()

    return

##################################################################################
### Chamada de main()
##################################################################################
if __name__== "__main__":
    main()