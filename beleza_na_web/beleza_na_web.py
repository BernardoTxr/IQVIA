'''
-------------------------------------------------
SCRIPT PYTHON PARA EXTRAÇÃO DE DADOS DA DROGASIL.
-------------------------------------------------

Autor: bernardo.teixeira@polijunior.com.br
Data de Criação: 09/02/2024

Input: Não há input. O script é feito para ser rodado diretamente.
Output: Um arquivo .csv com os dados extraídos, salvo em 'drogasil_oficial.csv'. 

Metodologia utilizada: No site, existiam algumas informações que poderiam ser
extraídas pela API e outras que permitiam a extração por HTML. Por isso, o script
foi dividido em duas partes: uma para a extração por HTML e outra para a extração
por API. A extração por HTML foi feita de forma assíncrona, para otimizar o tempo
de execução. A extração por API foi feita de forma síncrona, pois a API não permitia
a extração de forma assíncrona. No fim, as informações extraídas por HTML e API
são unidas em um único dataframe.

Url do html: https://www.drogasil.com.br/saude.html?p=1 onde 'saude' é um dos departamentos
da drogasil. A partir dessa url, é possível acessar todas as páginas de produtos de um departamento.

Url da api: https://www.drogasil.com.br/api/next/middlewareGraphql

Payload da api: o payload é utilizado para obter as informações dos produtos
a partir de uma lista de SKUs. Essa lista foi determinada a partir dos produtos extraídos
pelo HTML. 
'''

import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import httpx
import aiometer
from functools import partial
import time
from bs4 import BeautifulSoup

async def get_response(urls, tries = 3):
    responses = []
    async with httpx.AsyncClient(headers = {
  'Upgrade-Insecure-Requests': '1',
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"'
},timeout=30) as client:
        sem = asyncio.Semaphore(3)
        async def requisition(url):
            async with sem:
                for i in range(tries):
                    try:
                        response = await client.get(url)
                        response.raise_for_status()
                        responses.append(response.text)
                        break
                    except httpx.HTTPError as e:
                        # Trata exceções específicas de HTTP
                        print(f"Erro durante a requisição: {e}")
                        print(url)
                    except Exception as e:
                        # Trata exceções genéricas
                        print(f"Erro desconhecido durante a requisição: {e}")
                        print(url)
        await asyncio.gather(*[requisition(url) for url in urls])
    return responses

async def main():
    print("------------------------")
    print("------------------------")
    print("------------------------")
    print("---INÍCIO DA EXTRAÇÃO---")
    print("------------------------")
    print("------------------------")
    print("------------------------")

    inicio = time.time()

    # 1. formando urls dos xmls
    urls = ['https://data-exporter.belezanaweb.com.br/sitemap/1/produto'+str(p)+'.xml' for p in range(1,3)]

    # 2. retornando responses das páginas iniciais
    responses = await get_response(urls)

    urls_individuais = []
    # 3. retornando links individuais
    for response in responses:
        urls_temp = await extract_link(response)
        urls_individuais.extend(urls_temp)

    obter_link = time.time()

    # 4. solicitando urls das páginas individuais
    responses = await get_response(urls_individuais)

    final_df = pd.DataFrame()
    # 5. extraindo informações das urls das páginas individuais
    i=1
    for response in responses:
        df_temp = await extract_info(response)
        final_df = pd.concat([final_df, df_temp])
        print("Extraindo informações",i,"de",len(responses))
        i+=1

    final_df.to_csv('beleza_na_web/beleza_na_web_oficial.csv', index=False)

    final = time.time()

    print("Tempo para extração dos links:",obter_link-inicio)
    print("Tempo total de extração:",final-inicio)

    print("---------------------")
    print("---------------------")
    print("---------------------")
    print("---FIM DA EXTRAÇÃO---")
    print("---------------------")
    print("---------------------")
    print("---------------------")

async def extract_link(text):
    urls = []
    soup = BeautifulSoup(text, 'xml')
    locs = soup.find_all('loc')
    for loc in locs:
        urls.append(loc.text)

    return urls

async def extract_info(response):
    soup = BeautifulSoup(response, 'lxml')

    farmacia = 'Beleza na Web'
    cidade = 'São Paulo'
    nome = soup.find('h1').text if soup.find('h1') else 'Sem nome disponível.'
    preco_com_desconto = soup.find('div', class_='nproduct-price-value nproduct-price-value-redesign').text if soup.find('div', class_='nproduct-price-value nproduct-price-value-redesign') else None
    s_tag = soup.find('strong', class_='nproduct-price-max nproduct-price-max-redesign')
    if s_tag:
        preco_sem_desconto = s_tag.find('s').text if s_tag.find('s') else None
    else:
        preco_sem_desconto = None
    descricao = soup.find('div', class_='accordion-is-hidden').text if soup.find('div', class_='accordion-is-hidden') else 'Sem descrição disponível.'
    sku = soup.find('div',class_='product-sku').text if soup.find('div',class_='product-sku') else None

    # Ache o script type application ld json:
    script = soup.find('script', type='application/ld+json')
    script_dict = json.loads(script.string)
    script_dict = script_dict[0]
    marca = script_dict['brand']['name']

    # Transforma precos de RS 1.750,00 em float:
    preco_com_desconto = float(preco_com_desconto.replace('R$', '').replace('.', '').replace(',', '.')) if preco_com_desconto else None
    preco_sem_desconto = float(preco_sem_desconto.replace('R$', '').replace('.', '').replace(',', '.')) if preco_sem_desconto else None

    # Cálculo do desconto:
    if preco_sem_desconto and preco_com_desconto:
        desconto = (preco_sem_desconto - preco_com_desconto) / preco_sem_desconto
    else:
        desconto = None

    # Tirar espaços e quebras de linha:
    nome = nome.replace('\n', '')
    nome = ' '.join(nome.split())
    sku = sku.strip()
    descricao = descricao.replace('\n', '')

    nova_linha = {
        'farmacia': farmacia,
        'cidade': cidade,
        'nome': nome,
        'preco_com_desconto': preco_com_desconto,
        'preco_sem_desconto': preco_sem_desconto,
        'percentual_de_desconto': desconto,
        'marca': marca,
        'sku': sku,
        'descricao': descricao,
    }
    df = pd.DataFrame(nova_linha, index=[0])
    return df
    
if __name__ == "__main__":
    asyncio.run(main())

