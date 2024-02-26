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
    headers = {
  'authority': 'www.amobeleza.com.br',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
  'cache-control': 'no-cache',
  'cookie': 'VtexRCMacIdv7=6b7dbe0b-07ef-4478-b566-254e13c1ae6a; _gcl_au=1.1.85700703.1707570024; blueID=05e0c13c-ca79-468d-8a0f-c16deb4cbc37; biggy-anonymous=mC1USvlvNN81BqTvSNyOo; checkout.vtex.com=__ofid=4bfb001f3fdc4a98aaacc770c447b97c; _tt_enable_cookie=1; _ttp=pXhwo_KU3br85ngoCIDCmY7AGcx; voxusmediamanager_acs=true; voxusmediamanager_acs2=true; VtexWorkspace=master%3A-; _clck=16hrc3v%7C2%7Cfjj%7C0%7C1501; vtex_segment=eyJjYW1wYWlnbnMiOm51bGwsImNoYW5uZWwiOiIxIiwicHJpY2VUYWJsZXMiOm51bGwsInJlZ2lvbklkIjpudWxsLCJ1dG1fY2FtcGFpZ24iOm51bGwsInV0bV9zb3VyY2UiOm51bGwsInV0bWlfY2FtcGFpZ24iOm51bGwsImN1cnJlbmN5Q29kZSI6IkJSTCIsImN1cnJlbmN5U3ltYm9sIjoiUiQiLCJjb3VudHJ5Q29kZSI6IkJSQSIsImN1bHR1cmVJbmZvIjoicHQtQlIiLCJjaGFubmVsUHJpdmFjeSI6InB1YmxpYyJ9; _gid=GA1.3.1876526214.1708734816; vtex_binding_address=amobeleza.myvtex.com/; vtex_session=eyJhbGciOiJFUzI1NiIsImtpZCI6IkUzQUU3MTI4MkZFNDFGNDg5RTdFMzZDM0VDRUQ0MUREMDdBRUY2RDQiLCJ0eXAiOiJqd3QifQ.eyJhY2NvdW50LmlkIjoiMmMwZjQ2NzMtYzk2Mi00OTg5LTg1ZTMtZjRkNjVmNjRhMDE1IiwiaWQiOiI4YzY5ZWRhMS03ZmVlLTRlOGQtOWRlZC1kNGFjYjAwN2Y1YWQiLCJ2ZXJzaW9uIjoyLCJzdWIiOiJzZXNzaW9uIiwiYWNjb3VudCI6InNlc3Npb24iLCJleHAiOjE3MDk0ODkxOTMsImlhdCI6MTcwODc5Nzk5MywiaXNzIjoidG9rZW4tZW1pdHRlciIsImp0aSI6ImJkZGI4YmRiLTcyYWQtNDVjNS04ZWNkLTAyMTg0NTIzNzM0OSJ9.rQt0tOhoNc2KjWZGlmMfREZxopUONeQ5JsC24AkKEF9R2laW04OqQ9sA8cZ8NyE5iEzc6PmuVgMqcNcp3M2rCg; VtexRCSessionIdv7=db672a48-4b8f-47e4-a8af-0b22ac868c0c; RKT=false; biggy-session-amobeleza=K1Y2FhACiqC55JFUKhTlZ; _conv_r=s%3Awww.google.com*m%3Aorganic*t%3A*c%3A; _ga=GA1.3.977651751.1707570024; _conv_v=vi%3A1*sc%3A4*cs%3A1708818379*fs%3A1707570025*pv%3A6*exp%3A%7B100452144.%7Bv.1004128376-g.%7B100427612.1-100427614.1%7D%7D%7D*ps%3A1708798005; _conv_s=si%3A4*sh%3A1708818378694-0.06835993983605415*pv%3A2; cto_bundle=WXS8vl9YcHlLcUwxJTJGdExUbEFPRGlMRmtkNzQxM1F6VjQ0aGsyUlo4a2hRQ1kxUUpPJTJGU2ozSWJTaHU4OE8zeTVybTRaZ1Ewd29XYURZY0ZiaWQ4eDRVWVlzJTJCNVFkV05rQWYzQUMlMkZpRHN6RXk3UyUyRkpjTnVQTXloU1JuNzI2MkhENTVVQVFQNWRuY1FxZnNFdCUyQkcyR1NBdDVKa0hraUZxQmNaeUhiNHZtTWZVJTJGcDFGWSUzRA; voxusmediamanager_id=17073118654850.86464783991879785gg2g680r0f; voxusmediamanager__ip=200.170.250.118; _uetsid=5855d990d2ac11ee9646373b281417f0; _uetvid=5a8b24f0c81411eeb2d5edc5ec81cd54; _clsk=1te5uyn%7C1708818487308%7C5%7C1%7Cl.clarity.ms%2Fcollect; _ga_M5SW6R8DNE=GS1.1.1708818377.4.1.1708818489.38.0.0; biggy-event-queue=; janus_sid=2992294c-0b5a-42ce-84da-bb8f5ff14277; janus_sid=46f881bc-101e-4056-8c4d-2f4ad1e39d7c',
  'pragma': 'no-cache',
  'referer': 'https://www.google.com/',
  'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'service-worker-navigation-preload': 'true',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}
    async with httpx.AsyncClient(headers=headers) as client:
        sem = asyncio.Semaphore(1)
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
    urls = ['https://www.amobeleza.com.br/sitemap/product-'+str(p)+'.xml' for p in [0,1]]

    # 2. retornando responses das páginas iniciais
    responses = await get_response(urls)

    urls_individuais = []
    # 3. retornando links individuais
    for response in responses:
        urls_temp = await extract_link(response)
        urls_individuais.extend(urls_temp)

    print("Urls individuais únicas:",len(urls_individuais))

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

    final_df.to_csv('amo_beleza/amo_beleza_oficial.csv', index=False)

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

    farmacia = 'Amo Beleza'
    cidade = 'São Paulo'
    nome = soup.find('h1',class_='vtex-store-components-3-x-productNameContainer vtex-store-components-3-x-productNameContainer--quickview mv0 t-heading-4').text if soup.find('h1',class_='vtex-store-components-3-x-productNameContainer vtex-store-components-3-x-productNameContainer--quickview mv0 t-heading-4') else None
    if not nome: return pd.DataFrame()
    span_com_desconto = soup.find('span',class_='vtex-product-price-1-x-sellingPriceValue')
    preco_com_desconto_inteiro = span_com_desconto.find('span', class_='vtex-product-price-1-x-currencyInteger').text if span_com_desconto else None
    preco_com_desconto_decimal = span_com_desconto.find('span',class_='vtex-product-price-1-x-currencyFraction').text if span_com_desconto else None
    preco_com_desconto = preco_com_desconto_inteiro + ',' + preco_com_desconto_decimal if preco_com_desconto_inteiro and preco_com_desconto_decimal else None
    span_sem_desconto = soup.find('span',class_='vtex-product-price-1-x-listPriceValue strike')
    preco_sem_desconto_inteiro = span_sem_desconto.find('span',class_='vtex-product-price-1-x-currencyInteger').text if span_sem_desconto else None
    preco_sem_desconto_decimal = span_sem_desconto.find('span',class_='vtex-product-price-1-x-currencyFraction').text if span_sem_desconto else None
    preco_sem_desconto = preco_sem_desconto_inteiro + ',' + preco_sem_desconto_decimal if preco_sem_desconto_inteiro and preco_sem_desconto_decimal else None
    descricao = soup.find('div', class_='vtex-store-components-3-x-content h-auto').text if soup.find('div', class_='vtex-store-components-3-x-content h-auto') else None
    ean = soup.find('span',class_='vtex-product-identifier-0-x-product-identifier__value').text if soup.find('span',class_='vtex-product-identifier-0-x-product-identifier__value') else None
    marca = soup.find('span',class_='vtex-store-components-3-x-productBrandName').text if soup.find('span',class_='vtex-store-components-3-x-productBrandName') else None

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
    descricao = descricao.replace('\n', '')
    descricao = ' '.join(descricao.split())

    nova_linha = {
        'farmacia': farmacia,
        'cidade': cidade,
        'nome': nome,
        'preco_com_desconto': preco_com_desconto,
        'preco_sem_desconto': preco_sem_desconto,
        'percentual_de_desconto': desconto,
        'marca': marca,
        'ean': ean,
        'descricao': descricao,
    }
    df = pd.DataFrame(nova_linha, index=[0])
    return df
    
if __name__ == "__main__":
    asyncio.run(main())

