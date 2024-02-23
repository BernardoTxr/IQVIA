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

async def get_response(urls, tries = 10):
    responses = []
    async with httpx.AsyncClient() as client:
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

    setores = ['medicamentos','infantil','dermocosmeticos','saude','beleza','higiene-pessoal','pet-shop','nutricao-saudavel','mercado','maquiagem','cabelo']
    paginas = [152,51,27,58,40,44,19,23,78,13,48]

    setores = ['medicamentos']
    paginas = [1]

    # 1. formando urls das páginas iniciais
    urls = []
    for setores, n_páginas in zip(setores, paginas):
        base_url = "https://www.araujo.com.br/" + str(setores) + "?start="
        urls_temp = [base_url + str((p-1)*50) + "&sz=50&page=" + str(p) for p in range(1, n_páginas + 1)]
        urls.extend(urls_temp)

    # 2. retornando responses das páginas iniciais
    responses = await get_response(urls)

    urls_individuais = []
    # 3. retornando links individuais
    for response in responses:
        urls_temp = await extract_link(response)
        urls_individuais.extend(urls_temp)

    print(urls_individuais)

    obter_link = time.time()

    # 4. solicitando urls das páginas individuais
    responses = await get_response(urls_individuais)

    # 5. extraindo informações das urls das páginas individuais
    i=1
    for response in responses:
        df_temp = await extract_info(response)
        final_df = pd.concat([final_df, df_temp])
        print("Extraindo informações",i,"de",len(responses))
        i+=1

    final_df.to_csv('oncolog/oncolog_oficial.csv', index=False)

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
        soup = BeautifulSoup(text, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if script.get('type') == 'application/ld+json' and script.string:
                script_content = script.string.strip()
                if script_content.startswith('{"@context":"http://schema.org/","@type":"ItemList","itemListElement"'):
                    # Analisa o JSON encontrado
                    try:
                        data = json.loads(script_content)
                        urls = [item['url'] for item in data.get('itemListElement', [])]
                        return urls
                    except json.JSONDecodeError as e:
                        return None

async def extract_info(response):
    soup = BeautifulSoup(response, 'lxml')

    farmacia = 'Oncolog Medicamentos'
    cidade = 'São Paulo'
    nome = soup.find('h1', class_='nomeProduto').text if soup.find('h1', class_='nomeProduto') else None
    sku = soup.find('h6', class_='codProduto').text if soup.find('h6', class_='codProduto') else None
    sku = sku.replace('SKU: ', '') if sku else None
    preco_com_desconto = soup.find('span', id='preco').text if soup.find('span', id='preco') else None
    preco_sem_desconto = soup.find('span', id='preco-antigo').text if soup.find('span', id='preco-antigo') else None
    descricao = soup.find('div', class_='ui grid one column').text if soup.find('div', class_='column') else None
    descricao = re.sub(r'\n', '', descricao) if descricao else None
    # Cálculo do percentual de desconto:
    if preco_com_desconto and preco_sem_desconto:
        preco_com_desconto_str = preco_com_desconto.replace('R$', '').replace('.', '').replace(',', '.').strip()
        preco_sem_desconto_str = preco_sem_desconto.replace('R$', '').replace('.', '').replace(',', '.').strip()
        
        # Certifique-se de tratar casos em que a string fica vazia após a remoção dos caracteres
        preco_com_desconto = float(preco_com_desconto_str) if preco_com_desconto_str else None
        preco_sem_desconto = float(preco_sem_desconto_str) if preco_sem_desconto_str else None
        
        # Cálculo do desconto apenas se ambos os preços forem válidos
        if preco_com_desconto and preco_sem_desconto:
            desconto = 100 - (preco_com_desconto / preco_sem_desconto * 100)
        else:
            desconto = None
    else:
        desconto = None

    nova_linha = {
        'farmacia': farmacia,
        'cidade': cidade,
        'nome': nome,
        'sku': sku,
        'preco_com_desconto': preco_com_desconto,
        'preco_sem_desconto': preco_sem_desconto,
        'desconto': desconto,
        'descricao': descricao
    }
    df = pd.DataFrame(nova_linha, index=[0])
    return df


    
if __name__ == "__main__":
    asyncio.run(main())

