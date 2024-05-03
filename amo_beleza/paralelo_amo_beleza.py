'''
---------------------------------------------------
SCRIPT PYTHON PARA EXTRAÇÃO DE DADOS DA AMO BELEZA.
---------------------------------------------------

Autor: bernardo.teixeira@polijunior.com.br
Data de Criação: 23/04/2024

Input: Não há input. O script é feito para ser rodado diretamente.
Output: Um arquivo .csv com os dados extraídos, salvo em 'amo_beleza_oficial.csv'. 

Metodologia utilizada: Requisição em paralelo.

Problema encontrado: Código funcionando normalmente, mas com problema
de memória durante a execução. RAM sobrecarregada.
'''

import pandas as pd 
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import os
import time
import sys
from types import GeneratorType

def main():
    print("------------------------")
    print("------------------------")
    print("------------------------")
    print("---INÍCIO DA EXTRAÇÃO---")
    print("------------------------")
    print("------------------------")
    print("------------------------")

    MAX_WORKERS = os.cpu_count()
    print('Número de CPUs: ', MAX_WORKERS)

    inicio = time.time()

    # 1. formando urls dos xmls
    urls = ['https://www.amobeleza.com.br/sitemap/product-'+str(p)+'.xml' for p in [0, 1]]
    print(urls)

    # 2. retornando responses das páginas iniciais
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        responses = list(executor.map(get_response, urls))

    print(len(responses))

    urls_individuais = []
    # 3. retornando links individuais
    for response in responses:
        urls_temp = extract_link(response)
        urls_individuais.extend(urls_temp)

    print("Urls individuais únicas:", len(urls_individuais))

    obter_link = time.time()

    responses = []

    # 4. solicitando urls das páginas individuais
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        responses = list(executor.map(get_response, urls_individuais))

    final_df = pd.concat([extract_info(response) for response in responses], ignore_index=True)

    final_df.to_csv('amo_beleza/amo_beleza_oficial.csv', index=False)

    final = time.time()

    print("Tempo para extração dos links:", obter_link - inicio)
    print("Tempo total de extração:", final - inicio)

    print("---------------------")
    print("---------------------")
    print("---------------------")
    print("---FIM DA EXTRAÇÃO---")
    print("---------------------")
    print("---------------------")
    print("---------------------")


def get_response(url, tries=3):
    print(f'Requisição: {url}')
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    for _ in range(tries):
        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            yield response.text
            break
        except requests.HTTPError as e:
            # Trata exceções específicas de HTTP
            print(f"Erro durante a requisição: {e}")
            print(url)
        except Exception as e:
            # Trata exceções genéricas
            print(f"Erro desconhecido durante a requisição: {e}")
            print(url)


def extract_link(text):
    if isinstance(text, GeneratorType):
        text = ''.join(text)  
    urls = []
    soup = BeautifulSoup(text, 'xml')
    locs = soup.find_all('loc')
    for loc in locs:
        urls.append(loc.text)

    print("Urls individuais:", len(urls))

    return urls


def extract_info(response):
    if isinstance(response, GeneratorType):
        response = ''.join(response)
    soup = BeautifulSoup(response, 'lxml')

    farmacia = 'Amo Beleza'
    cidade = 'São Paulo'
    nome = soup.find('h1', class_='vtex-store-components-3-x-productNameContainer vtex-store-components-3-x-productNameContainer--quickview mv0 t-heading-4').text if soup.find('h1', class_='vtex-store-components-3-x-productNameContainer vtex-store-components-3-x-productNameContainer--quickview mv0 t-heading-4') else None
    if not nome:
        return pd.DataFrame()
    span_com_desconto = soup.find('span', class_='vtex-product-price-1-x-sellingPriceValue')
    preco_com_desconto_inteiro = span_com_desconto.find('span', class_='vtex-product-price-1-x-currencyInteger').text if span_com_desconto else None
    preco_com_desconto_decimal = span_com_desconto.find('span', class_='vtex-product-price-1-x-currencyFraction').text if span_com_desconto else None
    preco_com_desconto = preco_com_desconto_inteiro + ',' + preco_com_desconto_decimal if preco_com_desconto_inteiro and preco_com_desconto_decimal else None
    span_sem_desconto = soup.find('span', class_='vtex-product-price-1-x-listPriceValue strike')
    preco_sem_desconto_inteiro = span_sem_desconto.find('span', class_='vtex-product-price-1-x-currencyInteger').text if span_sem_desconto else None
    preco_sem_desconto_decimal = span_sem_desconto.find('span', class_='vtex-product-price-1-x-currencyFraction').text if span_sem_desconto else None
    preco_sem_desconto = preco_sem_desconto_inteiro + ',' + preco_sem_desconto_decimal if preco_sem_desconto_inteiro and preco_sem_desconto_decimal else None
    descricao = soup.find('div', class_='vtex-store-components-3-x-content h-auto').text if soup.find('div', class_='vtex-store-components-3-x-content h-auto') else None
    ean = soup.find('span', class_='vtex-product-identifier-0-x-product-identifier__value').text if soup.find('span', class_='vtex-product-identifier-0-x-product-identifier__value') else None
    marca = soup.find('span', class_='vtex-store-components-3-x-productBrandName').text if soup.find('span', class_='vtex-store-components-3-x-productBrandName') else None

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
    main()
