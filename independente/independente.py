'''
------------------------------------------------------------
SCRIPT PYTHON PARA EXTRAÇÃO DE DADOS DA FARMÁCIA PERMANENTE.
------------------------------------------------------------

Autor: bernardo.teixeira@polijunior.com.br
Data de Criação: 09/02/2024

Input: Não há input. O script é feito para ser rodado diretamente.
Output: Um arquivo .csv com os dados extraídos, salvo em 
'farmacia_permanente_oficial.csv'. 

Metodologia utilizada: A extração foi realizada por meio de duas únicas APIs.
Primeiramente, se extraiu o SKU de todos os produtos de todos os departamentos
a partir da API "https://farmaciapermanente.com.br/pesquisa/pesquisar",
modificando apenas o setor e a página do payload. A partir da lista de SKU
extraído, foi possível acessar a API "https://farmaciapermanente.com.br/pegar/preco",
onde foram obtidas as informações de preço e desconto de cada produto.
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

def get_apis(setor,pagina):
    apis = []
    url = "https://farmaciapermanente.com.br/pesquisa/pesquisar"
    payload = "csrfmiddlewaretoken=6UNJFxLfg83YjJMBGQ1ra0TJKnUWD9kFsL0X6LoxXPxna2vPomyCt1v9H6yXpSE2&termo=categorias%2F"+str(setor)+"&pagina="+str(pagina)+"&is_termo=true"
    headers = {
    'authority': 'farmaciapermanente.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=epYHUxaNAUqafH020Q41KuRnwCEv5G8ZAgbVlLN5hBUz60JgImBc3vtNtliwRpsm; _ga=GA1.1.168733410.1707512871; sessionid=qvd90aa4skqq679rbjw5an1dwmhq4cfi; _ga_WDHRYMGQCJ=GS1.1.1707512870.1.1.1707513021.0.0.0; sessionid=qvd90aa4skqq679rbjw5an1dwmhq4cfi',
    'origin': 'https://farmaciapermanente.com.br',
    'pragma': 'no-cache',
    'referer': 'https://farmaciapermanente.com.br/categorias/medicamentos',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    # Verifica se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        json_data = response.text
        json_data = json.loads(json_data)
        apis.append(json_data)  
    else :
        print(response.status_code)
    return apis

def get_details(payload):
    apis = []
    url = "https://farmaciapermanente.com.br/pegar/preco"
    payload = payload
    headers = {
    'authority': 'farmaciapermanente.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=epYHUxaNAUqafH020Q41KuRnwCEv5G8ZAgbVlLN5hBUz60JgImBc3vtNtliwRpsm; _ga=GA1.1.168733410.1707512871; sessionid=qvd90aa4skqq679rbjw5an1dwmhq4cfi; _ga_WDHRYMGQCJ=GS1.1.1707512870.1.1.1707513057.0.0.0; sessionid=qvd90aa4skqq679rbjw5an1dwmhq4cfi',
    'origin': 'https://farmaciapermanente.com.br',
    'pragma': 'no-cache',
    'referer': 'https://farmaciapermanente.com.br/categorias/medicamentos',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    # Verifica se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        json_data = response.text
        json_data = json.loads(json_data)
        apis.append(json_data)  
    else :
        print(response.status_code)
    return apis

# Nome e quantidade de página dos departamentos:
departamentos = ['medicamentos','saude','beleza','vitaminas-e-suplementos','dermocosmeticos','mamaes-e-bebes','cuidados-diarios','conveniencia']
paginas = [69,11,19,5,11,16,17,13]

def main():
    print("------------------------")
    print("------------------------")
    print("------------------------")
    print("---INÍCIO DA EXTRAÇÃO---")
    print("------------------------")
    print("------------------------")
    print("------------------------")

    apis = []
    # Iterando por departamentos e formando URLS:
    for departamento, pagina in zip(departamentos, paginas):
        print("Passando pelo departamento: ", departamento,"...")
        for i in range(pagina+1):
            api = get_apis(departamento, i)
            apis.append(api)
    
    print("Processando os valores de SKU...")
    dfs = []
    for api in apis:
        df = extract_api(api)
        dfs.append(df)
    df_final = pd.concat(dfs, ignore_index=True)

    sku_list = df_final['SKU'].tolist()

    def criar_payload(produtos_ids):
        csrf_token = "12zvwbskR5BTQG4jGVJiusav5WzFdIvgnTMJXp5CyM5iHZNxorgtNtMV2FdGZrPD"
        uf = "SP"
        payload = f"csrfmiddlewaretoken={csrf_token}&"
        
        for sku in produtos_ids:
            payload += f"produtos_ids%5B%5D={sku}&"
        
        payload += f"uf={uf}"
        
        return payload

    print("Realizando a extração a partir dos SKU...")
    payload = criar_payload(sku_list)
    detalhes = get_details(payload)
    
    print("Realizando extração final dos detalhes dos produtos...")
    df_detalhes = extract_detalhes(detalhes)
    df_final = df_final.merge(df_detalhes, on="SKU", how="left")
    df_final.to_csv('farmacia_permanente_oficial.csv', index=False)

    print("---------------------")
    print("---------------------")
    print("---------------------")
    print("---FIM DA EXTRAÇÃO---")
    print("---------------------")
    print("---------------------")
    print("---------------------")

def extract_api(api):
    api = api[0]
    dfs = []
    produtos = api['produtos']
    if produtos:
        for produto in produtos:
            if produto["_id"]:
                sku = produto["_id"]
            else: sku = None
            if produto['_source']['marca']:
                marca = produto['_source']['marca']
            else: marca = None
            if produto['_source']['nm_produto']:
                nome = produto['_source']['nm_produto']
            else: nome = None
            if produto['_source']['categoria']:
                categoria = produto['_source']['categoria']
            else: categoria = None
            

            nova_linha = {"SKU": sku, "Marca": marca, "Nome": nome, "Categoria": categoria}
            df_temp = pd.DataFrame([nova_linha])  # Criando DataFrame temporário
            dfs.append(df_temp)

    # Concatenando todos os DataFrames temporários
    df_final = pd.concat(dfs, ignore_index=True)
    
    return df_final

def extract_detalhes(detalhes):
    detalhes_list = []
    precos = detalhes[0].get("precos", {})
    for sku, preco in precos.items():
        if preco.get("publico"):
            produto_id = preco["publico"].get("produto_id")
            valor_ini = preco["publico"].get("valor_ini")
            valor_fim = preco["publico"].get("valor_fim")
            perc_desc = preco["publico"].get("per_desc")
            tipo_desc = preco["publico"].get("tipo_desc")
            detalhes_list.append({
                "SKU": sku,
                "produto_id": produto_id,
                "valor_ini": valor_ini,
                "valor_fim": valor_fim,
                "perc_desc": perc_desc,
                "tipo_desc": tipo_desc
            })
    return pd.DataFrame(detalhes_list)

if __name__ == "__main__":
    main()

