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
    url = "https://www.farmaciarosario.com.br/pesquisa/pesquisar"
    payload = "csrfmiddlewaretoken=9e2R7CYNpO8t98YsZQz6j1ZG3gnxoyogcsyxmvPzmCfytUhGmonMSP1b1KSLBSin&termo=categorias%2F"+str(setor)+"&pagina="+str(pagina)+"&ordenacao=&is_termo=true"
    headers = {
    'authority': 'www.farmaciarosario.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=SbzNFPhTtC1KKjPa4hSef8iO49QsrUGsVp5tUI8Fqq8P458orPGUOWkj2DlGEeAz; sessionid=n20wv0roezsv3uju82z5e1j6s622ktnv; _ga=GA1.1.32208427.1707570278; _gcl_au=1.1.737058181.1707570279; _hjSession_3567234=eyJpZCI6Ijg4ZGIzOTRiLTJkMzYtNDE2ZS1iMjgzLTdjMjFhYzQ1ZjY3NCIsImMiOjE3MDc1NzAyNzg3ODUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=; blueID=e6f3ca7d-a989-488f-8135-0923a9866f32; _pin_unauth=dWlkPU56Y3hNamd4WWpZdFlqWmxNUzAwTVRRMExUa3pPV1F0TVRjM056SmtaVEV6TVRjeA; _clck=1l56q05%7C2%7Cfj5%7C0%7C1501; _hjSessionUser_3567234=eyJpZCI6IjczOTA2M2E3LWMxMTAtNWJhYS1hMjg5LTBiNjE5NTE4OThmNyIsImNyZWF0ZWQiOjE3MDc1NzAyNzg3ODQsImV4aXN0aW5nIjp0cnVlfQ==; __csfpsid_125279771=bjIwd3Ywcm9lenN2M3VqdTgyejVlMWo2czYyMmt0bnYqU3VuLCAxMSBGZWIgMjAyNCAxMzowNDo0OCBHTVQ=; xe_config=ODFTNVRBUTA5MCxDMjM3RDc5RC03RjgyLTQ4NUUtOTk3MC1DQkZBRTYzNjFDMjQsZmFybWFjaWFyb3NhcmlvLmNvbS5icg==; xe_visitor=eyJpZCI6ImVkMjU3M2ExLWFhZjEtNGM5YS05NDM3LTM1YmU2ZWJiNzEzZCIsImVtYWlsIjoiIn0=; __csfpsid_758495428=bjIwd3Ywcm9lenN2M3VqdTgyejVlMWo2czYyMmt0bnYqU3VuLCAxMSBGZWIgMjAyNCAxMzowNTowNCBHTVQ=; _uetsid=f249dbf0c81411eebab48b46473b3093; _uetvid=f24a0920c81411eea98cefef91b0a30b; __udf_j=7641890b6eb042c94cf046fc6f8f8e90e61d4ebe64435a9e7dabce856bc856c61044369a8b597759697291a35c25804b; _clsk=rdpigs%7C1707572441861%7C6%7C1%7Cl.clarity.ms%2Fcollect; _ga_09RPC7SELW=GS1.1.1707570278.1.1.1707572521.60.0.0; sessionid=n20wv0roezsv3uju82z5e1j6s622ktnv',
    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI4OTk0MzciLCJhcCI6IjE4MzUwMjc3NzkiLCJpZCI6ImMyZGVhNjdkN2E0ZDVhOGEiLCJ0ciI6IjkzZjc3MmEzNjRmNWFkYjliMjgyM2ZlNDRjMDZiMGNmIiwidGkiOjE3MDc1NzI1MjIyNzd9fQ==',
    'origin': 'https://www.farmaciarosario.com.br',
    'pragma': 'no-cache',
    'referer': 'https://www.farmaciarosario.com.br/categorias/medicamentos',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'traceparent': '00-93f772a364f5adb9b2823fe44c06b0cf-c2dea67d7a4d5a8a-01',
    'tracestate': '2899437@nr=0-1-2899437-1835027779-c2dea67d7a4d5a8a----1707572522277',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'x-newrelic-id': 'Vg4OWFJQDxABU1hbAgEFX1ED',
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
    url = "https://www.farmaciarosario.com.br/pegar/preco"
    payload = payload
    headers = {
  'authority': 'www.farmaciarosario.com.br',
  'accept': '*/*',
  'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
  'cache-control': 'no-cache',
  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
  'cookie': 'csrftoken=SbzNFPhTtC1KKjPa4hSef8iO49QsrUGsVp5tUI8Fqq8P458orPGUOWkj2DlGEeAz; sessionid=n20wv0roezsv3uju82z5e1j6s622ktnv; _ga=GA1.1.32208427.1707570278; _gcl_au=1.1.737058181.1707570279; _hjSession_3567234=eyJpZCI6Ijg4ZGIzOTRiLTJkMzYtNDE2ZS1iMjgzLTdjMjFhYzQ1ZjY3NCIsImMiOjE3MDc1NzAyNzg3ODUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=; blueID=e6f3ca7d-a989-488f-8135-0923a9866f32; _pin_unauth=dWlkPU56Y3hNamd4WWpZdFlqWmxNUzAwTVRRMExUa3pPV1F0TVRjM056SmtaVEV6TVRjeA; _clck=1l56q05%7C2%7Cfj5%7C0%7C1501; _hjSessionUser_3567234=eyJpZCI6IjczOTA2M2E3LWMxMTAtNWJhYS1hMjg5LTBiNjE5NTE4OThmNyIsImNyZWF0ZWQiOjE3MDc1NzAyNzg3ODQsImV4aXN0aW5nIjp0cnVlfQ==; __csfpsid_125279771=bjIwd3Ywcm9lenN2M3VqdTgyejVlMWo2czYyMmt0bnYqU3VuLCAxMSBGZWIgMjAyNCAxMzowNDo0OCBHTVQ=; xe_config=ODFTNVRBUTA5MCxDMjM3RDc5RC03RjgyLTQ4NUUtOTk3MC1DQkZBRTYzNjFDMjQsZmFybWFjaWFyb3NhcmlvLmNvbS5icg==; xe_visitor=eyJpZCI6ImVkMjU3M2ExLWFhZjEtNGM5YS05NDM3LTM1YmU2ZWJiNzEzZCIsImVtYWlsIjoiIn0=; __csfpsid_758495428=bjIwd3Ywcm9lenN2M3VqdTgyejVlMWo2czYyMmt0bnYqU3VuLCAxMSBGZWIgMjAyNCAxMzowNTowNCBHTVQ=; __udf_j=7641890b6eb042c94cf046fc6f8f8e90e61d4ebe64435a9e7dabce856bc856c61044369a8b597759697291a35c25804b; _clsk=rdpigs%7C1707572441861%7C6%7C1%7Cl.clarity.ms%2Fcollect; _uetsid=f249dbf0c81411eebab48b46473b3093; _uetvid=f24a0920c81411eea98cefef91b0a30b; _ga_09RPC7SELW=GS1.1.1707570278.1.1.1707572523.58.0.0; sessionid=n20wv0roezsv3uju82z5e1j6s622ktnv',
  'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI4OTk0MzciLCJhcCI6IjE4MzUwMjc3NzkiLCJpZCI6IjEyYTIwZjlkZjk0ZDUxYzUiLCJ0ciI6Ijg2MGE4ZjM1ZWVhM2Q5ODBiMzVkYmM2YjE0ZTU1NTkyIiwidGkiOjE3MDc1NzI1MjM5NTl9fQ==',
  'origin': 'https://www.farmaciarosario.com.br',
  'pragma': 'no-cache',
  'referer': 'https://www.farmaciarosario.com.br/categorias/medicamentos',
  'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'traceparent': '00-860a8f35eea3d980b35dbc6b14e55592-12a20f9df94d51c5-01',
  'tracestate': '2899437@nr=0-1-2899437-1835027779-12a20f9df94d51c5----1707572523959',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'x-newrelic-id': 'Vg4OWFJQDxABU1hbAgEFX1ED',
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
departamentos = ['medicamentos','cuidados-pessoais','dermocosmeticos','infantil','manipulacao','sua-beleza']
paginas = [88,38,19,14,2,40]

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
        payload = "csrfmiddlewaretoken=lkjxbzLGAkWkGWuqzRxMAEa8c8YJBih7oyPdqsCsx83p0INEWpls9scDaCtXOCbe&"
        for sku in produtos_ids:
            payload += f"produtos_ids%5B%5D={sku}&"
        
        
        return payload

    print("Realizando a extração a partir dos SKU...")
    payload = criar_payload(sku_list)
    detalhes = get_details(payload)
    
    print("Realizando extração final dos detalhes dos produtos...")
    df_detalhes = extract_detalhes(detalhes)
    df_final = df_final.merge(df_detalhes, on="SKU", how="left")
    df_final.to_csv('rosario_teste.csv', index=False)

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

