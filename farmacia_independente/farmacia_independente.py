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
    url = "https://www.farmaciasindependente.com.br/pesquisa/pesquisar"
    payload = "csrfmiddlewaretoken=z2x4KPSnwgNL9AgHIFUoiXW6L7Vs7R1gX0BtvToq2Ust9IIosqzrmvYZxoPl64dQ&termo=categorias%2F"+str(setor)+"&pagina="+str(pagina)+"&ordenacao=&is_termo=true"
    headers = {
    'authority': 'www.farmaciasindependente.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=5P3qxsWjiDaL21fYiznZzee7yt8YfSn7tN7PiwsmOhPt29HF2k22DMg0kK2Re5zH; _ga=GA1.1.1125411275.1707569622; _tt_enable_cookie=1; _ttp=awjDlCqOIBnYorB2Yzt_QP3xOiq; FPID=FPID2.3.bj%2FzoswS3uNE5WjdVtg9ObqJe6K684itHrnY7PbhK%2BA%3D.1707569622; FPAU=1.3.222754983.1707569623; _pin_unauth=dWlkPU56Y3hNamd4WWpZdFlqWmxNUzAwTVRRMExUa3pPV1F0TVRjM056SmtaVEV6TVRjeA; sessionid=1qbimoxs672rcutx93e1s6870fzvkk7i; __udf_j=794a91255ec0aa44576eab070b5562d2bee2bb93c9abfd0c72f4151db4a59b31e5a42a7b1fed45c1a76ee69d8f732509; __csfpsid_1022520045=MXFiaW1veHM2NzJyY3V0eDkzZTFzNjg3MGZ6dmtrN2kqVGh1LCAxNSBGZWIgMjAyNCAxMDoyMzoyMCBHTVQ=; FPLC=HH%2BkDZ1cmKkjG6RzDy3EN4vNGg2zwviIqHNzHSJc5bn2lZSt8wZ30e8C9qGhrOeoK1FE5WIOLjUwCOuxA441C3DAgGeM5RdLpcAiyswobE8WvgFgEAnQ%2FxVZQq7zLg%3D%3D; FPGSID=1.1707906200.1707906200.G-ZM7LND5J6P.l68SlycCD1zfckjlVJj0mg; _ga_ZM7LND5J6P=GS1.1.1707906200.4.1.1707906207.53.0.0; _uetsid=1491a020cb2311ee9ee751a7f00c0f43; _uetvid=6b10bb40c81311eeb9ba2ffe863e61d9; __csfpsid_1276504757=MXFiaW1veHM2NzJyY3V0eDkzZTFzNjg3MGZ6dmtrN2kqVGh1LCAxNSBGZWIgMjAyNCAxMDoyMzoyNyBHTVQ=; _ga_43W2WYML5H=GS1.1.1707906202.4.1.1707906208.0.0.0',
    'origin': 'https://www.farmaciasindependente.com.br',
    'pragma': 'no-cache',
    'referer': 'https://www.farmaciasindependente.com.br/categorias/medicamentos',
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
    url = "https://www.farmaciasindependente.com.br/pegar/preco"
    payload = payload
    headers = {
    'authority': 'www.farmaciasindependente.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=5P3qxsWjiDaL21fYiznZzee7yt8YfSn7tN7PiwsmOhPt29HF2k22DMg0kK2Re5zH; _ga=GA1.1.1125411275.1707569622; _tt_enable_cookie=1; _ttp=awjDlCqOIBnYorB2Yzt_QP3xOiq; FPID=FPID2.3.bj%2FzoswS3uNE5WjdVtg9ObqJe6K684itHrnY7PbhK%2BA%3D.1707569622; FPAU=1.3.222754983.1707569623; _pin_unauth=dWlkPU56Y3hNamd4WWpZdFlqWmxNUzAwTVRRMExUa3pPV1F0TVRjM056SmtaVEV6TVRjeA; sessionid=1qbimoxs672rcutx93e1s6870fzvkk7i; __udf_j=794a91255ec0aa44576eab070b5562d2bee2bb93c9abfd0c72f4151db4a59b31e5a42a7b1fed45c1a76ee69d8f732509; __csfpsid_1022520045=MXFiaW1veHM2NzJyY3V0eDkzZTFzNjg3MGZ6dmtrN2kqVGh1LCAxNSBGZWIgMjAyNCAxMDoyMzoyMCBHTVQ=; FPLC=HH%2BkDZ1cmKkjG6RzDy3EN4vNGg2zwviIqHNzHSJc5bn2lZSt8wZ30e8C9qGhrOeoK1FE5WIOLjUwCOuxA441C3DAgGeM5RdLpcAiyswobE8WvgFgEAnQ%2FxVZQq7zLg%3D%3D; FPGSID=1.1707906200.1707906200.G-ZM7LND5J6P.l68SlycCD1zfckjlVJj0mg; _uetsid=1491a020cb2311ee9ee751a7f00c0f43; _uetvid=6b10bb40c81311eeb9ba2ffe863e61d9; __csfpsid_1276504757=MXFiaW1veHM2NzJyY3V0eDkzZTFzNjg3MGZ6dmtrN2kqVGh1LCAxNSBGZWIgMjAyNCAxMDoyMzoyNyBHTVQ=; _ga_ZM7LND5J6P=GS1.1.1707906200.4.1.1707906232.28.0.0; _ga_43W2WYML5H=GS1.1.1707906202.4.1.1707906232.0.0.0; sessionid=1qbimoxs672rcutx93e1s6870fzvkk7i',
    'origin': 'https://www.farmaciasindependente.com.br',
    'pragma': 'no-cache',
    'referer': 'https://www.farmaciasindependente.com.br/categorias/medicamentos',
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
departamentos = ['medicamentos','saude','alimentos-e-bebidas','higiene-e-cuidados','mamae-e-bebe','conveniencia','beleza-e-perfumaria']
paginas = [115,12,6,37,12,5,41]

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
        payload = "csrfmiddlewaretoken=7FuanjDu5TjZjxCAqKxFm9SyODVEbcULvDyz8n9xBxYHjF4havcIqHUrAUPxap6l&"
        for sku in produtos_ids:
            payload += f"produtos_ids%5B%5D={sku}&"
        
        
        return payload

    print("Realizando a extração a partir dos SKU...")
    payload = criar_payload(sku_list)
    detalhes = get_details(payload)
    
    print("Realizando extração final dos detalhes dos produtos...")
    df_detalhes = extract_detalhes(detalhes)
    df_final = df_final.merge(df_detalhes, on="SKU", how="left")
    df_final.to_csv('farmacia_independente_oficial.csv', index=False)

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

