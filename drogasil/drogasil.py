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
        sem = asyncio.Semaphore(5)
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

def get_apis(sku_list):
    apis = []
    url_api = "https://www.drogasil.com.br/api/next/middlewareGraphql"
    payload = {
    "query": "query ProductBySkuList($skuList: [String!]!, $origin: String, $hasVariants: Boolean!) {\n  productsBySkuList(input: {skuList: $skuList, origin: $origin}) {\n    sku\n    name\n    variant @include(if: $hasVariants) {\n      numberOfVariants\n      oldestSonSku\n      variantTypes {\n        attribute\n        label\n        value\n        image\n        __typename\n      }\n      products {\n        sku\n        name\n        liveComposition {\n          liveStock {\n            qty\n            __typename\n          }\n          __typename\n        }\n        variantTypes {\n          attribute\n          label\n          value\n          image\n          __typename\n        }\n        custom_attributes {\n          attribute_code\n          value_string\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    liveComposition {\n      sku\n      livePrice {\n        bestPrice {\n          valueFrom\n          valueTo\n          updateAt\n          type\n          discountPercentage\n          lmpmValueTo\n          lmpmQty\n          __typename\n        }\n        calcule {\n          valueFrom\n          valueTo\n          lmpmValueTo\n          lmpmQty\n          updateAt\n          type\n          __typename\n        }\n        discountPercentage\n        sku\n        type\n        updateAt\n        valueFrom\n        valueTo\n        lmpmValueTo\n        lmpmQty\n        __typename\n      }\n      liveStock {\n        sku\n        qty\n        dt\n        __typename\n      }\n      __typename\n    }\n    review {\n      average\n      best\n      count\n      histogram {\n        oneStar\n        twoStar\n        threeStar\n        fourStar\n        fiveStar\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n",
    "variables": {
        "skuList": sku_list[:50],
        "hasVariants": True,
        "origin": ""
        }
    }
    payload = json.dumps(payload)
    headers = {
        'authority': 'www.drogasil.com.br',
        'accept': '*/*',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'cookie': 'rrsession=h28ab332b1ca89150d91a; nav_id=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_p=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_browserId=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_c=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_s=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_anonymousUserId=anon-20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_testGroup=%7B%22experiment%22%3Anull%2C%22group%22%3Anull%2C%22testCode%22%3Anull%2C%22code%22%3Anull%2C%22session%22%3Anull%7D; guesttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; carttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; _gid=GA1.3.1680432160.1704587891; _rlu=47ba54e2-e976-4c22-87e4-e560bb2c4464; _rl_rl=0; _rlgm=7DD2zwA|y|11SvYaIv|z69E44GpO:y/k2DJBp5wr:y/k2B07zLgv:y/83Kqr1lP3:y/jY6vPp564:y/GRWzMj7JJ:y/XQpqOOPVl:y/r2V7glXWL:y/57K0OWEvx:y/oZXnvyODK:y/pZLoRznJ1:y/Br9YMo1rN:y/vQLyJQOyr:y/99YWAZ9lx:y/28Dm1NPyP:y/z6YM6PBq8:y|; _rllt=1704587891423; _rll_c_2267_d=1704587891429; _rll_c_2267_c=0; _rll_c_2267_sc=0; _rll_c_2267_sd=1704587891429; _tt_enable_cookie=1; _ttp=uzdapl01ydHOV-Bu2oxlWhhG0Lv; _pin_unauth=dWlkPU5URXhNVFJpT1RrdFpUUXhZUzAwT1RsbExXRTNPR0V0TVRKbFpEQXdOMlU1T0RVeA; _rlsnk=47ba_lr2rmci4; _gcl_au=1.1.1599598983.1704587894; blueID=c6fbd7b8-b313-4c10-9de6-ccfafc7504c1; user_unic_ac_id=8ceddb0d-c39c-cb32-463b-45e807d7f264; advcake_trackid=7aa5faa6-8b89-5bf6-59aa-1bd69c99aba0; OptanonAlertBoxClosed=2024-01-07T00:38:17.147Z; rcsicons=eF4Nx7ENgDAMBMAmFbtYsnkbfzZgjYAbCjpgftKcdG25v-cqNRBiqR7MroHknIi199yH9YOJEg-DuBuFNUxUN4Dh6_QHZvkQcw; rcsproducts=eF4FwbEVgDAIBcAmlbv89yBAAhu4hkIKCzt1fu9ae3M_so9z1URGHNC-EhRlYB8ibsoZtd3fcxWxuIAnqfkMJguFKcA_raERzw; _rll__sel10s_d=1704587916581; _rll__sel10s_c=1; _rll__sel10s_sc=1; _rll__sel10s_sd=1704587916581; _rll__sel20s_d=1704587916584; _rll__sel20s_c=1; _rll__sel20s_sc=1; _rll__sel20s_sd=1704587916584; _rll_c_1622_d=1704587919561; _rll_c_1622_sc=1; _rll_c_1622_sd=1704587919561; _rll_c_1622_c=0; _rll_sc_1622_c=0; _rll__sel30s_d=1704588009841; _rll__sel30s_c=1; _rll__sel30s_sc=1; _rll__sel30s_sd=1704588009841; _rll__sel40s_d=1704588009843; _rll__sel40s_c=1; _rll__sel40s_sc=1; _rll__sel40s_sd=1704588009843; _rll__sel50s_d=1704588009845; _rll__sel50s_c=1; _rll__sel50s_sc=1; _rll__sel50s_sd=1704588009845; _rll__sel1m_d=1704588009846; _rll__sel1m_c=1; _rll__sel1m_sc=1; _rll__sel1m_sd=1704588009846; _hjIncludedInSessionSample_557741=0; _rll_c_1813_d=1704588024848; _rll_c_1813_c=1; _rll_c_1813_sc=1; _rll_c_1813_sd=1704588024848; _rll__sel2m_d=1704588080161; _rll__sel2m_c=1; _rll__sel2m_sc=1; _rll__sel2m_sd=1704588080161; _ga=GA1.1.1543159432.1704587890; OptanonConsent=isIABGlobal=false&datestamp=Sat+Jan+06+2024+21%3A41%3A21+GMT-0300+(Brasilia+Standard+Time)&version=6.22.0&hosts=&consentId=af2697d4-7a7e-43f2-a718-4ffd2dcf34e5&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0008%3A1&geolocation=BR%3BSP&AwaitingReconsent=false; _hjSessionUser_557741=eyJpZCI6IjU2NDk4NDk0LWNjMDgtNWU3Mi05NWNkLWJlNjFmZGE1NjE3NiIsImNyZWF0ZWQiOjE3MDQ1ODgwMTIwOTksImV4aXN0aW5nIjp0cnVlfQ==; cto_bundle=OMBXd19ZalZNVllDJTJCTWQyRXMwVzdvaFluN00lMkJFNEZZWExZdmY1Mk5XNEJJM3N4Q1hOTSUyRlVsVW1yTFo1NmQyOVhOJTJCdlBsWUJ4TTBraTBWQlo5OWw2U3hybnUlMkZ5OEphWjlSVDN0dmhXZ0dmTWlXdEtJY3RHa3ZMTTkxeXlxalI1QWtQdlFoWDNvcjQydVFQT2tqMnZITWhqTzVRTWpOb0x2Y3RleEZnWHVGMzRoZEJCeUNuUlpjaUd2bEtXemlZU1pBYmtkY1ZkSCUyRmlxRlVVaGZlQnpheGduJTJCTEhDNmxjRWZaTUZ4ZDFZbjdOVkpsdXcxQUJsMWdFTDNLVDZyc3ZQem1nWVFGOEdxdyUyQlo5TW1MUlJQYkoyJTJCUjZuS2xJSiUyQlJYYzl5TUcyelNLYjQ3bCUyRnM4c1QzTnhibkhsTEdReEEwM2V3JTJCVA; _hjSession_557741=eyJpZCI6IjQ5MmU4ZDI2LTE1ZjgtNDI5OS05OWJiLTY1YzNmNGIxYzQ1YiIsImMiOjE3MDQ1OTMyMjcwMzEsInMiOjAsInIiOjAsInNiIjowfQ==; _hjAbsoluteSessionInProgress=1; impulsesuite_session=1704593228769-0.48336660000379017; _dc_gtm_UA-69376920-3=1; _dd_s=rum=0&expire=1704594148132; _ga_5J2QJPHWJP=GS1.1.1704593255.2.0.1704593255.60.0.0; mmapi.p.pd=%2251s8E6cgpLaWcdA6TCzeVcVyH7nHTJbbGx5tR9_dY_c%3D%7CBQAAAApDH4sIAAAAAAAEAGNheD4pcJq7xM49DMyZiSmMQgyMTgx6mWd5mBj-_72fpcp_x6Nos8YrCSDNAAT_oYCBzSWzKDW5hNFdggkkDgYwSRANFWJ0BQBc-vK2YQAAAA%3D%3D%22; mmapi.p.bid=%22prodiadcgus03%22; mmapi.p.srv=%22prodiadcgus03%22; chaordic_session=1704593260171-0.4213702262785006',
        'origin': 'https://www.drogasil.com.br',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-session-token-cart': 'ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ'
    }
    response = requests.request("POST", url_api, headers=headers, data=payload)
    # Verifica se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        json_data = response.text
        json_data = json.loads(json_data)
        apis.append(json_data)  
    else :
        print(response.status_code)
    return apis

# Nome e quantidade de página dos departamentos:
departamentos = ['saude', 'medicamentos','vitaminas-e-suplementos','beleza','cosmeticos','cuidados-diarios','drogasil-marcas-exclusivas']
paginas = [15,179,71,168,36,59,89,11]

async def main():
    print("------------------------")
    print("------------------------")
    print("------------------------")
    print("---INÍCIO DA EXTRAÇÃO---")
    print("------------------------")
    print("------------------------")
    print("------------------------")

    urls = []
    # Iterando por departamentos e formando URLS:
    for departamento, n_páginas in zip(departamentos, paginas):
        base_url = "https://www.drogasil.com.br/" + str(departamento) + ".html?p="
        urls_temp = [base_url + str(p) for p in range(1, n_páginas+1)]
        urls.extend(urls_temp)
    responses = await get_response(urls)

    final_df1 = pd.DataFrame()
    final_df2 = pd.DataFrame()

    # Extraindo as informações de HTML:
    i=1
    for response in responses:
        df = await extract_info(response)
        final_df1 = pd.concat([final_df1,df], ignore_index=True)
        print("Response",i,"de",len(responses))
        i+=1
    
    # A partir dos dados de HTML, obtemos o sku_list
    sku_list = final_df1['sku'].tolist()

    # Quebrar a lista sku_list em outras n listas de no máximo 50 elementos
    sku_lists = [sku_list[i:i + 50] for i in range(0, len(sku_list), 50)]
    apis = []

    # Usamos os dados de sku_list para obter as APIs
    i=1
    for sku_list in sku_lists:
        api = get_apis(sku_list)
        apis.append(api)
        print("Puxando API",i,"de",len(sku_lists))
        i+=1
    i=1
    final_df2 = pd.DataFrame()
    for api in apis:
        df = extract_api(api)  
        final_df2 = pd.concat([final_df2,df], ignore_index=True)
        print("Api",i,"de",len(apis))
        i+=1

        
    # Dando merge no dataframe de response e de api e exportando para csv:
    merged_df = pd.merge(final_df1, final_df2, on='sku', how='inner')
    merged_df.to_csv('drogasil_oficial.csv', index=False)

    print("---------------------")
    print("---------------------")
    print("---------------------")
    print("---FIM DA EXTRAÇÃO---")
    print("---------------------")
    print("---------------------")
    print("---------------------")

async def extract_info(response):
    soup = BeautifulSoup(response, 'html.parser')
    # Encontre o script <script id="__NEXT_DATA__" type="application/json">:
    script = soup.find('script', id='__NEXT_DATA__')
    # Leia o script como json:
    script = json.loads(script.string)
    itens = script['props']['pageProps']['pageData']['items']
    dfs = []
    # Obtenha os dados importantes:
    for item in itens:
        sku = item['sku']
        name = item['name']
        custom = item['custom_attributes']
        for c in custom:
            if c['attribute_code'] == 'fabricante':
                fabricante = c['value'][0]['label']
            if c['attribute_code'] == 'marca':
                marca = c['value'][0]['label']
            if c['attribute_code'] == 'ean':
                ean = c['value_string'][0]
            if c['attribute_code'] == 'description':
                descricao = c['value_string']
        nova_linha = {'sku': sku, 'name': name, 'fabricante': fabricante, 'marca': marca, 'ean': ean, 'descricao': descricao}
        # Verifica se a nova_linha está completa:
        if nova_linha:
            df_temp = pd.DataFrame([nova_linha])
            dfs.append(df_temp)

    df_final = pd.DataFrame()
    # Apague dataframes nulos em dfs:
    dfs = [df for df in dfs if not df.empty]
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
    return df_final

def extract_api(api):
    api = api[0]
    itens = api['data']['productsBySkuList']
    dfs = []
    # Obtendo dados importantes:
    for item in itens:
        if item:
            sku = item['sku']
            if item['liveComposition'] and item['liveComposition']['livePrice'] and item['liveComposition']['livePrice']['bestPrice']:
                value_from = item['liveComposition']['livePrice']['bestPrice']['valueFrom']
            else:
                value_from = 'sem value_from'
            if item['liveComposition'] and item['liveComposition']['livePrice'] and item['liveComposition']['livePrice']['bestPrice']:
                value_to = item['liveComposition']['livePrice']['bestPrice']['valueTo']
            else:
                value_to = 'sem value_to'
            if item['liveComposition'] and item['liveComposition']['livePrice'] and item['liveComposition']['livePrice']['bestPrice']:
                value_type = item['liveComposition']['livePrice']['bestPrice']['type']
            else:
                value_type = 'sem value_type'
            if item['liveComposition'] and item['liveComposition']['livePrice'] and item['liveComposition']['livePrice']['bestPrice']:
                discountPercentage = item['liveComposition']['livePrice']['bestPrice']['discountPercentage'] if 'discountPercentage' in item['liveComposition']['livePrice']['bestPrice'] else None
            else:
                discountPercentage = 'sem discountPercentage'
            nova_linha = {'sku': sku, 'value_from': value_from, 'value_to': value_to, 'value_type': value_type, 'discountPercentage': discountPercentage}
            df_temp = pd.DataFrame([nova_linha])  # Criando DataFrame temporário
            dfs.append(df_temp)

    # Concatenando todos os DataFrames temporários
    df_final = pd.concat(dfs, ignore_index=True)
    
    return df_final
    
if __name__ == "__main__":
    asyncio.run(main())

