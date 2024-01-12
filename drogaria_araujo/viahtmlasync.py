import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import asyncio
import aiohttp

'''
O Scrapping será realizado em 2 partes:
O Scrapping será realizado por objetos da classe Scrapper em 2 etapas:
1. mainScrapper: objeto responsável por iterar nas URLS das páginas de medicamento da Drogasil, selecionando o URLS de todos os produtos.
2. individualScrapper: objeto responsável por iterar nas URLS selecionadas pelo mainScrapper. Realizará a seleção de informações relevantes.
'''

class mainScraper(object):

    '''
    Função de Inicialização
    '''
    def __init__(self, urls):
        # Urls são passadas para o objeto
        self.urls = urls
        # Lista para armazenas as páginas para scrappear
        self.urls_list = []
        # Rodar o scrapper:
        asyncio.run(self.main())

    '''
    Função fetch que realiza a solicitação HTTP assíncrona
    '''
    async def fetch(self, session, url):
        try:
            async with session.get(url) as response:
                text = await response.text()
                url_alvo = await self.extract_url(text)
                return url_alvo
        except Exception as e:
            print(str(e))
    
    '''
    Função main orquestradora
    '''
    async def main(self):
        tasks = []
        headers = {
        'authority': 'www.araujo.com.br',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'cookie': 'dwanonymous_26e1a7081f3eae6e77b9b8a8fe4c8863=ac9VJcRVu5IxizaX4kFBIv7Uun; _gcl_au=1.1.1186962734.1704461251; _ga=GA1.1.266579656.1704461251; blueID=1c17c53a-26f7-4074-a184-263a80b84674; __cq_uuid=ac9VJcRVu5IxizaX4kFBIv7Uun; __privaci_cookie_consent_uuid=802309f7-e047-4d90-8b69-0ba95fae4c57:2; __privaci_cookie_consent_generated=802309f7-e047-4d90-8b69-0ba95fae4c57:2; _hjSessionUser_1858274=eyJpZCI6IjBhZWQ3Njg4LTg1MDEtNTZjMC1hNTEzLTI0ZDE4Y2E1YmY2NyIsImNyZWF0ZWQiOjE3MDQ0NjEyNTM0MzgsImV4aXN0aW5nIjp0cnVlfQ==; __privaci_cookie_consents={"consents":{"145":1,"146":1,"147":1,"148":1,"150":1},"location":"SP#BR","lang":"pt-br","gpcInBrowserOnConsent":false,"gpcStatusInPortalOnConsent":false,"status":"record-consent-success","implicit_consent":false,"suppressNonEssentials":false}; dwac_5237b620ba0dd344e50c944ce2=EpPdSFDSJ9hg1MaR5Z5sW3TOznPTrtMATgc%3D|dw-only|||BRL|false|America%2FSao%5FPaulo|true; cqcid=ac9VJcRVu5IxizaX4kFBIv7Uun; cquid=||; sid=EpPdSFDSJ9hg1MaR5Z5sW3TOznPTrtMATgc; __cq_dnt=0; dw_dnt=0; dwsid=XFdVp9PZUFQEve9sUDu_kjoKRpxQpaSMKm5pOU4WvIOlaeHIk1a9q9jRjcQJm3aFrJb0Xk0H8ozqgiwrX6sw8A==; _gcl_aw=GCL.1705080429.CjwKCAiA44OtBhAOEiwAj4gpOWLJH8HCaCzJBcV138pGeGcFXA0N4oZtgnsmobr0EIUD9OiHXv-zpRoCclQQAvD_BwE; AwinChannelCookie=other; origem=adwords; _hjIncludedInSessionSample_1858274=0; _hjSession_1858274=eyJpZCI6ImVlN2ZmNzMwLWJhZjQtNGQwNC04NzJlLTljMTgwNTQyMDU1MiIsImMiOjE3MDUwODA0MzA5OTUsInMiOjAsInIiOjAsInNiIjoxfQ==; _hjAbsoluteSessionInProgress=0; __cq_bc=%7B%22bgtx-Araujo%22%3A%5B%7B%22id%22%3A%22M-40457%22%2C%22sku%22%3A%2240457%22%7D%2C%7B%22id%22%3A%22M-17153%22%2C%22sku%22%3A%2217153%22%7D%2C%7B%22id%22%3A%22M-82510%22%2C%22sku%22%3A%2282510%22%7D%2C%7B%22id%22%3A%22M-11979%22%2C%22sku%22%3A%2211979%22%7D%2C%7B%22id%22%3A%22M-5157%22%2C%22sku%22%3A%225157%22%7D%2C%7B%22id%22%3A%22M-98867%22%2C%22sku%22%3A%2298867%22%7D%5D%7D; __cq_seg=0~0.15!1~-0.03!2~-0.16!3~0.39!4~-0.69!5~-0.07!6~0.32!7~0.19!8~0.42!9~0.08; _ga_5QCQVRJ2EG=GS1.1.1705080430.8.1.1705082325.60.0.0',
        'pragma': 'no-cache',
        'referer': 'https://www.araujo.com.br/medicamentos?start=50&sz=50&page=2',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            # Itera sobre todos os URLS passados
            for url in self.urls:
                # Adiciona a tarefa assíncrona à lista de tarefas tasks
                tasks.append(self.fetch(session, url))
            # Gather realiza as tarefas assíncronas em paralelo e coloca os resultados em htmls
            url_alvo = await asyncio.gather(*tasks)
            # Adiciona os resultados à lista urls_list
            for url in url_alvo:
                self.urls_list.extend(url)
        
    async def extract_url(self, text):
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
            
class individualScraper(object):

    '''
    Função de Inicialização
    '''
    def __init__(self, urls):
        # Urls são passadas para o objeto
        self.urls = urls
        # Dicionário para armazenar os dados coletados com a URL como chave
        self.master_dict = {}
        # Rodar o scrapper:
        asyncio.run(self.main())

    '''
    Função fetch que realiza a solicitação HTTP assíncrona
    '''
    async def fetch(self, session, url):
        try:
            async with session.get(url) as response:
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                nome, descricao, sku, preco = await self.extract_api(soup)
                return nome, descricao, sku, preco
        except Exception as e:
            print(str(e))
    
    '''
    Função main orquestradora
    '''
    async def main(self):
        tasks = []
        headers = {
        'authority': 'www.araujo.com.br',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'cookie': 'dwanonymous_26e1a7081f3eae6e77b9b8a8fe4c8863=ac9VJcRVu5IxizaX4kFBIv7Uun; _gcl_au=1.1.1186962734.1704461251; _ga=GA1.1.266579656.1704461251; blueID=1c17c53a-26f7-4074-a184-263a80b84674; __cq_uuid=ac9VJcRVu5IxizaX4kFBIv7Uun; __privaci_cookie_consent_uuid=802309f7-e047-4d90-8b69-0ba95fae4c57:2; __privaci_cookie_consent_generated=802309f7-e047-4d90-8b69-0ba95fae4c57:2; _hjSessionUser_1858274=eyJpZCI6IjBhZWQ3Njg4LTg1MDEtNTZjMC1hNTEzLTI0ZDE4Y2E1YmY2NyIsImNyZWF0ZWQiOjE3MDQ0NjEyNTM0MzgsImV4aXN0aW5nIjp0cnVlfQ==; __privaci_cookie_consents={"consents":{"145":1,"146":1,"147":1,"148":1,"150":1},"location":"SP#BR","lang":"pt-br","gpcInBrowserOnConsent":false,"gpcStatusInPortalOnConsent":false,"status":"record-consent-success","implicit_consent":false,"suppressNonEssentials":false}; dwac_5237b620ba0dd344e50c944ce2=EpPdSFDSJ9hg1MaR5Z5sW3TOznPTrtMATgc%3D|dw-only|||BRL|false|America%2FSao%5FPaulo|true; cqcid=ac9VJcRVu5IxizaX4kFBIv7Uun; cquid=||; sid=EpPdSFDSJ9hg1MaR5Z5sW3TOznPTrtMATgc; __cq_dnt=0; dw_dnt=0; dwsid=XFdVp9PZUFQEve9sUDu_kjoKRpxQpaSMKm5pOU4WvIOlaeHIk1a9q9jRjcQJm3aFrJb0Xk0H8ozqgiwrX6sw8A==; _gcl_aw=GCL.1705080429.CjwKCAiA44OtBhAOEiwAj4gpOWLJH8HCaCzJBcV138pGeGcFXA0N4oZtgnsmobr0EIUD9OiHXv-zpRoCclQQAvD_BwE; AwinChannelCookie=other; origem=adwords; _hjIncludedInSessionSample_1858274=0; _hjSession_1858274=eyJpZCI6ImVlN2ZmNzMwLWJhZjQtNGQwNC04NzJlLTljMTgwNTQyMDU1MiIsImMiOjE3MDUwODA0MzA5OTUsInMiOjAsInIiOjAsInNiIjoxfQ==; _hjAbsoluteSessionInProgress=0; __cq_bc=%7B%22bgtx-Araujo%22%3A%5B%7B%22id%22%3A%22M-40457%22%2C%22sku%22%3A%2240457%22%7D%2C%7B%22id%22%3A%22M-17153%22%2C%22sku%22%3A%2217153%22%7D%2C%7B%22id%22%3A%22M-82510%22%2C%22sku%22%3A%2282510%22%7D%2C%7B%22id%22%3A%22M-11979%22%2C%22sku%22%3A%2211979%22%7D%2C%7B%22id%22%3A%22M-5157%22%2C%22sku%22%3A%225157%22%7D%2C%7B%22id%22%3A%22M-98867%22%2C%22sku%22%3A%2298867%22%7D%5D%7D; __cq_seg=0~0.15!1~-0.03!2~-0.16!3~0.39!4~-0.69!5~-0.07!6~0.32!7~0.19!8~0.42!9~0.08; _ga_5QCQVRJ2EG=GS1.1.1705080430.8.1.1705082325.60.0.0',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            # Itera sobre todos os URLS passados
            for url in self.urls:
                # Adiciona a tarefa assíncrona à lista de tarefas tasks
                tasks.append(self.fetch(session, url))
            # Gather realiza as tarefas assíncronas em paralelo e coloca os resultados em htmls
            rows = await asyncio.gather(*tasks)

            # Itera sobre todos os htmls e coloca no dicionário
            for row in rows:
                nome = row[0]
                descricao = row[1]
                sku = row[2]
                preco = row[3]
                self.master_dict[row] = {
                    'Nome': nome,
                    'Descrição': descricao,
                    'SKU': sku,
                    'Preço': preco,
                }
        
    async def extract_api (self,soup):
        nome = None
        descricao = None
        sku = None
        preco = None

        #print(soup)
        #print('')
        #print('')
        #print('')
        
        script = soup.find('script', {'type': 'application/ld+json'})
        if script:
            json_data = json.loads(script.string)

            nome = json_data['name']
            descricao = json_data['description']
            sku = json_data['sku']
            preco = json_data['offers']['price']

        return nome, descricao, sku, preco

            
            
# Url das páginas iniciais (? total):
n_páginas = 1
urls = ["https://www.araujo.com.br/medicamentos?start=" + str((p-1)*50) + "&sz=50&page=" + str(p) for p in range(1, n_páginas + 1)]

# Criação do primeiro scrapper:
mainScraper = mainScraper(urls)

print("Passou pelo Main Scrapper!!!")
#print(mainScraper.urls_list)
#print(len(mainScraper.urls_list))

# Criação do segundo scrapper (recebe a saída do primeiro scrapper como entrada):
individualScraper = individualScraper(mainScraper.urls_list)

print(individualScraper.master_dict)