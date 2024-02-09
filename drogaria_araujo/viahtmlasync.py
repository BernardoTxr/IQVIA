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
        
        script = soup.find('script', {'type': 'application/ld+json'})
        if script:
            json_data = json.loads(script.string)

            nome = json_data['name']
            descricao = json_data['description']
            sku = json_data['sku']
            preco = json_data['offers']['price']

        return nome, descricao, sku, preco
    
    async def extract_html (self, soup):
        marca = None
        th_tags = soup.find_all('th')
        for th in th_tags:
            if th.text.strip() == 'Marca':
                td_tag = th.find_next('td')
                if td_tag:
                    div_tag = td_tag.find('div')
                    if div_tag:
                        marca = div_tag.text.strip()
        return marca

            
            
# Url das páginas iniciais (? total):
setores = ['medicamentos','infantil','dermocosmeticos','saude','beleza','higiene-pessoal','pet-shop','nutricao-saudavel','mercado','maquiagem','cabelo']
paginas = [152,51,27,58,40,44,19,23,78,13,48]

urls = []
for departamento, n_páginas in zip(setores, paginas):
    base_url = "https://www.araujo.com.br/" + str(setores) + "?start="
    urls_temp = [base_url + str((p-1)*50) + "&sz=50&page=" + str(p) for p in range(1, n_páginas + 1)]
    urls.extend(urls_temp)

# Criação do primeiro scrapper:
mainScraper = mainScraper(urls)

print("Passou pelo Main Scrapper!!!")
#print(mainScraper.urls_list)
#print(len(mainScraper.urls_list))

# Criação do segundo scrapper (recebe a saída do primeiro scrapper como entrada):
individualScraper = individualScraper(mainScraper.urls_list)

df = pd.DataFrame.from_dict(individualScraper.master_dict, orient='index')
df['Farmácia'] = 'Drogaria Araujo'

df.to_csv('drogaria_araujo_teste.csv', index=False)

print('Passou pelo Individual Scrapper!!!')