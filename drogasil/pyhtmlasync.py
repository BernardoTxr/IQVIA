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
            'authority': 'www.drogasil.com.br',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'cookie': 'rrsession=h28ab332b1ca89150d91a; nav_id=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_p=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_browserId=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_c=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_s=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_anonymousUserId=anon-20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_testGroup=%7B%22experiment%22%3Anull%2C%22group%22%3Anull%2C%22testCode%22%3Anull%2C%22code%22%3Anull%2C%22session%22%3Anull%7D; guesttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; carttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; _gid=GA1.3.1680432160.1704587891; _rlu=47ba54e2-e976-4c22-87e4-e560bb2c4464; _rl_rl=0; _rlgm=7DD2zwA|y|11SvYaIv|z69E44GpO:y/k2DJBp5wr:y/k2B07zLgv:y/83Kqr1lP3:y/jY6vPp564:y/GRWzMj7JJ:y/XQpqOOPVl:y/r2V7glXWL:y/57K0OWEvx:y/oZXnvyODK:y/pZLoRznJ1:y/Br9YMo1rN:y/vQLyJQOyr:y/99YWAZ9lx:y/28Dm1NPyP:y/z6YM6PBq8:y|; _rllt=1704587891423; _rll_c_2267_d=1704587891429; _rll_c_2267_c=0; _rll_c_2267_sc=0; _rll_c_2267_sd=1704587891429; _tt_enable_cookie=1; _ttp=uzdapl01ydHOV-Bu2oxlWhhG0Lv; _pin_unauth=dWlkPU5URXhNVFJpT1RrdFpUUXhZUzAwT1RsbExXRTNPR0V0TVRKbFpEQXdOMlU1T0RVeA; _rlsnk=47ba_lr2rmci4; _gcl_au=1.1.1599598983.1704587894; blueID=c6fbd7b8-b313-4c10-9de6-ccfafc7504c1; user_unic_ac_id=8ceddb0d-c39c-cb32-463b-45e807d7f264; advcake_trackid=7aa5faa6-8b89-5bf6-59aa-1bd69c99aba0; OptanonAlertBoxClosed=2024-01-07T00:38:17.147Z; _rll__sel10s_d=1704587916581; _rll__sel10s_c=1; _rll__sel10s_sc=1; _rll__sel10s_sd=1704587916581; _rll__sel20s_d=1704587916584; _rll__sel20s_c=1; _rll__sel20s_sc=1; _rll__sel20s_sd=1704587916584; _rll_sc_1622_c=0; _rll__sel30s_d=1704588009841; _rll__sel30s_c=1; _rll__sel30s_sc=1; _rll__sel30s_sd=1704588009841; _rll__sel40s_d=1704588009843; _rll__sel40s_c=1; _rll__sel40s_sc=1; _rll__sel40s_sd=1704588009843; _rll__sel50s_d=1704588009845; _rll__sel50s_c=1; _rll__sel50s_sc=1; _rll__sel50s_sd=1704588009845; _rll__sel1m_d=1704588009846; _rll__sel1m_c=1; _rll__sel1m_sc=1; _rll__sel1m_sd=1704588009846; _rll_c_1813_d=1704588024848; _rll_c_1813_c=1; _rll_c_1813_sc=1; _rll_c_1813_sd=1704588024848; _rll__sel2m_d=1704588080161; _rll__sel2m_c=1; _rll__sel2m_sc=1; _rll__sel2m_sd=1704588080161; _hjSessionUser_557741=eyJpZCI6IjU2NDk4NDk0LWNjMDgtNWU3Mi05NWNkLWJlNjFmZGE1NjE3NiIsImNyZWF0ZWQiOjE3MDQ1ODgwMTIwOTksImV4aXN0aW5nIjp0cnVlfQ==; _rll__sel5m_d=1704593267364; _rll__sel5m_c=1; _rll__sel5m_sc=1; _rll__sel5m_sd=1704593267364; _rll__sel10m_d=1704593267365; _rll__sel10m_c=1; _rll__sel10m_sc=1; _rll__sel10m_sd=1704593267365; _rll__sel30m_d=1704593267366; _rll__sel30m_c=1; _rll__sel30m_sc=1; _rll__sel30m_sd=1704593267366; _rll__sel60m_d=1704593267367; _rll__sel60m_c=1; _rll__sel60m_sc=1; _rll__sel60m_sd=1704593267367; rcsicons=eF4Vx7sNgDAQBNHEEb2sdMt9fHRAGzZOCMiA-jEa6UlTluu9zyHUVLCKxV-k-DyA5Tn2xq1n1QFzKsyYyNEIkVBNt3X6AWWbEGw; mmapi.p.bid=%22prodiadcgus02%22; mmapi.p.srv=%22prodiadcgus02%22; rcsproducts=eF4Vx7ERgDAIBdAmlbv8uxAIgQ1cQyGFhZ06v3qvemU5n-vISmwMGlX0Z_qtC0DljnWLpvvMgXDfIG0GqmcHmTJbFwrPF4KDEdI; impulsesuite_session=1704646471995-0.10658159894229269; chaordic_session=1704646472287-0.06931777319778276; _rll_c_1622_d=1704646473999; _rll_c_1622_sc=2; _rll_c_1622_sd=1704646473999; _rll_c_1622_c=0; _hjIncludedInSessionSample_557741=0; _hjSession_557741=eyJpZCI6IjFkZDYxMDM5LWI0NjAtNGYwMi1iZmJhLTBkNmE1ZWFmYzE3MCIsImMiOjE3MDQ2NDY1MjMxMzAsInMiOjAsInIiOjAsInNiIjowfQ==; _hjAbsoluteSessionInProgress=0; rcspdp=eF4NxLENgDAMBMAmFbs8wvZjOxswRwiRKOiA-eGKK-XZt9Fr09UVzEGwtr_MgNLNMpocotP13mefxdIgsdDp6WpqICAfbDEQqQ; mmapi.p.pd=%22dt2xbRiKUUDUSN1thEBCy_6hE3opjyWfV_lH5uI8PIw%3D%7CHgAAAApDH4sIAAAAAAAEAGNheD4pcJq7xM49DMyZiSmMQgyMTgwqTLNfsDEsPL6yYhH_HY-izRqvJIA0AxD8hwIGNpfMotTkEkZ3CTaQOBjAJEE0VIjRFQA25gtUYQAAAA%3D%3D%22; _ga=GA1.1.1543159432.1704587890; _ga_5J2QJPHWJP=GS1.1.1704646471.6.1.1704646973.35.0.0; cto_bundle=oHRiD19ZalZNVllDJTJCTWQyRXMwVzdvaFluN0JCVTJnZ1NPbElKdzBmTEl1WHRub255MHN6SXEwSXF4VUJLdDRodHJJZXJIV1pDc2d4VE9seWV2Z1lOUlglMkZWcUdVSVNGSWlXR2lSSXBJeWJvaXF2YlJTbTk5ckhXUFozMlJ2c1Y3NllKTFZqd1VnVnhCUkIxTm5wdCUyQmtWSFBXJTJCdzZpZXoxUFYlMkY4TEQwTG9mMXFQTW8zOWJZV01JdnJQaEJPM1JaWElnbDI1MXJ4TThtVkVFcHdZdjFja1B2Z1ByQVdpciUyRmg5WmRvYk4zZUdBU2d4M1FtQWhyJTJGZXJPaXhBbkJUdGtQOVpKc2UyY3RMcTdSOG9FR2RhWUIlMkZ4ZVM1QSUyRk0lMkY4cG41cDBMTG8lMkJaSUw2ODllOFZUdUxFeGhKJTJCSmJOM05tdmhPOFBaSQ; OptanonConsent=isIABGlobal=false&datestamp=Sun+Jan+07+2024+14%3A02%3A54+GMT-0300+(Brasilia+Standard+Time)&version=6.22.0&hosts=&consentId=af2697d4-7a7e-43f2-a718-4ffd2dcf34e5&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0008%3A1&geolocation=BR%3BSP&AwaitingReconsent=false; _dc_gtm_UA-69376920-3=1; _dd_s=rum=0&expire=1704648022438; rcspdp=eF5j4cotK8lM0TM0tjDWNTQ3MDEzNjU2MzIwMNI10dU1ZClN9khLsUwyMjUDClikmeiaWCYBCQsLc10joFpjC_Mkw1RDIwBZ5RCc',
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
            url_alvo = await asyncio.gather(*tasks)
            # Adiciona os resultados à lista urls_list
            for url in url_alvo:
                #print(url)
                self.urls_list.extend(url)
        
    async def extract_url(self, text):
            soup = BeautifulSoup(text, 'html.parser')
            print(soup)
            div_with_href = soup.find('div', {'class': 'ProductGridstyles__ProductGridStyles-tflwc1-0 gquykZ'})
            if div_with_href:
                href_tags = div_with_href.find_all('a', {'class': 'LinkNextstyles__LinkNextStyles-t73o01-0 cpRdBZ LinkNext'})
                return href_tags
            else:
                return 'Div not found or doesnt have the specified class.'
            
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
        
        script = soup.find('script', {'type': 'application/ld+json'})
        if script:
            json_data = json.loads(script.string)

            nome = json_data['name']
            descricao = json_data['description']
            sku = json_data['sku']
            preco = json_data['offers']['price']

        return nome, descricao, sku, preco

            
            

# Selecionando as urls das páginas de medicamentos a partir do número de páginas totais (215 total)
n_páginas = 215
base_url = "https://www.drogasil.com.br/medicamentos.html?p="
urls = [base_url + str(p) for p in range(1, n_páginas+1)]

# Criação do primeiro scrapper:
mainScraper = mainScraper(urls)

print("Passou pelo Main Scrapper!!!")
#print(mainScraper.urls_list)
#print(len(mainScraper.urls_list))

'''

# Criação do segundo scrapper (recebe a saída do primeiro scrapper como entrada):
individualScraper = individualScraper(mainScraper.urls_list)

df = pd.DataFrame.from_dict(individualScraper.master_dict, orient='index')

df.to_csv('C:/Users/berna/OneDrive/Documents/[PJ]IQVIA/drogaria_araujo/drogaria_araujo_teste.csv', index=False)

print('Passou pelo Individual Scrapper!!!')

'''