import requests
from bs4 import BeautifulSoup
import json   
import pandas as pd
import re 
import numpy as np 
import httpx
import asyncio
from unidecode import unidecode
from playwright.async_api import async_playwright
import time


async def fazer_requisicoes(urls, tentativas=4):
    # Lista para armazenar os resultados das requisições
    resultados_totais = []

    # Cria um cliente HTTP assíncrono usando httpx
    async with httpx.AsyncClient() as client:
        # Cria um semáforo para limitar o número de requisições simultâneas
        sem = asyncio.Semaphore(100)  # Limite o número de requisições simultâneas

        # Define uma função assíncrona para fazer uma única requisição
        async def fazer_requisicao(url):
            # Usa o semáforo para limitar o número de requisições simultâneas
            async with sem:
                for tentativa in range(tentativas):
                    try:
                        # Faz a requisição assíncrona usando o cliente HTTP
                        response = await client.get(url)

                        # Verifica se a resposta possui um código de status bem-sucedido
                        response.raise_for_status()

                        # Adiciona a resposta à lista de resultados
                        resultados_totais.append(response)
                        break  # Sai do loop se a requisição for bem-sucedida
                    except httpx.HTTPError as e:
                        # Trata exceções específicas de HTTP
                        print(f"Erro durante a requisição: {e}")
                        print(url)
                    except Exception as e:
                        # Trata exceções genéricas
                        print(f"Erro desconhecido durante a requisição: {e}")
                        print(url)

        # Usa asyncio.gather para executar as chamadas de fazer_requisicao em paralelo
        await asyncio.gather(*[fazer_requisicao(url) for url in urls])

    # Retorna a lista de resultados totais após todas as requisições serem concluídas
    return resultados_totais


async def extrai_links_marcas():

    # função para extrair marcas e suas respectivas quantidades de páginas (ok)
    async def extrai_marcas_quantidades(json_file, departamento):
        dicionario = {'Marca': [], 'Departamento': [], 'Paginas': [], 'Link': []}

        for chave, valor in json_file.items():
            if re.match(r'.*\.facets\.0\.values\(\{\}\)\.\d+$', chave):
                marca = valor['value']
                quantidade_produtos = valor['quantity']
                paginas = 1 if quantidade_produtos < 12 else min(50, (quantidade_produtos + 11) // 12)
                dicionario['Marca'].append(marca)
                dicionario['Paginas'].append(paginas)
                dicionario['Departamento'].append(departamento)
                dicionario['Link'].append(f'https://www.drogariastamoio.com.br/{departamento}/{marca}?initialMap=c&initialQuery={departamento}&map=category-1,brand')

        return dicionario

    # departamentos disponíveis
    departamentos = ['medicamentos',
                    'cuidados-com-a-saude',
                    'infantil',
                    'dermocosmeticos',
                    'higiene-pessoal',
                    'cuidados-com-os-cabelos',
                    'cuidados-com-a-pele']

    marcas_departamentos = pd.DataFrame()
    for departamento in departamentos:
        req = requests.get(f'https://www.drogariastamoio.com.br/{departamento}')
        soup = BeautifulSoup(req.text, 'html.parser')
        json_file = json.loads(soup.find('template', {'data-varname': '__STATE__'}).find('script').text)
        df_variavel = pd.DataFrame(await extrai_marcas_quantidades(json_file, departamento))
        marcas_departamentos = pd.concat([marcas_departamentos, df_variavel])

    marcas_departamentos.reset_index(inplace=True)

    return marcas_departamentos


# função para fazer a paginação (ok)
async def junta_urls(df):

    links = []
    urls = df['Link']
    responses = await fazer_requisicoes(urls)
    for response in responses:
        paginas = int(df[df['Link'] == response.url]['Paginas'].to_list()[0])
        for pagina in range(1, paginas + 1):
            links.append(str(response.url) + f'&page={pagina}')
    
    return links


# extrai o arquivo json
async def extrai_json(response):

    soup = BeautifulSoup(response.text, 'html.parser')
    return json.loads(soup.find('template', {'data-varname': '__STATE__'}).find('script').text)


# extrai as chaves para acessar os produtos
async def extrai_itens(json_data):
    # Define o padrão de regex
    padrao = re.compile(r'Product:sp-\d+.items\({"filter":"ALL"}\).0$')

    # Retorna uma lista com as chaves que correspondem ao padrão
    return [item for item in json_data.keys() if padrao.match(item)]


# extrai o nome e os códigos sku e ean
async def extrai_nome_sku_ean(json_data, num_produto, lista_filtrada):

    return json_data[lista_filtrada[num_produto]]['name'] or np.nan, json_data[lista_filtrada[num_produto]]['itemId'] or np.nan, json_data[lista_filtrada[num_produto]]['ean'] or np.nan


# extrai a marca do produto 
async def extrai_marca_produto(json_data, lista_filtrada, num_produto):

    return json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '')]['brand'] or np.nan


# extração do preço, preço com desconto e porcentagem de desconto
async def extrai_preco_desconto(json_data, num_produto, lista_filtrada):
    # Cria a chave do produto para buscar no json_data
    product_key = f'${lista_filtrada[num_produto]}.sellers.0.commertialOffer'
    
    # Usa o método get para evitar exceções KeyError
    # Se a chave não existir no dicionário, retorna um dicionário vazio
    offer = json_data.get(product_key, {})
    
    # Busca os preços no dicionário offer
    # Se a chave não existir, retorna 0 como valor padrão
    preco_sem_desconto = float(offer.get('ListPrice', 0)) or np.nan
    preco_com_desconto = float(offer.get('Price', 0)) or np.nan
    porcentagem_desconto = np.round(1 - (preco_com_desconto / preco_sem_desconto), 2)  if 0 <= np.round(1 - (preco_com_desconto / preco_sem_desconto), 2) < 1 else np.nan
    
    # Retorna os preços e o tipo de desconto
    return preco_sem_desconto, preco_com_desconto, porcentagem_desconto


async def extrai_descricao(json_data, lista_filtrada, num_produto):
    # Tenta extrair a descrição
    try:
        # extraindo o html com a descrição 
        html_text = json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '')]['description']
        
        # Parse da string HTML usando BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')

        # Extrair a descrição sem tags HTML
        texto = soup.get_text(separator='', strip=True)
    
    # Se ocorrer um erro, retorna NaN
    except Exception as e:
        texto = [np.nan]

    # Retorna o primeiro texto extraído, ou NaN se a lista estiver vazia
    return texto


async def main():

    json_path = 'drogarias_tamoio/base_oficial_drogarias_tamoio.json'

    df = await extrai_links_marcas()
    links = await junta_urls(df)
    print(f'Justamos {len(links)} urls')

    # realizando as requisições
    responses = await fazer_requisicoes(links)
    print(f'Trabalharemos com {len(responses)} urls')

    # extraindo as informações necessárias para cada requisição 
    for response in responses:
        informacoes_generator = extrai_informacoes(response)
        async for json_str in informacoes_generator:
            # Escreve o JSON no arquivo
            with open(json_path, 'a') as f:
                f.write(json_str + '\n')


async def extrai_informacoes(response):
    json_data = await extrai_json(response)
    lista_filtrada = await extrai_itens(json_data)

    for num_produto in range(len(lista_filtrada)):
        try:
            nome_sku_ean = await extrai_nome_sku_ean(json_data, num_produto, lista_filtrada)
            preco_desconto = await extrai_preco_desconto(json_data, num_produto, lista_filtrada)
            descricao = await extrai_descricao(json_data, lista_filtrada, num_produto)
            marca = await extrai_marca_produto(json_data, lista_filtrada, num_produto)

            informacoes = {
                'EAN': nome_sku_ean[2],
                'SKU': nome_sku_ean[1],
                'Nome': nome_sku_ean[0],
                'Marca': marca,
                'Descrição': descricao,
                'Farmácia': 'Drogarias Tamoio',
                'Preço_sem_desconto': preco_desconto[0],
                'Preço_com_desconto': preco_desconto[1],
                'Porcentagem_desconto': preco_desconto[2],
                'Cidade': 'São Paulo',
                'Região': 'Sudeste'
            }

            # Gera o JSON estantâneo
            json_str = json.dumps(informacoes)

            yield json_str
        
        except httpx.HTTPError as e:
            # Trata exceções específicas de HTTP
            print(f"Erro: {e}")

        except Exception as e:
            # Trata exceções genéricas
            print(f"Erro: {e}")


asyncio.run(main())