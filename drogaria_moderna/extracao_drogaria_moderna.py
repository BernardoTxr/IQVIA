import requests
from bs4 import BeautifulSoup
import json   
import pandas as pd
import re 
import numpy as np 
import httpx
import asyncio
import random
import aiohttp


async def fazer_requisicoes(urls, tentativas=4):
    # Lista para armazenar os resultados das requisições com resposta 200
    resultados_200 = []

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

                        # Verifica se a resposta possui um código de status 200
                        if response.status_code == 200:
                            # Adiciona a resposta à lista de resultados
                            resultados_200.append(response)
                        else:
                            print(f"Erro durante a requisição: {response.status_code} - {response.url}")
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

    # Retorna a lista de resultados 200 após todas as requisições serem concluídas
    return resultados_200


# função para extrai o json que contém as infos
async def extrai_json(response):

    soup = BeautifulSoup(response.text, 'html.parser')
    # Encontrar o script que contém __STATE__
    script_tag = soup.find('script', text=lambda text: '__STATE__' in text).text

    # Encontrar a posição onde '__STATE__ =' ocorre no texto
    indice_inicio = script_tag.find('__STATE__ =')


    resultado = script_tag[indice_inicio + len('__STATE__ ='):]
    json_data = json.loads(resultado)
    
    return json_data


# função para extrair as marcas e os respectivos links
async def extrai_links_marcas():
    
    urls_base = {
        'medicamentos-de-a-z': ('https://www.drogariamoderna.com.br/', '/drogaria-moderna---medicamentos?initialMap=productClusterIds&initialQuery=462&map=brand,productclusternames'),
        'amamentacao': ('https://www.drogariamoderna.com.br/', '/amamentacao?initialMap=productClusterIds&initialQuery=455&map=brand,productclusternames'),
        'genericos': ('https://www.drogariamoderna.com.br/', '/drogaria-moderna---medicamentos---genericos?initialMap=productClusterIds&initialQuery=303&map=brand,productclusternames'),
        'dermocosmeticos': ('https://www.drogariamoderna.com.br/', '/drogaria-moderna---dermocosmeticos?initialMap=productClusterIds&initialQuery=290&map=brand,productclusternames'),
        'conveniencia': ('https://www.drogariamoderna.com.br/', '/conveniencia?initialMap=productClusterIds&initialQuery=430&map=brand,productclusternames'),
        'diu': ('https://www.drogariamoderna.com.br/', '/diu?initialMap=productClusterIds&initialQuery=483&map=brand,productclusternames'),
        'higiene-e-beleza': ('https://www.drogariamoderna.com.br/', '/perfumaria?initialMap=productClusterIds&initialQuery=427&map=brand,productclusternames'),
        'cuidados-especiais': ('https://www.drogariamoderna.com.br/', '/drogaria-moderna---cuidados-especiais?initialMap=productClusterIds&initialQuery=289&map=brand,productclusternames')
    }

    dfs = []
    for departamento, url in urls_base.items():
        req = requests.get(f'https://www.drogariamoderna.com.br/{departamento}')
        json_file = await extrai_json(req)

        data = [
            {
                'Marca': valor['value'],
                'Departamento': departamento,
                'Link': url[0] + valor['value'] + url[1]
            }
            for chave, valor in json_file.items()
            if re.match(r'.*\.facets\.3\.values\(\{\}\)\.\d+$', chave)
        ]

        dfs.append(pd.DataFrame(data))

    marcas_departamentos = pd.concat(dfs, ignore_index=True)

    return marcas_departamentos


async def fetch(session, url, max_attempts=4):
    for attempt in range(max_attempts):
        try:
            async with session.get(url) as response:
                return await response.text()
        except Exception as e:
            if attempt < max_attempts - 1:  # i.e. se não for a última tentativa
                await asyncio.sleep(2**attempt)  # espera antes de tentar novamente
            else:
                raise e from None  # a última tentativa falhou, então levanta a exceção

async def junta_urls(df):
    urls = df['Link']
    responses = await fazer_requisicoes(urls)
    links = []

    async def junta_urls_async(response):
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            texto = soup.find('div', class_='vtex-search-result-3-x-totalProducts--layout pv5 ph9 bn-ns bt-s b--muted-5 tc-s tl t-action--small').find('span').text
            resultado = re.search(r'^(\d+)\b', texto)

            if resultado:
                numero_de_produtos = int(resultado.group(1))
                paginas = numero_de_produtos // 15 if (numero_de_produtos % 15) == 0 else (numero_de_produtos // 15) + 1

                for pagina in range(1, paginas + 1):   
                    print(str(response.url) + f'&page={pagina}')
                    links.append(str(response.url) + f'&page={pagina}')
        except:
            links.append(str(response.url))
    
    await asyncio.gather(*[junta_urls_async(response) for response in responses])
    return links


# função para extrai o json que contém as infos
async def extrai_json(response):

    soup = BeautifulSoup(response.text, 'html.parser')
    # Encontrar o script que contém __STATE__
    script_tag = soup.find('script', text=lambda text: '__STATE__' in text).text

    # Encontrar a posição onde '__STATE__ =' ocorre no texto
    indice_inicio = script_tag.find('__STATE__ =')


    resultado = script_tag[indice_inicio + len('__STATE__ ='):]
    json_data = json.loads(resultado)
    
    return json_data


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

    json_path = 'drogaria_moderna/base_oficial_drogaria_moderna.json'

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
                'Farmácia': 'Farma Conde',
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