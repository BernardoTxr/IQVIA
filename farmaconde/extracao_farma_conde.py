import requests
from bs4 import BeautifulSoup
import json   
import pandas as pd
import re 
import numpy as np 
import httpx
import asyncio
import random


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


async def extrai_links_marcas():

    # função para extrair marcas e suas respectivas quantidades de páginas (ok)
    async def extrai_marcas_quantidades(json_file, departamento):
        dicionario = {'Marca': [], 'Departamento': [], 'Paginas': [], 'Link': []}

        for chave, valor in json_file.items():
            if '.facets.4.values({"from":0,"to":10}).' in chave:
                marca = valor['value']
                quantidade_produtos = valor['quantity']
                if quantidade_produtos < 20:
                    paginas = 1
                else:
                    paginas = quantidade_produtos // 20 if (quantidade_produtos % 20) == 0 else (quantidade_produtos // 20) + 1
                dicionario['Marca'].append(marca)
                dicionario['Paginas'].append(paginas)
                dicionario['Departamento'].append(departamento)
                dicionario['Link'].append(f'https://www.farmaconde.com.br/{departamento}/{marca}?initialMap=c&initialQuery={departamento}&map=category-1,brand')

        return dicionario

    # departamentos disponíveis
    departamentos = ['medicamentos',
                    'dermocosmeticos',
                    'mamae-e-bebe',
                    'saude-e-bem-estar',
                    'cuidados-melhor-idade',
                    'beleza',
                    'higiene-e-cuidados-pessoais',
                    'conveniencia']

    marcas_departamentos = pd.DataFrame()
    for departamento in departamentos:
        req = requests.get(f'https://www.farmaconde.com.br/{departamento}')
        json_file = await extrai_json(req)
        df_variavel = pd.DataFrame(await extrai_marcas_quantidades(json_file, departamento))
        marcas_departamentos = pd.concat([marcas_departamentos, df_variavel])

    marcas_departamentos.reset_index(inplace=True)

    return marcas_departamentos


async def junta_urls(df):
    urls = df['Link']
    responses = await fazer_requisicoes(urls)
    links = (str(response.url) + f'&page={pagina}' 
             for response in responses 
             for pagina in range(1, int(df[df['Link'] == response.url]['Paginas'].to_list()[0]) + 1))
    return list(links)



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

    json_path = 'farmaconde/base_oficial_farma_conde.json'

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