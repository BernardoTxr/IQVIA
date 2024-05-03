import requests
from bs4 import BeautifulSoup
import json   
import pandas as pd
import re 
import numpy as np 
import httpx
import asyncio
import random
from playwright.async_api import async_playwright


async def rolar_ate_o_final(elemento):
    altura_anterior = -1

    while True:
        # Rolar para o final do elemento
        await elemento.evaluate('(element) => { element.scrollTop = element.scrollHeight; }')

        # Aguardar um curto período para garantir que a rolagem ocorreu
        await asyncio.sleep(1)

        # Verificar se a altura não mudou (não há mais espaço para rolar)
        altura_atual = await elemento.evaluate('(element) => element.scrollHeight')
        if altura_atual == altura_anterior:
            break

        # Atualizar a altura anterior para a próxima iteração
        altura_anterior = altura_atual


async def extrai_playwright():
    # departamentos disponíveis
    departamentos = ['medicamentos',
                    'dermocosmeticos',
                    'cuidados-diários',
                    'mamae-e-bebe',
                    'saude-e-bem-estar',
                    'home-care',
                    'conveniencia']

    # lista para armazenar as marcas e os departamentos dos produtos
    marcas_lista = list()
    departamentos_lista = list()

    async with async_playwright() as p:
        # inicializando o navegador
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()

        # # Configurar o timeout padrão para o contexto
        # await context.set_default_timeout(50000)  # Timeout de 50 segundos

        page = await context.new_page()

        # iterando pelos departamentos
        for departamento in departamentos:
            await page.goto(f"https://www.santaluciadrogarias.com.br/{departamento}")

            # Criar um localizador para o elemento
            container_locator = page.locator('xpath=/html/body/div[2]/div/div[1]/div/div[3]/div/div[2]/section/div[2]/div/div[5]/div/div[1]/div/div[2]/div/div/div[3]/div[2]')

            # Rolar até o elemento
            await container_locator.scroll_into_view_if_needed()

            # Rolar completamente até o final do elemento
            await rolar_ate_o_final(container_locator)

            # Extraia o conteúdo HTML da página
            conteudo_html = await page.content()

            # Use Beautiful Soup para analisar o HTML
            soup = BeautifulSoup(conteudo_html, 'html.parser')

            # armazenando as marcas em uma lista
            container_marcas = soup.find('div', class_='vtex-search-result-3-x-filter__container bb b--muted-4 vtex-search-result-3-x-filter__container--brand')
            marcas_variavel = [marca.text.lower().replace(' ', '-') for marca in container_marcas.find_all('label', class_='vtex-checkbox__label w-100 c-on-base pointer')]
            marcas_lista.extend(marcas_variavel)  

            departamentos_lista.extend([departamento for _ in range(len(marcas_variavel))])
            print(f'Foram extraídas {len(marcas_variavel)} marcas do departamento {departamento}.')

    df = pd.DataFrame({'Marcas': marcas_lista, 'Departamentos': departamentos_lista})

    return df


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



# função para extrair o número de páginas que serão percorridas
async def extrai_paginas(soup):

    total_produtos = int(re.findall(r'\d+', soup.find('div', class_='vtex-search-result-3-x-totalProducts--layout pv5 ph9 bn-ns bt-s b--muted-5 tc-s tl t-action--small').text)[0]) if soup.find('div', class_='vtex-search-result-3-x-totalProducts--layout pv5 ph9 bn-ns bt-s b--muted-5 tc-s tl t-action--small') is not None else 10

    return total_produtos // 10 if (total_produtos % 10) == 0 else (total_produtos // 10) + 1


# função para extrair as urls por departamento e marca
async def extrai_urls(response):
    urls = list()
    soup = BeautifulSoup(response.text, 'html.parser')
    paginas = await extrai_paginas(soup)
    for pagina in range(1, paginas + 1):
        try:
          urls.append(soup.find('link', {'rel': 'canonical', 'data-react-helmet': 'true'})['href'] + f'&page={pagina}')
        except:
            continue

    return urls


# juntando todas as urls
async def junta_urls(lista):
    
    responses = await fazer_requisicoes(lista)
    urls = list()

    for response in responses:
        urls_variavel = await extrai_urls(response)
        urls.extend(urls_variavel)
    
    return urls


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


async def extrai_itens(json_data):
    # Define o padrão de regex
    padrao = re.compile(r'Product:sp-\d+.items\({"filter":"ALL"}\).0$')

    # Retorna uma lista com as chaves que correspondem ao padrão
    return [item for item in json_data.keys() if padrao.match(item)]


# extrai o nome e os códigos sku e ean
async def extrai_nome_sku_ean(json_data, num_produto, lista_filtrada):

    return json_data[lista_filtrada[num_produto]]['name'] or np.nan, json_data[lista_filtrada[num_produto]]['itemId'] or np.nan, json_data[lista_filtrada[num_produto]]['ean'] or np.nan


# extrai o fabricante
async def extrai_fabricante(json_data, lista_filtrada, num_produto):

    return json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '')]['brand'] or np.nan


async def extrai_preco_desconto(json_data, num_produto, lista_filtrada):
    # Cria a chave do produto para buscar no json_data
    product_key = f'${lista_filtrada[num_produto]}.sellers.0.commertialOffer'
    
    # Usa o método get para evitar exceções KeyError
    # Se a chave não existir no dicionário, retorna um dicionário vazio
    offer = json_data.get(product_key, {})

    # verifica a existência do desconto leve + pague -
    try:
        leve_mais_pague_menos = json_data[product_key + '.teasers.0']
        leve_mais_pague_menos = True
    except:
        leve_mais_pague_menos = False
    
    # Busca os preços no dicionário offer
    # Se a chave não existir, retorna 0 como valor padrão
    preco_sem_desconto = float(offer.get('ListPrice', 0)) or np.nan
    preco_com_desconto = float(offer.get('Price', 0)) or np.nan
    porcentagem_desconto = np.round(1 - (preco_com_desconto / preco_sem_desconto), 2)  if 0 <= np.round(1 - (preco_com_desconto / preco_sem_desconto), 2) < 1 else np.nan
    
    # Define o tipo de desconto
    if leve_mais_pague_menos:
        tipo_de_desconto = 'Leve + pague -'

    elif 0 < porcentagem_desconto < 1:
        tipo_de_desconto = 'Normal'
        
    else:
        tipo_de_desconto = 'Ausente'


    # Retorna os preços e o tipo de desconto
    return preco_sem_desconto, preco_com_desconto, porcentagem_desconto, tipo_de_desconto


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


async def extrai_marca(response):

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        url = str(soup.find('link', {'rel': 'canonical'})['href'])

        # Padrão de regex para extrair a parte desejada da URL
        padrao = re.compile(r"/([^/]+)\?")

        # Procura por correspondências no URL
        correspondencias = padrao.search(url)

        # Obtém a parte desejada se houver correspondências
        if correspondencias:
            parte_desejada = correspondencias.group(1)

    except:
        parte_desejada = np.nan
    
    return parte_desejada


async def main():

    json_path = 'drogaria_santa_lucia/base_oficial_drogaria_santa_lucia.json'

    # extração dos links base
    df = await extrai_playwright()
    df['Marcas'] = df['Marcas'].apply(lambda x: x.replace('s.a', 's-a') if 's.a' in x else x)
    df['Marcas'] = df['Marcas'].apply(lambda x: x.replace('s/a', 's-a') if 's/a' in x else x)
    df['urls'] = 'https://www.santaluciadrogarias.com.br/' + df['Departamentos'] + '/' + df['Marcas'] + '?initialMap=c&initialQuery=' + df['Departamentos'] + '&map=category-1,brand'  
    df = df.dropna(subset=['urls'])

    # extração das páginas das marcas dos departamentos 
    urls = await junta_urls(list(df['urls']))
    print(f'Justamos {len(urls)} urls')

    # realizando as requisições
    responses = await fazer_requisicoes(urls)
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
            fabricante = await extrai_fabricante(json_data, lista_filtrada, num_produto)
            descricao = await extrai_descricao(json_data, lista_filtrada, num_produto)
            marca = await extrai_marca(response)

            informacoes = {
                'EAN': nome_sku_ean[2],
                'SKU': nome_sku_ean[1],
                'Nome': nome_sku_ean[0],
                'Marca': marca,
                'Fabricante': fabricante,
                'Descrição': descricao,
                'Preço_sem_desconto': preco_desconto[0],
                'Preço_com_desconto': preco_desconto[1],
                'Porcentagem_desconto': preco_desconto[2],
                'Tipo_desconto': preco_desconto[3]
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



if __name__ == "__main__":
    asyncio.run(main())