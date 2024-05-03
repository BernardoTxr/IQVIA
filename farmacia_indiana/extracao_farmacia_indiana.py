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
                    'beleza',
                    'mercado',
                    'mamae-e-bebe',
                    'saude',
                    'higiene-pessoal',
                    'eletronicos']

    # lista para armazenar as marcas e os departamentos dos produtos
    marcas_lista = list()
    total_produtos = list()
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
            await page.goto(f"https://www.farmaciaindiana.com.br/{departamento}")

            # Criar um localizador para o elemento
            container_locator = page.locator('xpath=//*[contains(concat( " ", @class, " " ), concat( " ", "vtex-search-result-3-x-filter__container--brand", " " ))]//*[contains(concat( " ", @class, " " ), concat( " ", "overflow-y-auto", " " ))]')

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
            marcas_variavel = [unidecode(re.sub(r'\s?\(\d+\)', '', marca.text).lower().replace(' ', '-').replace('s/a', 's-a').replace('s.a', 's-a')) for marca in container_marcas.find_all('label', class_='vtex-checkbox__label w-100 c-on-base pointer')]
            total_produtos.extend([re.sub(r'\D+', '', marca.text) for marca in container_marcas.find_all('label', class_='vtex-checkbox__label w-100 c-on-base pointer')])
            print(total_produtos)
            marcas_lista.extend(marcas_variavel)  

            departamentos_lista.extend([departamento for _ in range(len(marcas_variavel))])
            print(f'Foram extraídas {len(marcas_variavel)} marcas do departamento {departamento}.')

    df = pd.DataFrame({'Marcas': marcas_lista, 'Total_Produtos': total_produtos, 'Departamentos': departamentos_lista})
    df['urls'] = 'https://www.farmaciaindiana.com.br/' + df['Departamentos'] + '/' + df['Marcas'] + '?initialMap=c&initialQuery=' + df['Departamentos'] + '&map=category-1,brand'
    df = df.dropna(subset=['urls'])

    return df.to_csv('marcas_departamentos.csv')


async def fazer_requisicoes(urls, tentativas=4):
    # Lista para armazenar os resultados das requisições
    resultados_totais = []

    # Cria um cliente HTTP assíncrono usando httpx
    async with httpx.AsyncClient() as client:
        # Cria um semáforo para limitar o número de requisições simultâneas
        sem = asyncio.Semaphore(150)  # Limite o número de requisições simultâneas

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



# juntando todas as urls
async def junta_urls(df):
    
    responses = await fazer_requisicoes(df['urls'])
    urls = list()

    for response in responses:
        url = response.url 
        total_produtos = list(df[df['urls'] == url]['Total_Produtos'])[0]
        urls_variavel = await extrai_urls(response, total_produtos)
        urls.extend(urls_variavel)
    
    return urls


# função para extrair as urls por departamento e marca
async def extrai_urls(response, total_produtos):
    
    paginas = await extrai_paginas(total_produtos)
        
    return [str(response.url) + f'&page={pagina}' for pagina in range(1, paginas + 1)]


# função para extrair o número de páginas que serão percorridas
async def extrai_paginas(total_produtos):


    return total_produtos // 16 if (total_produtos % 16) == 0 else (total_produtos // 16) + 1


# função para extrai o json que contém as infos (ok) 
async def extrai_json(response):

    # criando objeto soup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontrar o arquivo json txt
    json_txt = soup.find('template', {'data-varname': '__STATE__'}).find('script').text

    # transformando em json
    json_data = json.loads(json_txt)
    
    return json_data


# chaves para achar as informações dos produtos (ok) 
async def extrai_itens(json_data):
    # Define o padrão de regex
    padrao = re.compile(r'Product:sp-\d+.items\({"filter":"ALL"}\).0$')

    # Retorna uma lista com as chaves que correspondem ao padrão
    return [item for item in json_data.keys() if padrao.match(item)]


# extrai o nome e os códigos sku e ean (ok)
async def extrai_nome_sku_ean(json_data, num_produto, lista_filtrada):

    return json_data[lista_filtrada[num_produto]]['name'] or np.nan, json_data[lista_filtrada[num_produto]]['itemId'] or np.nan, json_data[lista_filtrada[num_produto]]['ean'] or np.nan


# extrai o fabricante (ok)
async def extrai_marca(json_data, lista_filtrada, num_produto):

    return json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '')]['brand'] or np.nan


# verifica a existência de desconto de laboratório (ok)
async def desconto_laboratorio(json_data, lista_filtrada, num_produto):

    try: 
        for propertie in range(0, 20):
            if json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '') + f'.properties.{propertie}']['name'] == 'PBM':
                return True
            
    except:
        return False


# extrai fabricante (ok)
async def extrai_fabricante(json_data, lista_filtrada, num_produto):
    
    try:
        return json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '') + '.properties.1']['values']['json'][0] if json_data[lista_filtrada[num_produto].replace('.items({"filter":"ALL"}).0', '') + '.properties.1']['values'] is not None else np.nan
    
    except:
        return np.nan


# (ok)
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

    # verifica o desconto de laboratório
    desconto_lab = await desconto_laboratorio(json_data, lista_filtrada, num_produto)

    # Busca os preços no dicionário offer
    # Se a chave não existir, retorna 0 como valor padrão
    preco_sem_desconto = float(offer.get('ListPrice', 0)) or np.nan
    preco_com_desconto = float(offer.get('Price', 0)) or np.nan
    porcentagem_desconto = np.round(1 - (preco_com_desconto / preco_sem_desconto), 2)  if 0 <= np.round(1 - (preco_com_desconto / preco_sem_desconto), 2) < 1 else np.nan
    
    # Define o tipo de desconto
    if leve_mais_pague_menos and desconto_lab is False:
        tipo_de_desconto = 'Leve + pague -'
    
    elif leve_mais_pague_menos and desconto_lab:
        tipo_de_desconto = 'Leve + pague - / Desconto de Laboratório'
    
    elif desconto_lab and leve_mais_pague_menos is False:
        tipo_de_desconto = 'Desconto de Laboratório'

    elif 0 < porcentagem_desconto < 1:
        tipo_de_desconto = 'Normal'
        
    else:
        tipo_de_desconto = 'Ausente'


    # Retorna os preços e o tipo de desconto
    return preco_sem_desconto, preco_com_desconto, porcentagem_desconto, tipo_de_desconto


# (ok)
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


async def extrai_informacoes(response):
    json_data = await extrai_json(response)
    lista_filtrada = await extrai_itens(json_data)

    for num_produto in range(len(lista_filtrada)):
        try:
            nome_sku_ean = await extrai_nome_sku_ean(json_data, num_produto, lista_filtrada)
            preco_desconto = await extrai_preco_desconto(json_data, num_produto, lista_filtrada)
            fabricante = await extrai_fabricante(json_data, lista_filtrada, num_produto)
            descricao = await extrai_descricao(json_data, lista_filtrada, num_produto)
            marca = await extrai_marca(json_data, lista_filtrada, num_produto)

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

# função principal 
async def main():

    # Caminho para o arquivo CSV
    csv_path = 'farmacia_indiana/base_oficial_farmacia_indiana.json'

    # # extração dos links base
    # df = await extrai_playwright()
    df = pd.read_csv("C:/Users/rafab/area de trabalho/Desktop/IQVIA/marcas_departamentos.csv")
    
    # extração das páginas dos departamentos
    urls = await junta_urls(df)
    print(f'Juntamos {len(urls)} urls')

    # realizando as requisições
    responses = await fazer_requisicoes(urls)
    print(f'Trabalharemos com {len(responses)} urls')

    # extraindo as informações necessárias para cada requisição 
    for response in responses:
        informacoes_generator = extrai_informacoes(response)
        async for json_str in informacoes_generator:
            # Escreve o JSON no arquivo
            with open(csv_path, 'a') as f:
                f.write(json_str + '\n')


if __name__ == "__main__":
    asyncio.run(main())