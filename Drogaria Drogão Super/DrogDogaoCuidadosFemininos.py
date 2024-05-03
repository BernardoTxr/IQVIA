from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import time
from bs4 import BeautifulSoup
import re

def extrair_informacoes_produto(driver, url):
    try:
        driver.get(url)
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        titulo = soup.find('h1', class_='nome').text.strip() if soup.find('h1', class_='nome') else ''
        codigo = soup.find('p', class_='codigo').text.strip() if soup.find('p', class_='codigo') else ''
        preco_sem_desconto = soup.find('p', class_='precoDe').find('span', id='PrecoProdutoDetalhe').text.strip() if soup.find('p', class_='precoDe') else ''
        preco_com_desconto = soup.find('p', class_='precoPor').find('span').text.strip() if soup.find('p', class_='precoPor') else ''
        promocao = soup.find('p', class_='economize').find('span').text.strip() if soup.find('p', class_='economize') else ''
        descricao = soup.find('div', class_='aba1 descResumida aba-scroll').find('p').text.strip()
        marca = soup.find('a', class_='logomarca').text.strip() if soup.find('a', class_='logomarca') else ''

        return {
            'Título': titulo,
            'Código': codigo,
            'Preço com Desconto': preco_com_desconto,
            'Preço Sem Desconto': preco_sem_desconto,
            'Promoção': promocao,
            'Descrição': descricao,
            'Marca': marca
        }
    except Exception as e:
        return None

def gerar_urls_paginas(url_base, num_paginas):
    urls = []
    for page in range(1, num_paginas + 1):
        url = url_base.replace("idPage=1", f"idPage={page}")
        urls.append(url)
    return urls

def extrair_links_produtos(urls):
    links_produtos = []

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    # Aguarda a página carregar completamente
    time.sleep(3)

    for url in urls:
        try:
            driver.get(url)
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Encontrando todos os links de produtos na página
            links = soup.find_all('a', href=True)

            # Filtrando apenas os links de produtos e adicionando à lista
            for link in links:
                href = link['href']
                if '/produto/' in href:
                    links_produtos.append(href)
        except Exception as e:
            print(f"Erro ao extrair links da página {url}: {e}")

    driver.quit()

    return links_produtos


url_base_medicamentos = "https://www.drogaosuper.com.br/categoria.asp?idcategoria=5326&nivel=05&categoria=CUIDADOS%20FEMININOS&viewType=M&nrRows=18&idPage=1&ordem=V"


urls_paginas_medicamentos = gerar_urls_paginas(url_base_medicamentos, 58)


links_produtos_medicamentos = extrair_links_produtos(urls_paginas_medicamentos)


produtos_medicamentos = []


service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

for link_produto in links_produtos_medicamentos:
    info_produto = extrair_informacoes_produto(driver, link_produto)
    if info_produto:
        produtos_medicamentos.append(info_produto)

driver.quit()


df_produtos_medicamentos = pd.DataFrame(produtos_medicamentos)


df_produtos_medicamentos.drop_duplicates(inplace=True)


nome_arquivo_csv_medicamentos = "produtos_cuidados_femininos.csv"
df_produtos_medicamentos.to_csv(nome_arquivo_csv_medicamentos, index=False)