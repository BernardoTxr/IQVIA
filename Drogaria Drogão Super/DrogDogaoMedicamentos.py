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


def extrair_links_produtos(url, numero_de_paginas):
    links_produtos = []

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    driver.get(url)

    # Aguarda a página carregar completamente
    time.sleep(4)

    for pagina in range(1, numero_de_paginas + 1):
        try:
            url_pagina = f"{url}&idPage={pagina}"

            driver.get(url_pagina)
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
            print(f"Erro ao extrair links da página {pagina}: {e}")

    return links_produtos


# URL da categoria de Medicamentos
url_categoria_medicamentos = "https://www.drogaosuper.com.br/categoria.asp?idcategoria=5166&nivel=02&categoria=MEDICAMENTOS&viewType=M&nrRows=18&ordem=V"

# Número total de páginas a serem percorridas na categoria de Medicamentos
numero_total_paginas_medicamentos = 174

# Extrair links de produtos da categoria de Medicamentos
links_produtos_medicamentos = extrair_links_produtos(url_categoria_medicamentos, numero_total_paginas_medicamentos)

# Criar uma lista para armazenar os produtos da categoria de Medicamentos
produtos_medicamentos = []

# Configurar o driver do Chrome
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# Extrair informações de cada produto da categoria de Medicamentos
for link_produto in links_produtos_medicamentos:
    info_produto = extrair_informacoes_produto(driver, link_produto)
    if info_produto:
        produtos_medicamentos.append(info_produto)

driver.quit()

# Convertendo a lista de produtos de Medicamentos em um DataFrame pandas
df_produtos_medicamentos = pd.DataFrame(produtos_medicamentos)

# Removendo linhas duplicadas
df_produtos_medicamentos.drop_duplicates(inplace=True)

# Salvando as informações dos produtos de Medicamentos em um arquivo CSV
nome_arquivo_csv_medicamentos = "produtos_medicamentos.csv"
df_produtos_medicamentos.to_csv(nome_arquivo_csv_medicamentos, index=False)
