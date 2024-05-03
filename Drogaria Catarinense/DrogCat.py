import requests
import xml.etree.ElementTree as ET
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

def extrair_informacoes_produto(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extrair informações
        titulo = soup.find('div', class_='productName').text.strip()
        codigo = soup.find('div', class_='productReference').text.strip()
        marca = soup.find('a', class_='brand').text.strip()
        preco_sem_desconto = soup.find('strong', class_='skuListPrice').text.strip()
        preco_com_desconto = soup.find('strong', class_='skuBestPrice').text.strip()
        descricao = soup.find('div', class_='productDescription').text.strip()
        
        return {
            'Título': titulo,
            'Código': codigo,
            'Marca': marca,
            'Preço Sem Desconto': preco_sem_desconto,
            'Preço Com Desconto': preco_com_desconto,
            'Descrição': descricao
        }
    except Exception as e:
        print(f"Erro ao extrair informações do produto da URL {url}: {e}")
        return None

def extrair_urls_do_sitemap(url_sitemap):
    try:
        response = requests.get(url_sitemap)
        response.raise_for_status()  
        xml_string = response.text
        urls = []
        root = ET.fromstring(xml_string)
        loc_elements = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        for loc_element in loc_elements:
            urls.append(loc_element.text)
        return urls
    except Exception as e:
        print(f"Erro ao extrair URLs do sitemap {url_sitemap}: {e}")
        return []

# Lista de URLs dos sitemaps em ordem crescente
urls_sitemaps = [
    "https://www.drogariacatarinense.com.br/sitemap/product-1.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-2.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-3.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-4.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-5.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-6.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-7.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-8.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-9.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-10.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-11.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-12.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-13.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-14.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-15.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-16.xml",
    "https://www.drogariacatarinense.com.br/sitemap/product-17.xml"
]

# Extrair URLs de todos os sitemaps
urls_combinadas = []
for url_sitemap in urls_sitemaps:
    urls_combinadas.extend(extrair_urls_do_sitemap(url_sitemap))

# Extrair informações dos produtos das URLs coletadas
produtos = []
with ThreadPoolExecutor(max_workers=8) as executor:
    resultados = executor.map(extrair_informacoes_produto, urls_combinadas)
    for resultado in resultados:
        if resultado:
            produtos.append(resultado)

# Criar DataFrame do Pandas com os dados dos produtos
df_produtos = pd.DataFrame(produtos)

# Salvar os dados do DataFrame em um arquivo CSV
nome_arquivo_csv = "produtos.csv"
df_produtos.to_csv(nome_arquivo_csv, index=False)
print(f"As informações dos produtos foram salvas em {nome_arquivo_csv}")
