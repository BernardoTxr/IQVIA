# Suppress DeprecationWarning
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import re
import json
import time

def access_site(url, max_attempts=5):
    headers = {'Accept': '*/*', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for retry in range(max_attempts):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response
        except Exception as e:
                print(f"An error occurred while accessing URL: {url}: {e}. Retrying... (Attempt {retry + 1}/{5})")
                time.sleep(1)

def get_products_sitemap(url):
    response = access_site(url)
    soup = BeautifulSoup(response.text, 'xml')
    products_sitemap = [loc.text for loc in soup.find_all('loc') if '/product' in loc.text]
    return products_sitemap


def get_all_products_links(sitemap):
    response = access_site(sitemap)
    soup = BeautifulSoup(response.text, 'xml')
    locs = soup.find_all('loc')
    urls_site = []
    for loc in locs:
        urls_site.append(loc.text)
    return urls_site


def get_products_urls(products_sitemap, workers):
    all_urls = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(get_all_products_links, sitemap) for sitemap in products_sitemap]
        for future in as_completed(futures):
            pages_links = future.result()
            all_urls.append(pages_links)

    all_urls = [url for page_url in all_urls for url in page_url]
    return all_urls


def get_products_informations(products_urls, workers):
    all_products_informations = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(product_info, url) for url in products_urls]
        # Get results from the completed tasks
        for future in as_completed(futures):
            product_relevant_infos = future.result()
            all_products_informations.append(product_relevant_infos)
    return all_products_informations


def get_json_data(soup):
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        if '"@type": "Product"' in script.text:
            json_data = script.text
    return json_data


def product_info(url):               
    product = {}
    try:
        response = access_site(url)
        product_all_infos = get_products_infos(response.text)
        json_data = json.loads(product_all_infos)
        product_id = get_product_id(product_all_infos)
        product_relevant_infos = get_products_relevant_infos(json_data, product_id)
        return product_relevant_infos
    except Exception as e:
        return product


def get_products_infos(html_text):
    state_match = re.search('__STATE__', html_text)
    state_index = state_match.start()

    # Find the first occurrence of <script> after '__STATE__'
    script_match = re.search(r'<script>', html_text[state_index:])
    script_start_index = state_index + script_match.start()

    # Find the first occurrence of </script> after '<script>'
    script_end_match = re.search(r'</script>', html_text[script_start_index:])
    script_end_index = script_start_index + script_end_match.start()

    # Extract the text between <script> and </script> tags
    script_text = html_text[script_start_index + len('<script>'):script_end_index]
    return script_text


def get_product_id(products_text):
    pattern = r'"Product:(.*?)":\{'
    id = re.findall(pattern, products_text)[0]
    return id


def get_products_relevant_infos(json_data, id):   
    name = json_data[f'Product:{id}']['productName']
    description = json_data[f'Product:{id}']['description']
    brand = json_data[f'Product:{id}']['brand']
    ean = json_data[f'Product:{id}.items.0']['ean']
    price_with_discount = json_data[f'$Product:{id}.items.0.sellers.0.commertialOffer']['Price']
    price_without_discount = json_data[f'$Product:{id}.items.0.sellers.0.commertialOffer']['ListPrice']
    product = {
        'ean': ean,
        'produto': name,
        'marca': brand,
        'farmacia': 'Drogaria Coop',
        'preco com desconto': price_with_discount,
        'preco sem desconto': price_without_discount,
        'descricao': description
    }
    return product



def organizar_colunas(dataframe):
    ordem_colunas = ['ean', 'produto', 'marca', 'farmacia', 'preco com desconto', 'preco sem desconto', '% desconto', 'descricao']
    return dataframe[ordem_colunas]


def creates_df(infos):
    df = pd.DataFrame(infos)
    df['% desconto'] = (1 - (df['preco com desconto'] / df['preco sem desconto'])) * 100
    df = organizar_colunas(df)
    df = df[~df.duplicated()]
    return df


def main():
    start_time = time.time()
    print('Extração iniciada')
    MAX_WORKERS = os.cpu_count()

    sitemap = 'https://www.coopdrogaria.com.br/sitemap.xml'

    products_sitemap = get_products_sitemap(sitemap)

    products_urls = get_products_urls(products_sitemap, MAX_WORKERS)

    print(f'Quantidade disponíveis de produtos para extração: {len(products_urls)}')

    products_informations = get_products_informations(products_urls, MAX_WORKERS)

    df = creates_df(products_informations)
    df.to_csv('Drogaria Coop/arquivo_final.csv', index=False)

    end_time = time.time()
    print(f'Extração concluída em {(end_time-start_time)/60 :.2f} minutos')
    print(f'Extração concluída em {(end_time-start_time) :.2f} segundos')
    

if __name__ == '__main__':
    main()