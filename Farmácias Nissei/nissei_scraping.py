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
    products_sitemap = [loc.text for loc in soup.find_all('loc') if '/produtos' in loc.text]
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
    all_products = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(product_info, url) for url in products_urls]
        for future in as_completed(futures):
            product = future.result()
            all_products.append(product)
    return all_products


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
        soup = BeautifulSoup(response.text, 'html.parser')
        data_string = get_json_data(soup)

        product['ean'] = get_ean(data_string)
        product['produto'] = get_product(data_string)
        product['marca'] = get_brand(data_string)
        product['preco com desconto'] = get_price_with_discount(data_string)
        #product['preco sem desconto'] = get_price_without_discount(data_dict)
        product['farmacia'] = 'Farmácias Nissei'
        product['descricao'] = get_description(soup)
        return product
    except Exception as e:
        return product


def get_ean(data_string): 
    try:
        return re.search(r'"gtin": "(.*)"', data_string).group(1)
    except:
        None


def get_product(data_string): 
    try:
        return re.search(r'"name": "(.*)"', data_string).group(1)
    except:
        None


def get_brand(data_string): 
    try:
        return re.search(r'"url":(.*)', data_string).group(1).split('/')[-1]
    except:
        None


def get_price_with_discount(data_string): 
    try:
        return float(re.search(r'"price": "(.*)"', data_string).group(1))
    except:
        None

    
def get_price_without_discount(data_string): 
    try:
        return float(re.search(r'"price": "(.*)"', data_string).group(1))
    except:
        None


def get_description(soup): 
    try:
        return soup.find('div', class_='tab-content').find('p').get_text()
    except:
        None


def organizar_colunas(dataframe):
    ordem_colunas = ['ean', 'produto', 'marca', 'farmacia', 'preco com desconto', 'descricao']
    return dataframe[ordem_colunas]


def creates_df(infos):
    df = pd.DataFrame(infos)
    #df['% desconto'] = (1 - (df['preco com desconto'] / df['preco sem desconto'])) * 100
    df = organizar_colunas(df)
    df['marca'] = df['marca'].apply(lambda x: x.split('"')[0])
    df = df[~df.duplicated()]
    return df


def main():
    start_time = time.time()
    print('Extração iniciada')
    MAX_WORKERS = os.cpu_count()

    sitemap = 'https://www.farmaciasnissei.com.br/sitemap.xml'

    products_sitemap = get_products_sitemap(sitemap)

    products_urls = get_products_urls(products_sitemap, MAX_WORKERS)

    print(f'Quantidade disponíveis de produtos para extração: {len(products_urls)}')

    products_informations = get_products_informations(products_urls, MAX_WORKERS)

    df = creates_df(products_informations)
    df.to_csv('Farmácias Nissei/arquivo_final.csv', index=False)

    end_time = time.time()
    print(f'Extração concluída em {(end_time-start_time)/60 :.2f} minutos')
    print(f'Extração concluída em {(end_time-start_time) :.2f} segundos')
    

if __name__ == '__main__':
    main()