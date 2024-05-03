# Suppress DeprecationWarning
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import requests
import re
import json
import os
import time
import math


def transform_links(categories_brands):
    links = []
    # Iterate over categories
    for category, brands in categories_brands.items():
        category_lower = category.lower()
        brands_count = len(brands)

        # Create URLs for the first 10 brands (1 brand per URL)
        for i in range(min(10, brands_count)):
            brand = brands[i]
            url = f'https://www.farmadelivery.com.br/{category_lower}/{brand}?initialMap=c&initialQuery={category_lower}&map=category-1,brand&page=1'
            links.append(url)

        # Create URLs for the next 7 sets of 3 brands each
        for i in range(10, min(31, brands_count), 3):
            brand1 = brands[i]
            brand2 = brands[i + 1] if i + 1 < brands_count else ''
            brand3 = brands[i + 2] if i + 2 < brands_count else ''
            url = f'https://www.farmadelivery.com.br/{category_lower}/{brand1}/{brand2}/{brand3}?initialMap=c&initialQuery={category_lower}&map=category-1,{'brand,'*2}brand&page=1'
            links.append(url)

        # Create URLs for the next 20 sets of 6 brands each
        for i in range(31, min(101, brands_count), 6):
            brands_subset = brands[i:i+6]
            url = f'https://www.farmadelivery.com.br/{category_lower}/{"/".join(brands_subset)}?initialMap=c&initialQuery={category_lower}&map=category-1,{'brand,'*5}brand&page=1'
            links.append(url)

        # Create URLs for the remaining brands
        remaining_brands = brands[101:]
        for i in range(0, len(remaining_brands), 12):
            brands_subset = remaining_brands[i:i+12]
            url = f'https://www.farmadelivery.com.br/{category_lower}/{"/".join(brands_subset)}?initialMap=c&initialQuery={category_lower}&map=category-1,{'brand,'*11}brand&page=1'
            links.append(url)

    return links


def get_all_pages_per_link(url):
    headers = {'Accept': '*/*', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for retry in range(5):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                number_products_available = re.search(r'"recordsFiltered":(\d+),', str(soup)).group(1)
                total_pages = (int(number_products_available) + 11) // 12
                link = url
                all_links = []
                for i in range(total_pages):
                    link = link.replace(f'&page={i}', f'&page={i+1}')
                    all_links.append(link)
                return all_links

            else:
                print(f"Failed to fetch number of pages for URL: {url}. Retrying... (Attempt {retry + 1}/{5})")
                time.sleep(1)  # Wait before retrying
        except Exception as e:
            print(f"An error occurred while fetching number of pages for URL: {url}: {e}. Retrying... (Attempt {retry + 1}/{5})")
            time.sleep(1)  # Wait before retrying
    
    print(f"Maximum retries reached for URL: {url}. Failed to fetch number of pages.")
    return []


def get_ean(json_data, id):
    try:
        ean = json_data[f'Product:sp-{id}.items({{"filter":"ALL"}}).0']['ean']
        return ean
    except:
        return None
    
def get_product(json_data, id):
    try:
        product = json_data[f'Product:sp-{id}']['productName']
        return product
    except:
        return None
    

def get_brand(json_data, id):
    try:
        brand = json_data[f'Product:sp-{id}']['brand']
        return brand
    except:
        return None
    

def get_price_without_discount(json_data, id):
    try:
        price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL"}}).0.sellers.1.commertialOffer']['ListPrice']
        return price
    except:
        try:
            price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL"}}).0.sellers.0.commertialOffer']['ListPrice']
        except:
            return None
    

def get_price_with_discount(json_data, id):
    try:
        price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL"}}).0.sellers.1.commertialOffer']['Price']
        return price
    except:
        try:
            price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL"}}).0.sellers.0.commertialOffer']['Price']
        except:
            return None
    

def get_description(json_data, id):
    try:
        description = json_data[f'Product:sp-{id}']['description']
        return description
    except:
        return None
    

def get_products_infos(url):
    headers = {'Accept': '*/*', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    for retry in range(5):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                state = soup.find('template', {'data-type': 'json', 'data-varname': '__STATE__'}).find('script')
                json_text = state.get_text()
                json_data = json.loads(json_text)

                lista = re.findall(r'Product:sp-(\d+)', str(json_data.keys()))
                ids = set(lista)
                
                #with open('Farma Delivery/json_problem.json', 'w') as json_file:
                #    json.dump(json_data, json_file, indent=4)

                page_products = []
                for id in ids:
                    product_dict = {}
                    product_dict['ean'] = get_ean(json_data, id)
                    product_dict['produto'] = get_product(json_data, id)
                    product_dict['marca'] = get_brand(json_data, id)
                    product_dict['farmacia'] = 'Farma Delivery'
                    product_dict['preco com desconto'] = get_price_with_discount(json_data, id)
                    product_dict['preco sem desconto'] = get_price_without_discount(json_data, id)
                    product_dict['descricao'] = get_description(json_data, id)
                    page_products.append(product_dict)
                return page_products
            else:
                print(f"Failed to fetch product infos for URL: {url}. Retrying... (Attempt {retry + 1}/{5})")
                time.sleep(1) 
        except Exception as e:
            print(f"An error occurred while fetching product infos for URL: {url}: {e}. Retrying... (Attempt {retry + 1}/{5})")
            time.sleep(1)  
        
    print(f"Maximum retries reached for URL: {url}. Failed to fetch product infos.")
    #with open('Farma Delivery/page_html_error.txt', 'w', encoding='utf-8') as file:
    #        file.write(response.text)
    product_error = {
        'ean': None,
        'produto': None,
        'marca': None,
        'farmacia': None,
        'preco com desconto': None,
        'preco sem desconto': None,
        'descricao': None
    }
    return product_error


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
    inicio = time.time()
    print('Extraction started')

    MAX_WORKERS = os.cpu_count()

    with open('Farma Delivery/categories_brands.json', 'r') as file:
        categories_brands = json.load(file)

    # Creates the url category+brands
    links = transform_links(categories_brands)

    all_links = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_all_pages_per_link, url) for url in links]
        # Get results from the completed tasks
        for future in as_completed(futures):
            pages_links = future.result()
            all_links.append(pages_links)
        
    all_links = [link for page_links in all_links for link in page_links]

    print(f'Há {len(all_links)} links')
    all_products_informations = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_products_infos, url) for url in all_links]
        for future in as_completed(futures):
            product_infos = future.result()
            all_products_informations.append(product_infos)

    all_products_informations = [product for page in all_products_informations for product in page]
    
    df = creates_df(all_products_informations)
    df.to_csv('Farma Delivery/arquivo_final.csv', index=False)

    final = time.time()
    print(f'tempo de extração foi de: {(final - inicio)/60 :.2f} minutos')

    '''
    Observação importante:
    Não está dando para pegar os preços de alguns produtos
    porque no arquivo json de alguns produtos as chaves usadas para pegar os preços estão escritas como
    $Product:sp-1981.items({"\filter\":"\ALL\"}).0.sellers.1.commertialOffer'
    com esses backslashes

    Eu não consegui resolver esse problema e tentei:
    formatar usando raw string -> recebia \\ ou \\\\ no lugar de apenas uma
    formatar usando f string -> erro parecido
    formatar simplesmente com a string como deve ser, porém interpreta \f como um código estranho
    '''


if __name__ == '__main__':
    main()