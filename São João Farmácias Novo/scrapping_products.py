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


def get_json(text):
    state = re.search(r'__STATE__ = (.*)"SearchMetadata"}}', text)
    json_text = state.group(1) + r'"final do json"}}'
    json_data = json.loads(json_text)
    return json_data


def get_ean(json_data, id):
    try:
        ean = json_data[f'Product:sp-{id}.items({{"filter":"ALL_AVAILABLE"}}).0']['ean']
        ean = int(ean)
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
        price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL_AVAILABLE"}}).0.sellers.0.commertialOffer']['ListPrice']
        return price
    except:
        try:
            price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL_AVAILABLE"}}).0.sellers.1.commertialOffer']['ListPrice']
        except:
            return None
    

def get_price_with_discount(json_data, id):
    try:
        price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL_AVAILABLE"}}).0.sellers.0.commertialOffer']['Price']
        return price
    except:
        try:
            price = json_data[f'$Product:sp-{id}.items({{"filter":"ALL_AVAILABLE"}}).0.sellers.1.commertialOffer']['Price']
        except:
            return None
    

def get_description(json_data, id):
    try:
        description = json_data[f'Product:sp-{id}']['description']
        return description
    except:
        return None
    

def get_products_infos(url, max_retry=10):
    headers = {
        'authority': 'www.saojoaofarmacias.com.br',
        'accept': '*/*',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'if-none-match': '624CDF92BB674EDAAF34D2BC6E267B82',
        'sec-ch-ua': '"Opera GX";v="105", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    for retry in range(max_retry):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = get_json(response.text)

                lista = re.findall(r'Product:sp-(\d+)', str(json_data.keys()))
                ids = set(lista)

                page_products = []
                for id in ids:
                    product_dict = {}
                    product_dict['ean'] = get_ean(json_data, id)
                    product_dict['produto'] = get_product(json_data, id)
                    product_dict['marca'] = get_brand(json_data, id)
                    product_dict['farmacia'] = 'São João Farmácias'
                    product_dict['preco com desconto'] = get_price_with_discount(json_data, id)
                    product_dict['preco sem desconto'] = get_price_without_discount(json_data, id)
                    product_dict['descricao'] = get_description(json_data, id)
                    page_products.append(product_dict)
                return page_products
            else:
                print(f"Failed to fetch product infos for URL: {url}. Retrying... (Attempt {retry + 1}/{max_retry})")
                time.sleep(1) 

        except requests.exceptions.ConnectionError as e:
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            time.sleep(1)

        except Exception as e:
            print(f"An error occurred while fetching product infos for URL: {url}: {e}. Retrying... (Attempt {retry + 1}/{max_retry})")
            time.sleep(1)  
        
    print(f"Maximum retries reached for URL: {url}. Failed to fetch product infos.")

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


def transform_links(categories_brands):
    links = []
    for category, brands_and_numbers in categories_brands.items():
        print(f"Category: {category}")
        for brand_number in brands_and_numbers:
            try:
                brand, number = brand_number.split("###")
            except:
                print(brand_number)
            total_pages = (int(number) + 23) // 24
            
            if total_pages > 50:
                url = f'https://www.saojoaofarmacias.com.br/{category}/{brand}?initialMap=c&initialQuery=medicamentos&map=category-1,category-2&page=1'
                url_temp = url
                for i in range(50):
                    url_temp = url_temp.replace(f'&page={i}', f'&page={i+1}')
                    links.append(url_temp)

            else:
                url = f'https://www.saojoaofarmacias.com.br/{category}/{brand}?initialMap=c&initialQuery=medicamentos&map=category-1,category-2&page=1'
                url_temp = url
                for i in range(total_pages):
                    url_temp = url_temp.replace(f'&page={i}', f'&page={i+1}')
                    links.append(url_temp)
    return links


def organizar_colunas(dataframe):
    ordem_colunas = ['ean', 'produto', 'marca', 'farmacia', 'preco com desconto', 'preco sem desconto', '% desconto', 'descricao']
    return dataframe[ordem_colunas]


def creates_df(infos):
    def make_int(number):
        try:
            return str(number)[:13]
        except:
            return number

    df = pd.DataFrame(infos)
    df['% desconto'] = (1 - (df['preco com desconto'] / df['preco sem desconto'])) * 100
    df['ean'] = df['ean'].apply(make_int)
    df = organizar_colunas(df)
    df = df[~df.duplicated()]
    return df


def main():
    inicio = time.time()
    print('Extraction started')

    MAX_WORKERS = os.cpu_count()

    with open('São João Farmácias Novo/categories_brands.json', 'r') as file:
        categories_brands = json.load(file)

    # Creates the url category+brands
    links = transform_links(categories_brands)

    print(f'Há {len(links)} links')
    all_products_informations = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_products_infos, url) for url in links]
        for future in as_completed(futures):
            product_infos = future.result()
            all_products_informations.append(product_infos)

    all_products_informations = [product for page in all_products_informations for product in page]
    
    df = creates_df(all_products_informations)
    df.to_csv('São João Farmácias Novo/arquivo_final.csv', index=False)

    final = time.time()
    print(f'tempo de extração foi de: {(final - inicio)/60 :.2f} minutos')


if __name__ == '__main__':
    main()