# importando bibliotecas necessárias
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import numpy as np
import pandas as pd
import re
from bs4 import BeautifulSoup
import json 
from html import unescape
from functools import lru_cache
from selenium.webdriver.chrome.options import Options

# Define um decorador de cache para o método find_all
@lru_cache(maxsize=None)
def find_all_cached(soup, tag, class_=None):
    # Esta função irá armazenar em cache os resultados do método find_all
    return soup.find_all(tag, class_)

def paginas_departamento(departamento, driver):
    # Navega para a página do departamento
    driver.get(departamento)
    
    # Obtém o HTML da página
    html_da_pagina = driver.page_source
    
    # Faz o parse do HTML com BeautifulSoup
    soup = BeautifulSoup(html_da_pagina, 'html.parser')

    # Busca todos os itens de paginação
    pagination_items = find_all_cached(soup, 'li', 'Paginationstyles__Item-sc-1am2zyy-2 GkKTY')
    
    # Retorna o número da última página se existir, caso contrário retorna 0
    return int(pagination_items[-1].text) if pagination_items else 0


def extrai_preco_desconto(num_produto, price_boxes):
    
    try:
        # Dentro do cartão do produto, busca a caixa de preço
        price_box = price_boxes[num_produto]

        # Verifica se o produto tem desconto
        price_from = price_box.find('div', class_='price price-from')
        tem_desconto = price_from is not None

        # Se o produto tem desconto, extrai o preço sem desconto, o preço com desconto e a porcentagem de desconto
        if tem_desconto:
            preco_sem_desconto = float(re.search(r'R\$\s*([\d,]+)', price_from.text.strip()).group(0).replace('R$', '').replace(',', '.'))
            preco_com_desconto = float(re.search(r'R\$\s*([\d,]+)', price_box.find('div', class_='price-final').text.strip()).group(0).replace('R$', '').replace(',', '.'))
            porcentagem_desconto = np.round(1 - (preco_com_desconto / preco_sem_desconto), 2)

        else:
            # Se o produto não tem desconto, extrai apenas o preço final
            preco_sem_desconto = float(re.search(r'R\$\s*([\d,]+)', price_box.find('div', class_='price-final').text.strip()).group(0).replace('R$', '').replace(',', '.'))
            preco_com_desconto = np.nan
            porcentagem_desconto = 'Ausente'
        
    except Exception as e:
        print(f"Erro inesperado: {e}")

    # Retorna o preço sem desconto, o preço com desconto, a porcentagem de desconto e o tipo de desconto
    return preco_sem_desconto, preco_com_desconto, porcentagem_desconto

# extrai código sku e o nome do produto
def extrai_sku_name(page_json_file, num_produto):

    return unescape(page_json_file[num_produto]['sku']) or np.nan, unescape(page_json_file[num_produto]['name']) or np.nan


# extrai marca, fabricante, código ean e descrição 
def extrai_marca_fabricante_ean_descricao(page_json_file, num_produto):

    marca = unescape(page_json_file[num_produto]['custom_attributes'][17]['value'][0]['label']) if page_json_file[num_produto]['custom_attributes'][17]['value'][0]['label'] is not None else np.nan
    fabricante =  unescape(page_json_file[num_produto]['custom_attributes'][16]['value'][0]['label']) if page_json_file[num_produto]['custom_attributes'][16]['value'][0]['label'] is not None else np.nan
    ean = page_json_file[num_produto]['custom_attributes'][-1]['value_string'][0] or np.nan
    descricao = BeautifulSoup(unescape(' '.join(page_json_file[num_produto]['custom_attributes'][1]['value_string'])), 'html.parser').get_text(separator=' ', strip=True) if page_json_file[num_produto]['custom_attributes'][1]['value_string'] is not None else np.nan

    return marca, fabricante, ean, descricao


def main():

    # Departamentos do site
    departamentos = ['https://www.drogaraia.com.br/cabelo.html',
                    'https://www.drogaraia.com.br/higiene-pessoal.html']


    json_path = 'droga_raia/base_oficial_droga_raia_3.json'
    

    options = webdriver.ChromeOptions()
    options.add_argument('--disable-dev-shm-usage')
    #options.add_argument('--headless')

    driver = webdriver.Chrome(options=options)

    for departamento in departamentos:
        for pagina in range(1, paginas_departamento(departamento, driver) + 1):
            # Abrir a página desejada
            driver.get(departamento + f'?page={pagina}')

            try:
                # Definir um tempo máximo de espera 
                tempo_maximo_espera = 20

                # Esperar até que o elemento específico seja visível
                elemento = WebDriverWait(driver, tempo_maximo_espera).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'price'))
                )
            except:
                continue

            # Obter o conteúdo HTML da página
            html_da_pagina = driver.page_source

            # Passar o conteúdo HTML para BeautifulSoup
            soup = BeautifulSoup(html_da_pagina, 'html.parser')

            # informações em json
            page_json_file = json.loads(soup.find('script', {'id': '__NEXT_DATA__'}).text)
            page_json_file = page_json_file['props']['pageProps']['pageData']['items']

            # Buscar a lista de elementos de preço uma vez
            price_boxes = soup.find_all('div', class_='ProductGridstyles__ProductGridStyles-sc-1wbcxrt-0')[0].find_all('div', class_='ProductCardstyles__ProductCardStyle-iu9am6-6')
            # extraindo as informações
            for num_produto in range(0, len(price_boxes)):

                try:
                    preco_desconto = extrai_preco_desconto(num_produto, price_boxes)
                    marca_fabricante = extrai_marca_fabricante_ean_descricao(page_json_file, num_produto)
                    sku_nome = extrai_sku_name(page_json_file, num_produto)

                    informacoes = {
                        'EAN': marca_fabricante[2],
                        'SKU': sku_nome[0],
                        'Nome': sku_nome[1],
                        'Marca': marca_fabricante[0],
                        'Descrição': marca_fabricante[3],
                        'Farmácia': 'Droga Raia',
                        'Preço_sem_desconto': preco_desconto[0],
                        'Preço_com_desconto': preco_desconto[1],
                        'Porcentagem_desconto': preco_desconto[2],
                        'Cidade': 'São Paulo',
                        'Região': 'Sudeste'
                    }

                    # Escreve o JSON no arquivo
                    with open(json_path, 'a') as f:
                        json.dump(informacoes, f)
                        f.write('\n')
                
                except Exception as e:
                    print(f"Erro inesperado: {e}")



if __name__ == "__main__":
    main()
