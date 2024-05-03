# Suppress DeprecationWarning
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncio
import re
import json
import time


async def scroll_container(element):
    previous_height = -1
    while True:
        await element.evaluate('(element) => { element.scrollTop = element.scrollHeight; }')
        await asyncio.sleep(1)
        # Verificar se a altura não mudou (não há mais espaço para rolar)
        current_height = await element.evaluate('(element) => element.scrollHeight')
        if current_height == previous_height:
            break
        # Atualizar a altura anterior para a próxima iteração
        previous_height = current_height


async def access_page_and_roll_container(url, max_retries=8):
    container_css = 'css=.vtex-search-result-3-x-filter__container--brand .overflow-y-auto'
    for attempt in range(max_retries):
        try:
            async with async_playwright() as p:
                await asyncio.sleep(2) if attempt == 2 else None
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(url)
                await asyncio.sleep(1)
                container_locator = page.locator(container_css)
                await container_locator.scroll_into_view_if_needed()
                await scroll_container(container_locator)
                html_content = await page.content()
                await browser.close()
                return html_content
            
        except Exception as e:
            if "net::ERR_CONNECTION_RESET" in str(e):
                print(f"Encountered 'net::ERR_CONNECTION_RESET' error. Retrying... (Attempt {attempt + 1}/{max_retries})")
                continue
            else:
                print(f"Encountered unexpected error: {e}")
                continue
    print("Max retries exceeded. Failed to access the website.")
    return None


async def get_brands(url, category):
    html = await access_page_and_roll_container(url)
    #with open('Farma Delivery/page_html.txt', 'w', encoding='utf-8') as file:
    #        file.write(html)
    #print('Arquivo exportado')
    
    soup = BeautifulSoup(html, 'html.parser')
    brand_container_content = soup.find_all('div', class_='vtex-search-result-3-x-filterContent')[0]
    brand_content = brand_container_content.find_all('div', alt=True)
    brands = []
    for brand_code in brand_content:
        brand = re.search(r'x-filterItem--(.*?) lh-copy', str(brand_code)).group(1)
        brands.append(brand)
    return brands


async def main():
    start_time = time.time()
    # Primeiro buscamos por todas as marcas existentes em cada categoria
    # para na segunda etapa iterar por cada url de busca juntando categoria + marca
    # a fim de ultrapassar a limitacao de 50 paginas e extrair o maximo de produtos possiveis
    categories = ['diabetes',
                  'outlet',
                  'mamaes-e-bebes',
                  'genericos',
                  'dermocosmeticos',
                  'saude-e-bem-estar',
                  'perfumaria',
                  'remedios-e-medicamentos']

    # Aqui cria-se um dicionário onde cada chave é uma categoria e cada valor é uma lista com todas as marcas
    categories_brands = {}
    for category in categories:
        url_category = f'https://www.farmadelivery.com.br/{category}'
        brands_list = await get_brands(url_category, category)
        categories_brands[category] = brands_list

    # Export the dictionary as JSON
    with open('Farma Delivery/categories_brands.json', 'w') as json_file:
        json.dump(categories_brands, json_file, indent=4)

    end_time = time.time()
    print(f'Extração concluída em {(end_time-start_time)/60 :.2f} minutos')
    

if __name__ == '__main__':
    asyncio.run(main())