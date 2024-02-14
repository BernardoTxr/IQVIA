import re
import pandas as pd
import numpy as np
from playwright.async_api import async_playwright

async def extract_href(page):
    href_list = []
    produtos_desejados = await page.query_selector('.ui.three.doubling.cards.centered')
    produtos = await produtos_desejados.query_selector_all('.ui.image.fluid.attached')
    for produto in produtos:
        href_list.append(await produto.get_attribute('href'))
    return href_list

async def extract_info(page):
    sku_element = await page.query_selector('.product__rating.stackable > div:nth-of-type(1) > .codProduto')
    sku_text = await sku_element.inner_text()
    sku = sku_text[5:] if sku_text else None # Remove 'sku: ' from the beginning if exists
    nome_element = await page.query_selector('.information #produto-nome')
    nome = await nome_element.inner_text()
    descricao_element = await page.query_selector('div:nth-of-type(2) > .column.grid.one.ui > .column')
    descricao_text = await descricao_element.inner_text()
    descricao = descricao_text.replace('\n', '')
    preco_sem_desconto_element = await page.query_selector('span#preco-antigo')
    if preco_sem_desconto_element:
        preco_sem_desconto_text = await preco_sem_desconto_element.inner_text()
        preco_sem_desconto = float(preco_sem_desconto_text[3:].replace('.', '').replace(',','.'))
    else:
        preco_sem_desconto = 'sem preço antigo'
    preco_com_desconto_element = await page.query_selector('span#preco')
    preco_com_desconto_text = await preco_com_desconto_element.inner_text()
    preco_com_desconto = float(preco_com_desconto_text[3:].replace('.', '').replace(',','.'))
    # Calculo do percentual de desconto:
    if preco_sem_desconto != 'sem preço antigo':
        percentual_desconto = (preco_sem_desconto - preco_com_desconto) / preco_sem_desconto * 100
    else: 
        percentual_desconto = 'sem desconto'

    nova_linha = {'SKU':sku, 'Nome':nome, 'Preço sem desconto':preco_sem_desconto, 'Preço com desconto':preco_com_desconto, 'Percentual de desconto':percentual_desconto, 'Descrição':descricao}
    return nova_linha

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        # Definindo os setores que serão iterados
        setores = ['ginecologia','oncologia']
        # Definindo o número de páginas 
        paginas = [1,1]
        page = await browser.new_page()
        href_list = []
        # Extraindo os href da página 'pagina' do setor 'setor':
        for pagina,setor in zip(paginas,setores):
            print("Percorrendo o setor "+setor)

            pages = ['https://www.oncologmedicamentos.com.br/product/getproductscategory/?path=%2Fcategoria%2F'+str(setor)+'&viewList=g&pageNumber='+str(pagina)+'&pageSize=12&order=&brand=&category=112890' for pagina in range(1,pagina+1)]
            for instancia in pages:
                await page.goto(instancia)
                href_list.extend(await extract_href(page))

        print(len(href_list))

        # Extraindo as informações de cada href
        df_list = []

        async with browser.new_page() as page:
            for href_list in href_list:
                tasks = [extract_info(page, href) for href in href_list]
                results = await asyncio.gather(*tasks)
                df_list.append(pd.DataFrame(results))

        
        df = pd.concat(df_list, ignore_index=True)

                
        print(df)
        await browser.close()
        # Exporte o df em csv:
        df.to_csv('oncolog_oficial.csv', index=False)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
