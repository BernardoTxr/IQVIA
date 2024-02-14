from playwright.sync_api import sync_playwright
import re
import pandas as pd
import numpy as np


def extract_href(page):
    href_list = []
    produtos_desejados = page.query_selector('.ui.three.doubling.cards.centered')
    produtos = produtos_desejados.query_selector_all('.ui.image.fluid.attached')
    for produto in produtos:
        href_list.append(produto.get_attribute('href'))
    return href_list

def extract_info(page):
    sku = page.query_selector('.product__rating.stackable > div:nth-of-type(1) > .codProduto').inner_text()
    sku = sku[5:] # tira o texto 'sku: ' do início
    nome = page.query_selector('.information #produto-nome').inner_text()
    descricao = page.query_selector('div:nth-of-type(2) > .column.grid.one.ui > .column').inner_text()
    descricao = descricao.replace('\n', '')
    if page.query_selector('span#preco-antigo') == None:
        preco_sem_desconto = 'sem preço antigo'
    else:
        preco_sem_desconto = page.query_selector('span#preco-antigo').inner_text()
        preco_sem_desconto = float(preco_sem_desconto[3:].replace('.', '').replace(',','.'))
    preco_com_desconto = page.query_selector('span#preco').inner_text()
    preco_com_desconto = float(preco_com_desconto[3:].replace('.', '').replace(',','.'))
    # Calculo do percentual de desconto:
    if preco_sem_desconto != 'sem preço antigo':
        percentual_desconto = (preco_sem_desconto - preco_com_desconto) / preco_sem_desconto * 100
    else: 
        percentual_desconto = 'sem desconto'

    nova_linha = {'SKU':sku, 'Nome':nome, 'Preço sem desconto':preco_sem_desconto, 'Preço com desconto':preco_com_desconto, 'Percentual de desconto':percentual_desconto, 'Descrição':descricao}
    return nova_linha

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        # Definindo os setores que serão iterados
        setores = ['ginecologia','oncologia','endocrinologia','hematologia','nutricao','nefrologia','reumatologia','veterinario','reproducao-humana','gastroenterologia','controlados','suplementos-e-vitaminas','antibiotico','anti-inflamatorios','cardiologia','pneumologia','hepatologia','urologia','neurologia','oftamologia','antitermico','reposicao-hormonal','verao','dermocosmeticos']
        # Definindo o número de páginas 
        paginas = [2,40,4,11,18,1,3,12,1,3,7,7,2,4,7,2,1,1,2,1,2,1,4,6]

        setores = ['ginecologia','oncologia']
        paginas = [2,2]

        page = browser.new_page()

        href_list = []
        # Extraindo os href da página 'pagina' do setor 'setor':
        for pagina,setor in zip(paginas,setores):
            print("Percorrendo o setor "+setor)
            pages = ['https://www.oncologmedicamentos.com.br/product/getproductscategory/?path=%2Fcategoria%2F'+str(setor)+'&viewList=g&pageNumber='+str(pagina)+'&pageSize=12&order=&brand=&category=112890' for pagina in range(1,pagina+1)]
            for instancia in pages:
                page.goto(instancia)
                href_list.extend(extract_href(page))

        print(len(href_list))

        # Extraindo as informações de cada href
        df = pd.DataFrame()
        for href in href_list:
            page.goto('https://www.oncologmedicamentos.com.br/'+href)
            nova_linha = extract_info(page)
            df_temp = pd.DataFrame(nova_linha, index=[0])
            df = pd.concat([df, df_temp], ignore_index=True)
                
        print(df)
        browser.close()
        # Exporte o df em csv:
        df.to_csv('oncolog_oficial.csv', index=False)

if __name__ == '__main__':
    main()
