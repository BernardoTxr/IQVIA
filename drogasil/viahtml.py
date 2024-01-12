import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup

href_set = set()
for p in range(2):
  url = "https://www.drogasil.com.br/medicamentos.html?p=" + str(p)
  payload = {}
  headers = {
    'authority': 'www.drogasil.com.br',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'cookie': 'rrsession=h28ab332b1ca89150d91a; nav_id=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_p=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_browserId=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_c=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_s=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_anonymousUserId=anon-20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_testGroup=%7B%22experiment%22%3Anull%2C%22group%22%3Anull%2C%22testCode%22%3Anull%2C%22code%22%3Anull%2C%22session%22%3Anull%7D; guesttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; carttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; _gid=GA1.3.1680432160.1704587891; _rlu=47ba54e2-e976-4c22-87e4-e560bb2c4464; _rl_rl=0; _rlgm=7DD2zwA|y|11SvYaIv|z69E44GpO:y/k2DJBp5wr:y/k2B07zLgv:y/83Kqr1lP3:y/jY6vPp564:y/GRWzMj7JJ:y/XQpqOOPVl:y/r2V7glXWL:y/57K0OWEvx:y/oZXnvyODK:y/pZLoRznJ1:y/Br9YMo1rN:y/vQLyJQOyr:y/99YWAZ9lx:y/28Dm1NPyP:y/z6YM6PBq8:y|; _rllt=1704587891423; _rll_c_2267_d=1704587891429; _rll_c_2267_c=0; _rll_c_2267_sc=0; _rll_c_2267_sd=1704587891429; _tt_enable_cookie=1; _ttp=uzdapl01ydHOV-Bu2oxlWhhG0Lv; _pin_unauth=dWlkPU5URXhNVFJpT1RrdFpUUXhZUzAwT1RsbExXRTNPR0V0TVRKbFpEQXdOMlU1T0RVeA; _rlsnk=47ba_lr2rmci4; _gcl_au=1.1.1599598983.1704587894; blueID=c6fbd7b8-b313-4c10-9de6-ccfafc7504c1; user_unic_ac_id=8ceddb0d-c39c-cb32-463b-45e807d7f264; advcake_trackid=7aa5faa6-8b89-5bf6-59aa-1bd69c99aba0; OptanonAlertBoxClosed=2024-01-07T00:38:17.147Z; _rll__sel10s_d=1704587916581; _rll__sel10s_c=1; _rll__sel10s_sc=1; _rll__sel10s_sd=1704587916581; _rll__sel20s_d=1704587916584; _rll__sel20s_c=1; _rll__sel20s_sc=1; _rll__sel20s_sd=1704587916584; _rll_sc_1622_c=0; _rll__sel30s_d=1704588009841; _rll__sel30s_c=1; _rll__sel30s_sc=1; _rll__sel30s_sd=1704588009841; _rll__sel40s_d=1704588009843; _rll__sel40s_c=1; _rll__sel40s_sc=1; _rll__sel40s_sd=1704588009843; _rll__sel50s_d=1704588009845; _rll__sel50s_c=1; _rll__sel50s_sc=1; _rll__sel50s_sd=1704588009845; _rll__sel1m_d=1704588009846; _rll__sel1m_c=1; _rll__sel1m_sc=1; _rll__sel1m_sd=1704588009846; _rll_c_1813_d=1704588024848; _rll_c_1813_c=1; _rll_c_1813_sc=1; _rll_c_1813_sd=1704588024848; _rll__sel2m_d=1704588080161; _rll__sel2m_c=1; _rll__sel2m_sc=1; _rll__sel2m_sd=1704588080161; _hjSessionUser_557741=eyJpZCI6IjU2NDk4NDk0LWNjMDgtNWU3Mi05NWNkLWJlNjFmZGE1NjE3NiIsImNyZWF0ZWQiOjE3MDQ1ODgwMTIwOTksImV4aXN0aW5nIjp0cnVlfQ==; _rll__sel5m_d=1704593267364; _rll__sel5m_c=1; _rll__sel5m_sc=1; _rll__sel5m_sd=1704593267364; _rll__sel10m_d=1704593267365; _rll__sel10m_c=1; _rll__sel10m_sc=1; _rll__sel10m_sd=1704593267365; _rll__sel30m_d=1704593267366; _rll__sel30m_c=1; _rll__sel30m_sc=1; _rll__sel30m_sd=1704593267366; _rll__sel60m_d=1704593267367; _rll__sel60m_c=1; _rll__sel60m_sc=1; _rll__sel60m_sd=1704593267367; rcsicons=eF4Vx7sNgDAQBNHEEb2sdMt9fHRAGzZOCMiA-jEa6UlTluu9zyHUVLCKxV-k-DyA5Tn2xq1n1QFzKsyYyNEIkVBNt3X6AWWbEGw; mmapi.p.bid=%22prodiadcgus02%22; mmapi.p.srv=%22prodiadcgus02%22; rcsproducts=eF4Vx7ERgDAIBdAmlbv8uxAIgQ1cQyGFhZ06v3qvemU5n-vISmwMGlX0Z_qtC0DljnWLpvvMgXDfIG0GqmcHmTJbFwrPF4KDEdI; impulsesuite_session=1704646471995-0.10658159894229269; chaordic_session=1704646472287-0.06931777319778276; _rll_c_1622_d=1704646473999; _rll_c_1622_sc=2; _rll_c_1622_sd=1704646473999; _rll_c_1622_c=0; _hjIncludedInSessionSample_557741=0; _hjSession_557741=eyJpZCI6IjFkZDYxMDM5LWI0NjAtNGYwMi1iZmJhLTBkNmE1ZWFmYzE3MCIsImMiOjE3MDQ2NDY1MjMxMzAsInMiOjAsInIiOjAsInNiIjowfQ==; _hjAbsoluteSessionInProgress=0; rcspdp=eF4NxLENgDAMBMAmFbs8wvZjOxswRwiRKOiA-eGKK-XZt9Fr09UVzEGwtr_MgNLNMpocotP13mefxdIgsdDp6WpqICAfbDEQqQ; mmapi.p.pd=%22dt2xbRiKUUDUSN1thEBCy_6hE3opjyWfV_lH5uI8PIw%3D%7CHgAAAApDH4sIAAAAAAAEAGNheD4pcJq7xM49DMyZiSmMQgyMTgwqTLNfsDEsPL6yYhH_HY-izRqvJIA0AxD8hwIGNpfMotTkEkZ3CTaQOBjAJEE0VIjRFQA25gtUYQAAAA%3D%3D%22; _ga=GA1.1.1543159432.1704587890; _ga_5J2QJPHWJP=GS1.1.1704646471.6.1.1704646973.35.0.0; cto_bundle=oHRiD19ZalZNVllDJTJCTWQyRXMwVzdvaFluN0JCVTJnZ1NPbElKdzBmTEl1WHRub255MHN6SXEwSXF4VUJLdDRodHJJZXJIV1pDc2d4VE9seWV2Z1lOUlglMkZWcUdVSVNGSWlXR2lSSXBJeWJvaXF2YlJTbTk5ckhXUFozMlJ2c1Y3NllKTFZqd1VnVnhCUkIxTm5wdCUyQmtWSFBXJTJCdzZpZXoxUFYlMkY4TEQwTG9mMXFQTW8zOWJZV01JdnJQaEJPM1JaWElnbDI1MXJ4TThtVkVFcHdZdjFja1B2Z1ByQVdpciUyRmg5WmRvYk4zZUdBU2d4M1FtQWhyJTJGZXJPaXhBbkJUdGtQOVpKc2UyY3RMcTdSOG9FR2RhWUIlMkZ4ZVM1QSUyRk0lMkY4cG41cDBMTG8lMkJaSUw2ODllOFZUdUxFeGhKJTJCSmJOM05tdmhPOFBaSQ; OptanonConsent=isIABGlobal=false&datestamp=Sun+Jan+07+2024+14%3A02%3A54+GMT-0300+(Brasilia+Standard+Time)&version=6.22.0&hosts=&consentId=af2697d4-7a7e-43f2-a718-4ffd2dcf34e5&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0008%3A1&geolocation=BR%3BSP&AwaitingReconsent=false; _dc_gtm_UA-69376920-3=1; _dd_s=rum=0&expire=1704648022438; rcspdp=eF5j4cotK8lM0TM0tjDWNTQ3MDEzNjU2MzIwMNI10dU1ZClN9khLsUwyMjUDClikmeiaWCYBCQsLc10joFpjC_Mkw1RDIwBZ5RCc',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  soup = BeautifulSoup(response.text, 'html.parser')
  div_with_href = soup.find('div', {'class': 'ProductGridstyles__ProductGridStyles-tflwc1-0 gquykZ'})
  if div_with_href:
      href_tags = div_with_href.find_all('a', {'class': 'LinkNextstyles__LinkNextStyles-t73o01-0 cpRdBZ LinkNext'})
      href_set.update({tag.get('href') for tag in href_tags if tag.get('href') is not None})
  else:
      print("Div not found or doesn't have the specified class.")

html_produtos = []
for dominio in href_set:
    url = dominio
    payload = {}
    headers = {
    'authority': 'www.drogasil.com.br',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'cookie': 'rrsession=h28ab332b1ca89150d91a; nav_id=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_p=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_browserId=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_c=20209ead-3f78-41b4-8ef9-91c8e9de46ab; legacy_s=20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_anonymousUserId=anon-20209ead-3f78-41b4-8ef9-91c8e9de46ab; chaordic_testGroup=%7B%22experiment%22%3Anull%2C%22group%22%3Anull%2C%22testCode%22%3Anull%2C%22code%22%3Anull%2C%22session%22%3Anull%7D; guesttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; carttoken=ceksHPxt7i8UNdQcF6rFTOgbju2khBxJ; _gid=GA1.3.1680432160.1704587891; _rlu=47ba54e2-e976-4c22-87e4-e560bb2c4464; _rl_rl=0; _rlgm=7DD2zwA|y|11SvYaIv|z69E44GpO:y/k2DJBp5wr:y/k2B07zLgv:y/83Kqr1lP3:y/jY6vPp564:y/GRWzMj7JJ:y/XQpqOOPVl:y/r2V7glXWL:y/57K0OWEvx:y/oZXnvyODK:y/pZLoRznJ1:y/Br9YMo1rN:y/vQLyJQOyr:y/99YWAZ9lx:y/28Dm1NPyP:y/z6YM6PBq8:y|; _rllt=1704587891423; _rll_c_2267_d=1704587891429; _rll_c_2267_c=0; _rll_c_2267_sc=0; _rll_c_2267_sd=1704587891429; _tt_enable_cookie=1; _ttp=uzdapl01ydHOV-Bu2oxlWhhG0Lv; _pin_unauth=dWlkPU5URXhNVFJpT1RrdFpUUXhZUzAwT1RsbExXRTNPR0V0TVRKbFpEQXdOMlU1T0RVeA; _rlsnk=47ba_lr2rmci4; _gcl_au=1.1.1599598983.1704587894; blueID=c6fbd7b8-b313-4c10-9de6-ccfafc7504c1; user_unic_ac_id=8ceddb0d-c39c-cb32-463b-45e807d7f264; advcake_trackid=7aa5faa6-8b89-5bf6-59aa-1bd69c99aba0; OptanonAlertBoxClosed=2024-01-07T00:38:17.147Z; _rll__sel10s_d=1704587916581; _rll__sel10s_c=1; _rll__sel10s_sc=1; _rll__sel10s_sd=1704587916581; _rll__sel20s_d=1704587916584; _rll__sel20s_c=1; _rll__sel20s_sc=1; _rll__sel20s_sd=1704587916584; _rll_sc_1622_c=0; _rll__sel30s_d=1704588009841; _rll__sel30s_c=1; _rll__sel30s_sc=1; _rll__sel30s_sd=1704588009841; _rll__sel40s_d=1704588009843; _rll__sel40s_c=1; _rll__sel40s_sc=1; _rll__sel40s_sd=1704588009843; _rll__sel50s_d=1704588009845; _rll__sel50s_c=1; _rll__sel50s_sc=1; _rll__sel50s_sd=1704588009845; _rll__sel1m_d=1704588009846; _rll__sel1m_c=1; _rll__sel1m_sc=1; _rll__sel1m_sd=1704588009846; _rll_c_1813_d=1704588024848; _rll_c_1813_c=1; _rll_c_1813_sc=1; _rll_c_1813_sd=1704588024848; _rll__sel2m_d=1704588080161; _rll__sel2m_c=1; _rll__sel2m_sc=1; _rll__sel2m_sd=1704588080161; _hjSessionUser_557741=eyJpZCI6IjU2NDk4NDk0LWNjMDgtNWU3Mi05NWNkLWJlNjFmZGE1NjE3NiIsImNyZWF0ZWQiOjE3MDQ1ODgwMTIwOTksImV4aXN0aW5nIjp0cnVlfQ==; _rll__sel5m_d=1704593267364; _rll__sel5m_c=1; _rll__sel5m_sc=1; _rll__sel5m_sd=1704593267364; _rll__sel10m_d=1704593267365; _rll__sel10m_c=1; _rll__sel10m_sc=1; _rll__sel10m_sd=1704593267365; _rll__sel30m_d=1704593267366; _rll__sel30m_c=1; _rll__sel30m_sc=1; _rll__sel30m_sd=1704593267366; _rll__sel60m_d=1704593267367; _rll__sel60m_c=1; _rll__sel60m_sc=1; _rll__sel60m_sd=1704593267367; rcsicons=eF4Vx7sNgDAQBNHEEb2sdMt9fHRAGzZOCMiA-jEa6UlTluu9zyHUVLCKxV-k-DyA5Tn2xq1n1QFzKsyYyNEIkVBNt3X6AWWbEGw; mmapi.p.srv=%22prodiadcgus02%22; rcsproducts=eF4Vx7ERgDAIBdAmlbv8uxAIgQ1cQyGFhZ06v3qvemU5n-vISmwMGlX0Z_qtC0DljnWLpvvMgXDfIG0GqmcHmTJbFwrPF4KDEdI; impulsesuite_session=1704646471995-0.10658159894229269; chaordic_session=1704646472287-0.06931777319778276; _rll_c_1622_d=1704646473999; _rll_c_1622_sc=2; _rll_c_1622_sd=1704646473999; _rll_c_1622_c=0; _hjIncludedInSessionSample_557741=0; _hjSession_557741=eyJpZCI6IjFkZDYxMDM5LWI0NjAtNGYwMi1iZmJhLTBkNmE1ZWFmYzE3MCIsImMiOjE3MDQ2NDY1MjMxMzAsInMiOjAsInIiOjAsInNiIjowfQ==; _hjAbsoluteSessionInProgress=0; rcspdp=eF4NxLENgDAMBMAmFbs8wvZjOxswRwiRKOiA-eGKK-XZt9Fr09UVzEGwtr_MgNLNMpocotP13mefxdIgsdDp6WpqICAfbDEQqQ; mmapi.p.pd=%221bjcoO6odHojz0QfNW01v6keFtIMK7MCahm__uoj_mY%3D%7CHwAAAApDH4sIAAAAAAAEAGNheD4pcJq7xM49DMyZiSmMQgyMTgwqTLNfsDH4uPy6voj_jkfRZo1XEkCaAQj-QwEDm0tmUWpyCaO7BBtIHAxgkiAaKsToCgBcOxKHYQAAAA%3D%3D%22; _ga=GA1.1.1543159432.1704587890; OptanonConsent=isIABGlobal=false&datestamp=Sun+Jan+07+2024+14%3A05%3A36+GMT-0300+(Brasilia+Standard+Time)&version=6.22.0&hosts=&consentId=af2697d4-7a7e-43f2-a718-4ffd2dcf34e5&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0008%3A1&geolocation=BR%3BSP&AwaitingReconsent=false; cto_bundle=ir-aNV9ZalZNVllDJTJCTWQyRXMwVzdvaFluN0FGYjdrNGklMkZ3aHNINE9BMDE0M2N3TG5qS2JHTGMwMTZCVFF4dHhlbG8xazBvRU1mWEtCdVdFS0taaWN6V3FhdllPYTJ2OEZ3WUx3Z0hWRWpCQk4lMkJCclVJSkJJV3JFWGVoQ2R3NWpsWDV3eWpzYSUyRmxIc1RTMnpWSmg3RlZOSCUyRmwyTTk5ZmZTcTQzOGU5M21uVVlqVnZlRGx3Z2prYk1pVFlVeTFodm1YOFhXbmc1QUQ3SkxMTWtuekxHVVhNTURBdlAwUkolMkZuS2sxSU1TTTY0MGFzdyUyRjBPRVRlSTNOJTJGZFQyRllGbUs3ZWVsQ1NCTnpmUVIlMkJvN1FmMWY2eGdpbEtpWmh1NCUyQlpjcFpHTHBBUVptb2dJQiUyRjAwMVQzJTJGQ2o1NUpRdiUyQmFDaFE0bDVp; _dc_gtm_UA-69376920-3=1; _ga_5J2QJPHWJP=GS1.1.1704646471.6.1.1704647882.57.0.0; _dd_s=rum=0&expire=1704648782707; rcspdp=eF4NxDESgCAMBMCGyr-cQ5KDhB_4DkRmLOzU9-sWm5brvc-xioVBPLPSm0rJBgKSnn2bo3UtVcGYBFv_i3Aoq1l4l0P0A1vZEKY',
    'pragma': 'no-cache',
    'referer': 'https://www.drogasil.com.br/medicamentos.html',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    html_produtos.append(response.text)

# Colunas da Tabela:
df_sku = []
df_ean = []
df_cidade = []
df_produto = []
df_marca = []
df_farmacia = [] 
df_preco_com_desconto = []
df_preco_sem_desconto = []
df_perc_de_desconto = []
df_descricao = []
df_regiao = []

for html_produto in html_produtos:

    #Valores neutros:
    sku_value = 'Sem SKU'
    ean_value = 'Sem EAN'
    marca = 'Sem marca'
    value_from = None
    value_to = None
    nome_produto = None

    soup = BeautifulSoup(html_produto, 'html.parser')
    
    #extração da descrição
    product_description_div = soup.find('div', {'class': 'ProductDescriptionstyles__ProductDescriptionStyle-sc-17jtyta-0 eErzFZ'})
    if product_description_div:
        # Extrair todo o texto dentro da div
        description_text = product_description_div.get_text(strip=True)
    else:
        description_text = 'sem descricao'
    df_descricao.append(description_text)

    #extração do sku, ean e marca:
    th_tags = soup.find_all('th')
    for th in th_tags:
        if th.text.strip() == 'SKU':
            td_tag = th.find_next('td')
            if td_tag:
                div_tag = td_tag.find('div')
                if div_tag:
                    sku_value = div_tag.text.strip()
        if th.text.strip() == 'EAN':
            td_tag = th.find_next('td')
            if td_tag:
                div_tag = td_tag.find('div')
                if div_tag:
                    ean_value = div_tag.text.strip()
        if th.text.strip() == 'Marca':
            td_tag = th.find_next('td')
            if td_tag:
                a_tag = td_tag.find('a')
                if a_tag:
                    marca = a_tag.text.strip()
    df_sku.append(sku_value)
    df_ean.append(ean_value)
    df_marca.append(marca)
    
    #extração do preço com e sem desconto:
    scripts = soup.find_all('script')
    for script in scripts:
        if script.get('id') == '__NEXT_DATA__' and script.get('type') == 'application/json':
            json_data = json.loads(script.string)
            value_from = json_data['props']['pageProps']['pageData']['productData']['productBySku']['price_aux']['value_from']
            value_to = json_data['props']['pageProps']['pageData']['productData']['productBySku']['price_aux']['value_to']
            nome_produto = json_data['props']['pageProps']['pageQuery']['title']
    df_preco_sem_desconto.append(value_from)
    df_preco_com_desconto.append(value_to)
    df_produto.append(nome_produto)
    
    #calculo do percentual de desconto:
    if value_from is not None and value_to is not None:
        desconto = (value_from - value_to) * 100 / value_from
    else:
        desconto = None
    df_perc_de_desconto.append(desconto)

    #por default, vamos estabelecer alguns dados:
    df_cidade.append('São Paulo')
    df_regiao.append('SE')
    df_farmacia.append('Drogasil')

df = pd.DataFrame({
    'Região': df_regiao,
    'Cidade': df_cidade,
    'Farmácia': df_farmacia,
    'Produto': df_produto,
    'EAN': df_ean,
    'SKU': df_sku,
    'Preço sem Desconto': df_preco_sem_desconto,
    'Preço com Desconto': df_preco_com_desconto,
    'Percentual de Desconto': df_perc_de_desconto,
    'Descrição': df_descricao
})

df.to_csv('C:/Users/berna/OneDrive/Documents/[PJ] IQVIA/drogasil/drogasil_teste.csv', index=False)
