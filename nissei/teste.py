import requests

url = "https://www.farmaciasnissei.com.br/pegar/preco"

payload = "csrfmiddlewaretoken=AeEziSSd8iagBw2JOOr7mcxmo6asFJRJ5E0O3pZ6QXXvEt08dJ2loejfbEEma7hz&produtos_ids%5B%5D=6774&produtos_ids%5B%5D=10696&produtos_ids%5B%5D=15082&produtos_ids%5B%5D=23971&produtos_ids%5B%5D=26860&produtos_ids%5B%5D=27503&produtos_ids%5B%5D=28072&produtos_ids%5B%5D=29211&produtos_ids%5B%5D=40705&produtos_ids%5B%5D=44294&produtos_ids%5B%5D=44723&produtos_ids%5B%5D=51099&produtos_ids%5B%5D=51295&produtos_ids%5B%5D=51940&produtos_ids%5B%5D=52935&produtos_ids%5B%5D=53274&produtos_ids%5B%5D=53727&produtos_ids%5B%5D=53864&produtos_ids%5B%5D=55846&produtos_ids%5B%5D=56669&produtos_ids%5B%5D=56684&produtos_ids%5B%5D=56995&produtos_ids%5B%5D=57126&produtos_ids%5B%5D=57860&produtos_ids%5B%5D=58047&produtos_ids%5B%5D=58068&produtos_ids%5B%5D=58082&produtos_ids%5B%5D=59751&produtos_ids%5B%5D=59765&produtos_ids%5B%5D=59789&produtos_ids%5B%5D=59827&produtos_ids%5B%5D=60693&produtos_ids%5B%5D=61029&produtos_ids%5B%5D=61082&produtos_ids%5B%5D=61346&produtos_ids%5B%5D=61348&produtos_ids%5B%5D=61956&produtos_ids%5B%5D=61957&produtos_ids%5B%5D=62562&produtos_ids%5B%5D=63078&produtos_ids%5B%5D=63605&produtos_ids%5B%5D=63607&produtos_ids%5B%5D=63609&produtos_ids%5B%5D=63611&produtos_ids%5B%5D=63618&produtos_ids%5B%5D=63766&produtos_ids%5B%5D=64096&produtos_ids%5B%5D=64914&produtos_ids%5B%5D=64915&produtos_ids%5B%5D=65734&produtos_ids%5B%5D=65766&produtos_ids%5B%5D=66467&produtos_ids%5B%5D=66607&produtos_ids%5B%5D=66755&produtos_ids%5B%5D=66979&produtos_ids%5B%5D=66980&produtos_ids%5B%5D=67914&produtos_ids%5B%5D=77818&produtos_ids%5B%5D=94979&produtos_ids%5B%5D=653663&produtos_ids%5B%5D="
for p in range(1,3):
    headers = {
    'authority': 'www.farmaciasnissei.com.br',
    'accept': '*/*',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'csrftoken=uIaJnfIYMWdMml4LSE6JgQTxY3XTtUIGZ8wY8MPRuB01pi2ahzHXiSFqLBrNYi8w; _gcl_au=1.1.1723091261.1706525409; _ga=GA1.1.1474061591.1706525410; _hjSession_3567234=eyJpZCI6ImY2YTQ3YWViLTQ4OWQtNDUwMi1iZjZhLWRkNjBmMDhmYTE1NyIsImMiOjE3MDY1MjU0MTAwMDQsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=; blueID=dbef6668-4929-410e-9023-ed08978119fa; nvg80195=13fef3dbdc078bc62f6c000af610|0_30; _clck=1iyk0rp%7C2%7Cfit%7C0%7C1489; sessionid=3e3kz4ai1sh3swoqu28445x8edkjkr24; _hjSessionUser_3567234=eyJpZCI6Ijk2N2FjMzVjLTcyMTAtNWZiNi05ZTMxLTQxMTBmOTdhNmEwNSIsImNyZWF0ZWQiOjE3MDY1MjU0MTAwMDMsImV4aXN0aW5nIjp0cnVlfQ==; cto_bundle=akWNMF85M2g0JTJCOU9xUnVkNHpMdkNoMjIyRkVEYm1vUEVnWmY3WkpMaXVzT0R3TzVGbGpxbkl3RTE1Y0RjQUMwdGdaJTJCdXdKdzN3RlZBVWNrSjBYanAzQXBnbDlKOTJKbk1NUVJ5SWJpcVFlRGVjN1ZFQm1ta0RXRUV5R01tWWVQa2RXVnh6dFk2NGd2Y2NVRmIlMkJ4czJ3RDRYeFlTRzJqcjAxeXR3ZSUyRjhIVFdkSmlLayUzRA; _uetsid=2c26a120be9411ee8ab09d0a7bb6b740; _uetvid=2c26cee0be9411eebbff6fcd53f45266; _clsk=1yxx38q%7C1706525756081%7C7%7C1%7Cl.clarity.ms%2Fcollect; _ga_ECGJX07HER=GS1.1.1706525751.1.1.1706525768.43.0.0; _ga_G8H8ZH3E1D=GS1.1.1706525409.1.1.1706525769.42.0.0',
    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI4OTk0MzciLCJhcCI6IjE4MzM0MTQxMTUiLCJpZCI6ImYzOTdmNWYyMDEyZjI2MjEiLCJ0ciI6ImI0NWYzMjc2OTYyNjlmMWI4NzM0ZmYzMGExNjA5N2MyIiwidGkiOjE3MDY1MjU3NzAwMzN9fQ==',
    'origin': 'https://www.farmaciasnissei.com.br',
    'referer': 'https://www.farmaciasnissei.com.br/categorias/medicamentos?pagina='+str(p)+'&ordenacao=&is_termo=true&preco-min=undefined&preco-max=undefined',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'traceparent': '00-b45f327696269f1b8734ff30a16097c2-f397f5f2012f2621-01',
    'tracestate': '2899437@nr=0-1-2899437-1833414115-f397f5f2012f2621----1706525770033',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-newrelic-id': 'Vg4OWFJQDxAGU1hbBgUFUlU=',
    'x-requested-with': 'XMLHttpRequest'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)
    print(p)