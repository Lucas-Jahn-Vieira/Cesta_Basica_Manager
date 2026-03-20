import requests
import json

BASE_URL = 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.'

def buscar_serie(codigo_serie:int, dataInicial:str=None, dataFinal:str=None) -> dict:
    #cria a url
    serie_url:str = f'{BASE_URL}{codigo_serie}/dados?formato=json'
    
    if dataInicial != None:
        serie_url = serie_url + f'&dataInicial={dataInicial}'
    if dataFinal != None:
        serie_url = serie_url + f'&dataFinal={dataFinal}'

    #chama API
    resposta = requests.get(serie_url)

    # vê nq dá
    if resposta.status_code == 200:
        datas_dict = list(json.loads(resposta.text))
        print(datas_dict[0])
        return datas_dict
    else:
        print('deu ruim chefe ;-;')
        return None

buscar_serie(10844)