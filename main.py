#fazer os scripts pra as funções em seus arquivos e dps juntar tdd aq
import os
import pandas as pd
import json

from bd import criar_banco, inserir
from api import buscar_serie
from scraper import init_scraper

def set_preco_cesta(tamanho) -> float:
    print('pegando preços')
    produtos_cesta = {
    'arroz':0.0,
    'feijão':0.0,
    'óleo':0.0,
    'áçucar':0.0,
    'café':0.0
    }

    precos = []
    ultima_chave = (produtos[0]['produto'].split(' ')[0]).lower()
    for produto in produtos:
        nome_produto = produto['produto'].lower()
        preco = float(produto['preco_total'])

        if ultima_chave in nome_produto:
            precos.append(float(preco))
        else:
            print('trocou de chave')
            if len(precos) == 0:
                print(f'{ultima_chave} não teve valores')
                produtos_cesta[ultima_chave] = 0.0
            elif tamanho == 'maior':
                produtos_cesta[ultima_chave] = max(precos)
                print(f'{ultima_chave} teve {len(precos)} itens, o maior foi {produtos_cesta[ultima_chave]}')
                precos = []
            else:
                produtos_cesta[ultima_chave] = min(precos)
                print(f'{ultima_chave} teve {len(precos)}, o menor foi {produtos_cesta[ultima_chave]}')
                precos = []
            
            for chave in produtos_cesta.keys():
                if chave in nome_produto:
                    print(chave, ' / ',nome_produto)
                    ultima_chave = chave
            
    valor_cesta = sum(produtos_cesta.values())

    return valor_cesta

#lógica do programa
if not os.path.isfile('./cesta_basica.xlsx'):
    criar_banco()

    serie = 10844
    inserir('tabela_ipca', buscar_serie(serie))

df = pd.read_excel('cesta_basica.xlsx')

df.sort_values(by='valor', ascending=False)
IPCA_maior = df.iloc[0]
IPCA_menor = df.iloc[-1]

print(f'Mais cara: {IPCA_maior} \n\nMais barata: {IPCA_menor}\n')

if not os.path.isfile('./produtos.json'):
    print('SCRAPER PRINTS:')
    init_scraper()

produtos = json.load(open('produtos.json'))
for p in produtos:
    if p['preco_total'] == None:
        produtos.remove(p)

cesta_mais_cara = set_preco_cesta('maior')
cesta_mais_barata = set_preco_cesta('menor')

print(f'cesta mais cara: {cesta_mais_cara} \n\ncesta mais barata: {cesta_mais_barata}')
