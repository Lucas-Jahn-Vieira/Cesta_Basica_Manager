#fazer os scripts pra as funções em seus arquivos e dps juntar tdd aq
import os
import pandas as pd
from bd import criar_banco, inserir
from api import buscar_serie

#lógica do programa
if not os.path.isfile('./cesta_basica.xlsx'):
    criar_banco()

    serie = 10844
    inserir('tabela_ipca', buscar_serie(serie))

df = pd.read_excel('cesta_basica.xlsx')

print('\n \n ') #adiciona espaços pra diferenciar o log do banco de dados com os prints do programa

df.sort_values(by='valor', ascending=False)
IPCA_maior = df.iloc[0]
IPCA_menor = df.iloc[-1]

print(f'mais cara: {IPCA_maior} \n\nMais barata: {IPCA_menor}')

#adicionar lógica de scrapping para pegar os itens dessas cestas