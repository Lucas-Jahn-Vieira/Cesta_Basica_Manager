import json
import pandas as pd
from datetime import datetime
from bd import engine, tabela_categoria, tabela_produto
from sqlalchemy import text
import os

def carregar_produtos_do_json():
    with open('produtos.json', 'r', encoding='utf-8') as f:
        produtos = json.load(f)
    # filtrar produtos sem preço
    produtos = [p for p in produtos if p['preco_total'] is not None]
    return produtos

def associar_categoria(produto_nome):
    """Associa o nome do produto a uma categoria do banco."""
    nome_lower = produto_nome.lower()
    if 'arroz' in nome_lower:
        return 'Arroz'
    if 'feijão' in nome_lower or 'feijao' in nome_lower:
        return 'Feijão'
    if 'óleo' in nome_lower or 'oleo' in nome_lower:
        return 'Óleo de soja'
    if 'açúcar' in nome_lower or 'acucar' in nome_lower:
        return 'Açúcar'
    if 'café' in nome_lower or 'cafe' in nome_lower:
        return 'Café'
    if 'macarrão' in nome_lower:
        return 'Macarrão'
    if 'farinha' in nome_lower:
        return 'Farinha'
    if 'sal' in nome_lower:
        return 'Sal'
    return None

def calcular_cestas(produtos):
    # Agrupar por categoria
    categorias = {}
    for p in produtos:
        cat = associar_categoria(p['produto'])
        if cat:
            if cat not in categorias:
                categorias[cat] = []
            categorias[cat].append(p)

    # Para cada categoria, encontrar menor e maior preço
    cesta_menor = {}
    cesta_maior = {}
    for cat, lista in categorias.items():
        # Ordena pelo preço total (já normalizado para a quantidade desejada)
        lista_ordenada = sorted(lista, key=lambda x: x['preco_total'])
        cesta_menor[cat] = lista_ordenada[0]  # mais barato
        cesta_maior[cat] = lista_ordenada[-1] # mais caro

    return cesta_menor, cesta_maior

def gerar_relatorio_cesta(cesta, nome_arquivo):
    """Salva um relatório em CSV com a composição da cesta."""
    linhas = []
    for cat, produto in cesta.items():
        linhas.append({
            'Categoria': cat,
            'Produto': produto['produto'],
            'Marca': produto.get('marca', 'N/A'),
            'Preço Unitário (R$)': produto['preco_total'],
            'Quantidade Necessária (kg)': produto['quantia'],
            'Unidades para atingir': produto.get('unidades', 1),  # se tiver
            'URL': produto['url_do_produto']
        })
    df = pd.DataFrame(linhas)
    df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
    print(f"Relatório salvo: {nome_arquivo}")

def calcular_estimativa_historica(valor_atual_cesta, ano_referencia=2026):
    """
    Calcula o valor estimado da cesta em anos anteriores usando deflação pelo IPCA.
    Supõe que temos a tabela ipca no banco com colunas 'data' (YYYY-MM-DD) e 'valor' (variação mensal).
    """
    # Carregar IPCA do banco
    query = "SELECT data, valor FROM ipca ORDER BY data"
    df_ipca = pd.read_sql(query, engine)
    df_ipca['data'] = pd.to_datetime(df_ipca['data'])
    df_ipca['ano'] = df_ipca['data'].dt.year
    # Calcular fator acumulado por ano (multiplicativo)
    # Agrupar por ano e multiplicar (1 + valor/100) - cuidado: valor é percentual
    df_ipca['fator_mensal'] = 1 + df_ipca['valor'] / 100
    fator_anual = df_ipca.groupby('ano')['fator_mensal'].prod().reset_index()
    fator_anual = fator_anual.sort_values('ano')

    # Obter o fator acumulado do ano de referência até o ano desejado
    # Para deflação: valor_anterior = valor_atual / fator_acumulado_desde_entao
    # Vamos calcular o valor para cada ano desde o primeiro disponível até ano_referencia-1
    resultados = []
    # Precisamos do fator acumulado desde o início do ano_referencia até cada ano anterior
    # Para simplificar, calculamos o valor de cada ano como valor_atual / (produto dos fatores dos anos entre ano e ano_referencia-1)
    # Vamos iterar sobre os anos disponíveis
    fator_total = 1.0
    # Ordenar decrescente para aplicar deflação
    for _, row in fator_anual.sort_values('ano', ascending=False).iterrows():
        ano = row['ano']
        if ano >= ano_referencia:
            continue
        # Valor estimado = valor_atual / fator_total (fator_total é o acumulado do ano até ano_referencia-1)
        valor_estimado = valor_atual_cesta / fator_total
        resultados.append({'ano': ano, 'valor_estimado': round(valor_estimado, 2)})
        # Atualiza fator_total com o fator do ano atual para o próximo (mais antigo)
        fator_total *= row['fator_mensal']

    return resultados

# --- Execução principal ---
if __name__ == "__main__":
    # 1. Verificar se o banco existe e criar se necessário
    from bd import criar_banco, popular_categorias
    if not os.path.exists('cesta_basica.db'):
        criar_banco()
        popular_categorias()

    # 2. Se ainda não fez scraping, executar
    if not os.path.exists('produtos.json'):
        from scraper import init_scraper
        init_scraper()

    # 3. Carregar produtos do JSON
    produtos = carregar_produtos_do_json()

    # 4. Calcular cestas
    cesta_menor, cesta_maior = calcular_cestas(produtos)

    # 5. Gerar relatórios das cestas
    gerar_relatorio_cesta(cesta_menor, 'cesta_menor_valor.csv')
    gerar_relatorio_cesta(cesta_maior, 'cesta_maior_valor.csv')

    # 6. Calcular valor total de cada cesta
    valor_menor = sum(p['preco_total'] for p in cesta_menor.values())
    valor_maior = sum(p['preco_total'] for p in cesta_maior.values())
    print(f"Valor cesta mais barata: R$ {valor_menor:.2f}")
    print(f"Valor cesta mais cara: R$ {valor_maior:.2f}")

    # 7. Estimar valores históricos (usando a cesta mais barata como exemplo)
    historico = calcular_estimativa_historica(valor_menor, ano_referencia=2026)
    df_historico = pd.DataFrame(historico)
    df_historico.to_csv('estimativa_historica.csv', index=False)
    print("Estimativa histórica salva em 'estimativa_historica.csv'")