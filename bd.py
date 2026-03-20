import pandas as pd

import sqlalchemy
sqlalchemy.__version__  

"""
01_schema.py
Cria o schema do banco de dados SQLite para o projeto Cesta Básica.
Tabelas: ipca, categoria, produto

Uso:
    python 01_schema.py
"""

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey, UniqueConstraint
)
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Engine — banco salvo no arquivo cesta_basica.db (mesma pasta do script)
# ---------------------------------------------------------------------------
engine = create_engine("sqlite:///cesta_basica.db", echo=True)
metadados = MetaData()

# ---------------------------------------------------------------------------
# Tabela: ipca
# Armazena a série histórica mensal do IPCA vinda da API do Banco Central.
# Cada linha = um mês com sua variação percentual.
# ---------------------------------------------------------------------------
tabela_ipca = Table(
    "ipca",
    metadados,
    Column("id",        Integer, primary_key=True, autoincrement=True),
    Column("data",      String,    nullable=False),   # primeiro dia do mês: 2024-01-01
    Column("valor",     Float,   nullable=False),   # variação % do mês  : ex. 0.42
    # Garante que não salvamos o mesmo mês duas vezes
    # UniqueConstraint("data", name="uq_ipca_data"),
)

# ---------------------------------------------------------------------------
# Tabela: categoria
# Representa cada item da cesta básica (arroz, feijão, óleo…).
# Separa a definição do item dos produtos concretos encontrados no mercado.
# ---------------------------------------------------------------------------
tabela_categoria = Table(
    "categoria",
    metadados,
    Column("id",              Integer, primary_key=True, autoincrement=True),
    Column("nome",            String(100), nullable=False, unique=True),  # ex: "Arroz"
    Column("quantidade_kg",   Float,       nullable=False),               # ex: 5.0
    Column("unidade",         String(20),  nullable=False),               # ex: "kg" ou "ml" ou "g"
    Column("eh_bonus",        Integer,     nullable=False, default=0),    # 0 = cesta básica, 1 = complemento bônus
)

# ---------------------------------------------------------------------------
# Tabela: produto
# Cada linha é um produto concreto encontrado no site do Giassi.
# Relacionado a uma categoria via chave estrangeira.
# ---------------------------------------------------------------------------
tabela_produto = Table(
    "produto",
    metadados,
    Column("id",           Integer, primary_key=True, autoincrement=True),
    Column("categoria_id", Integer, ForeignKey("categoria.id"), nullable=False),
    Column("nome",         String(200), nullable=False),   # nome completo no site
    Column("marca",        String(100)),                   # marca do produto
    Column("preco",        Float,       nullable=False),   # preço unitário atual (R$)
    Column("quantidade",   Float,       nullable=False),   # quantidade na embalagem (kg/ml/g)
    Column("unidade",      String(20),  nullable=False),   # unidade da embalagem
    Column("url",          String(500)),                   # URL da página do produto
    Column("data_coleta",  Date,        nullable=False),   # data em que o preço foi capturado
)

# ---------------------------------------------------------------------------
# Cria todas as tabelas no banco (ignora se já existirem)
# ---------------------------------------------------------------------------
def criar_banco():
    metadados.create_all(engine)
    print("\n✅  Banco de dados criado com sucesso: cesta_basica.db")
    print("   Tabelas criadas: ipca, categoria, produto")


# ---------------------------------------------------------------------------
# Popula as categorias da cesta básica (só insere se a tabela estiver vazia)
# ---------------------------------------------------------------------------
CATEGORIAS = [
    # Cesta básica obrigatória (eh_bonus = 0)
    {"nome": "Arroz",     "quantidade_kg": 5.0,   "unidade": "kg",  "eh_bonus": 0},
    {"nome": "Feijão",    "quantidade_kg": 2.0,   "unidade": "kg",  "eh_bonus": 0},
    {"nome": "Óleo de soja", "quantidade_kg": 0.9, "unidade": "ml", "eh_bonus": 0},
    {"nome": "Açúcar",    "quantidade_kg": 1.0,   "unidade": "kg",  "eh_bonus": 0},
    {"nome": "Café",      "quantidade_kg": 0.5,   "unidade": "g",   "eh_bonus": 0},
    # Complemento bônus (eh_bonus = 1)
    {"nome": "Macarrão",          "quantidade_kg": 1.0, "unidade": "kg", "eh_bonus": 1},
    {"nome": "Farinha",           "quantidade_kg": 0.5, "unidade": "kg", "eh_bonus": 1},
    {"nome": "Sal",               "quantidade_kg": 1.0, "unidade": "kg", "eh_bonus": 1},
]

def popular_categorias():
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM categoria")).scalar()
        if total > 0:
            print("ℹ️   Categorias já existem — nada foi inserido.")
            return

        conn.execute(tabela_categoria.insert(), CATEGORIAS)
        print(f"✅  {len(CATEGORIAS)} categorias inseridas.")


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    criar_banco()
    popular_categorias()

def inserir(tabela_escolhida, informacao:list[dict]):
    tabela = []

    match tabela_escolhida:
        case 'tabela_ipca':
            tabela = tabela_ipca
    
    with engine.begin() as bd:
        for d in informacao:
            bd.execute(tabela.insert().values(data = d['data'], valor = d['valor']))
    
    atualizar_db()

def atualizar_db():
    df = pd.read_sql("SELECT * FROM ipca", engine)

    df.to_excel('cesta_basica.xlsx', index=False)