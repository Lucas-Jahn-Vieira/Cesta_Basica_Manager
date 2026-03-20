#fazer os scripts pra as funções em seus arquivos e dps juntar tdd aq

from bd import criar_banco, inserir
from api import buscar_serie


criar_banco()

inserir('tabela_ipca', buscar_serie(10844))