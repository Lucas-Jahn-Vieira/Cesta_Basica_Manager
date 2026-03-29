from sqlalchemy import create_engine, inspect
import pandas as pd
import os

def create_tables_csv():
    # Caminho para o banco
    db_path = "cesta_basica.db"

    # Criar engine
    engine = create_engine(f"sqlite:///{db_path}")

    # Inspecionar tabelas
    insp = inspect(engine)
    tabelas = insp.get_table_names()

    # Criar pasta para exportação
    os.makedirs("csv_exports", exist_ok=True)

    # Exportar cada tabela para CSV
    for tabela in tabelas:
        df = pd.read_sql_table(tabela, con=engine)
        output_path = os.path.join("csv_exports", f"{tabela}.csv")
        df.to_csv(output_path, index=False)
        print(f"Tabela {tabela} exportada para {output_path}")

create_tables_csv()