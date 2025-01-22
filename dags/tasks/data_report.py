import pandas as pd
import logging

def data_report():
    """Gera e exibe o relatório final com duas tabelas de dados.

    Este processo realiza as seguintes operações:
    1. Carrega as Tabelas 1 e 2 a partir dos arquivos CSV 'data/table1.csv' e 'data/table2.csv'.
    2. Exibe a Tabela 1, que contém a média de 'risk_score' por 'location_region'.
    3. Exibe a Tabela 2, que contém as 3 maiores transações do tipo 'sale'.
    4. Exibe as tabelas no console.

    Logs são gerados para informar o início do processo de geração do relatório.

    Example:
        data_report()

    Returns:
        None

    Raises:
        FileNotFoundError: Se os arquivos 'data/table1.csv' ou 'data/table2.csv' não forem encontrados.
        pd.errors.EmptyDataError: Se algum dos arquivos CSV estiver vazio ou não contiver dados válidos.
    """
    logging.info("Gerando relatório final.")
    table1 = pd.read_csv("data/table1.csv")
    table2 = pd.read_csv("data/table2.csv")

    print("Tabela 1: Média de 'risk_score' por 'location_region'")
    print(table1)

    print("\nTabela 2: Top 3 transações 'sale'")
    print(table2)
