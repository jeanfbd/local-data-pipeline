import pandas as pd
import logging

def data_quality():
    """Calcula e exibe métricas de qualidade dos dados a partir de um arquivo CSV.

    Este processo realiza as seguintes operações:
    1. Carrega os dados limpos de um arquivo CSV.
    2. Calcula o total de registros, o número de valores ausentes e a taxa de conformidade dos dados.
    3. Exibe as métricas calculadas no log.

    Logs são gerados para informar os resultados das métricas de qualidade.

    Example:
        data_quality()

    Returns:
        None

    Raises:
        FileNotFoundError: Se o arquivo 'data/cleaned_data.csv' não for encontrado.
        ValueError: Se o arquivo CSV estiver vazio ou não contiver dados válidos.
    """
    logging.info("Calculando métricas de qualidade.")
    df = pd.read_csv("data/cleaned_data.csv")

    total_records = len(df)
    missing_values = df.isnull().sum().sum()
    compliance_rate = 100 * (total_records - missing_values) / total_records

    logging.info(f"Total de registros: {total_records}")
    logging.info(f"Valores ausentes: {missing_values}")
    logging.info(f"Taxa de conformidade: {compliance_rate:.2f}%")
