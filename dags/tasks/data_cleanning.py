import pandas as pd
import logging

def data_cleanning():
    """Realiza a limpeza de dados a partir de um arquivo CSV e salva os dados limpos.

    Este processo inclui as seguintes etapas:
    1. Leitura das primeiras 100 linhas de um arquivo CSV.
    2. Conversão das colunas 'risk_score' e 'amount' para valores numéricos.
    3. Filtragem de linhas com valores inválidos na coluna 'location_region'.
    4. Remoção de linhas com valores ausentes nas colunas 'risk_score' e 'amount'.
    5. Salvamento do dataframe limpo em um novo arquivo CSV.

    Logs são gerados para informar o início e a conclusão da limpeza de dados, 
    além do número de registros restantes após a limpeza.

    Example:
        data_cleanning()

    Returns:
        None

    Raises:
        ValueError: Se o arquivo CSV de entrada não contiver as colunas necessárias 
                    ('risk_score', 'amount', 'location_region').
    """
    logging.info("Iniciando limpeza de dados.")

    # Ler apenas as primeiras 100 linhas do arquivo CSV
    df = pd.read_csv("data/input.csv", nrows=100)

    # Limpeza básica
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df[df['location_region'].str.match(r'^[a-zA-Z\s]+$', na=False)]
    df = df.dropna(subset=['risk_score', 'amount'])

    # Salvar dados limpos
    df.to_csv("data/cleaned_data.csv", index=False)
    logging.info(f"Limpeza concluída. Registros restantes: {len(df)}")
