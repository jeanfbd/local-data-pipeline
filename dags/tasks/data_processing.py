import pandas as pd
import logging

def data_processing():
    """Processa dados de um arquivo CSV, gerando duas tabelas e salvando os resultados.

    Este processo realiza as seguintes operações:
    1. Carrega os dados limpos de um arquivo CSV.
    2. Gera a Tabela 1: Média de 'risk_score' por 'location_region', ordenada de forma decrescente.
    3. Gera a Tabela 2: As 3 maiores transações de 'sale', agrupadas por 'receiving_address' e ordenadas por 'amount'.
    4. Salva ambas as tabelas em arquivos CSV separados.

    Logs são gerados para informar o início e a conclusão do processamento de dados.

    Example:
        data_processing()

    Returns:
        None

    Raises:
        FileNotFoundError: Se o arquivo 'data/cleaned_data.csv' não for encontrado.
        KeyError: Se as colunas necessárias ('location_region', 'risk_score', 'transaction_type', 'receiving_address', 'amount', 'timestamp') não existirem no dataframe.
    """
    logging.info("Iniciando processamento de dados.")
    df = pd.read_csv("data/cleaned_data.csv")

    # Tabela 1: Média de 'risk_score' por 'location_region'
    table1 = (
        df.groupby('location_region')['risk_score']
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    # Tabela 2: 3 maiores transações
    df_filtered = df[df['transaction_type'] == 'sale']
    df_filtered = (
        df_filtered.sort_values('timestamp')
        .groupby('receiving_address', as_index=False)
        .last()
    )
    table2 = df_filtered.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]

    # Salvar resultados
    table1.to_csv("data/table1.csv", index=False)
    table2.to_csv("data/table2.csv", index=False)
    logging.info("Processamento concluído e tabelas geradas.")
