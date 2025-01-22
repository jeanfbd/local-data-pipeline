import pandas as pd
import os

# Configurações de exibição do Pandas (opcional)
pd.set_option('display.max_columns', None)  # Exibir todas as colunas
pd.set_option('display.max_rows', None)     # Exibir todas as linhas
pd.set_option('display.width', None)        # Ajustar a largura da exibição
pd.set_option('display.max_colwidth', None) # Exibir todo o conteúdo das células


def load_data(file_path):
    """
    Carrega os dados de um arquivo CSV.

    Args:
        file_path (str): Caminho para o arquivo CSV.

    Returns:
        pd.DataFrame: DataFrame contendo os dados carregados.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    return pd.read_csv(file_path)


def clean_data(df):
    """
    Limpa os dados para preparar para processamento.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame limpo.
    """
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df[df['location_region'].str.match(r'^[a-zA-Z\s]+$', na=False)]
    df = df.dropna(subset=['risk_score', 'amount'])
    return df


def compute_table1(df):
    """
    Calcula a média de 'risk_score' por 'location_region', em ordem decrescente.

    Args:
        df (pd.DataFrame): DataFrame limpo.

    Returns:
        pd.DataFrame: DataFrame contendo a tabela 1.
    """
    return (
        df.groupby('location_region')['risk_score']
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )


def compute_table2(df):
    """
    Seleciona os 3 maiores valores de 'amount' considerando transações recentes.

    Args:
        df (pd.DataFrame): DataFrame limpo.

    Returns:
        pd.DataFrame: DataFrame contendo a tabela 2.
    """
    df_filtered = df[df['transaction_type'] == 'sale']
    df_filtered = (
        df_filtered.sort_values('timestamp')
        .groupby('receiving_address', as_index=False)
        .last()
    )
    return df_filtered.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]


def calculate_metrics(df, original_count):
    """
    Calcula métricas de qualidade dos dados.

    Args:
        df (pd.DataFrame): DataFrame limpo.
        original_count (int): Total de registros no DataFrame original.

    Returns:
        dict: Dicionário contendo as métricas calculadas.
    """
    total_records = original_count
    valid_records = len(df)
    error_records = total_records - valid_records
    compliance_rate = (valid_records / total_records) * 100

    return {
        "total_records": total_records,
        "valid_records": valid_records,
        "error_records": error_records,
        "compliance_rate": compliance_rate,
    }


def main():
    """
    Função principal para executar o pipeline de dados.
    """
    input_file = "data/input.csv"
    output_dir = "data/output"

    # Criação do diretório de saída, se não existir
    os.makedirs(output_dir, exist_ok=True)

    print("=== Iniciando o pipeline de dados ===")

    # Carregando os dados
    print("Carregando os dados...")
    df_original = load_data(input_file)
    original_count = len(df_original)

    # Limpando os dados
    print("Limpando os dados...\n")
    df_cleaned = clean_data(df_original)

    # Calculando métricas de qualidade
    print("Calculando metricas de qualidade...\n")
    metrics = calculate_metrics(df_cleaned, original_count)
    print(f"Metricas calculadas: ")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    # Processando a Lista 1
    print("\nGerando a Lista 1...")
    table1 = compute_table1(df_cleaned)
    table1.to_csv(os.path.join(output_dir, "table1.csv"), index=False)
    print("Lista 1 gerada com sucesso!")
    print(table1)

    # Processando a Lista 2
    print("\nGerando a Lista 2...")
    table2 = compute_table2(df_cleaned)
    table2.to_csv(os.path.join(output_dir, "table2.csv"), index=False)
    print("Lista 2 gerada com sucesso!")
    print(table2)

    print("\n=== Pipeline concluido ===")


if __name__ == "__main__":
    main()
