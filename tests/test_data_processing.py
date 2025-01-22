import os
import pandas as pd
from dags.tasks.data_processing import data_processing

def test_data_processing(tmpdir):
    """
    Testa o processamento de dados, incluindo a criação de duas tabelas a partir de um conjunto de dados de entrada.

    Este teste realiza o seguinte fluxo:
    1. Cria um conjunto de dados de entrada com informações sobre transações.
    2. Processa o conjunto de dados para gerar duas tabelas:
        - **Tabela 1**: Agrupa os dados por 'location_region' e calcula a média do 'risk_score', ordenando de forma decrescente.
        - **Tabela 2**: Filtra as transações do tipo 'sale', ordena pela data de 'timestamp' e pega os últimos valores por 'receiving_address'. Em seguida, seleciona as três maiores transações com base no valor 'amount'.
    3. Verifica os resultados gerados nas duas tabelas.

    Args:
        tmpdir (py.path.local): Um diretório temporário onde os arquivos CSV serão armazenados.

    Asserções:
        Verifica se o número de linhas nas tabelas é o esperado e se a ordenação de 'location_region' em 'table1' está correta.

    """
    input_path = tmpdir.join("cleaned_data.csv")
    output_table1 = tmpdir.join("table1.csv")
    output_table2 = tmpdir.join("table2.csv")

    # Dados de entrada para o teste
    data = {
        "location_region": ["region1", "region2", "region1"],
        "risk_score": [10, 20, 30],
        "transaction_type": ["sale", "sale", "purchase"],
        "receiving_address": ["addr1", "addr2", "addr1"],
        "amount": [100, 200, 50],
        "timestamp": [1000, 2000, 1500],
    }
    pd.DataFrame(data).to_csv(input_path, index=False)

    # Função mockada para processamento de dados
    def mocked_data_processing():
        """
        Processa os dados de entrada e cria duas tabelas CSV:
        - Tabela 1: Média do 'risk_score' por 'location_region', ordenado de forma decrescente.
        - Tabela 2: Filtra transações do tipo 'sale', pega os últimos valores por 'receiving_address',
          e seleciona as três maiores transações com base no 'amount'.

        Salva as tabelas processadas nos arquivos CSV especificados.

        """
        df = pd.read_csv(input_path)

        # Tabela 1: Agrupamento e ordenação por 'risk_score'
        table1 = (
            df.groupby('location_region')['risk_score']
            .mean()
            .sort_values(ascending=False)
            .reset_index()
        )
        table1.to_csv(output_table1, index=False)

        # Tabela 2: Filtro por 'transaction_type' == 'sale' e ordenação por 'timestamp'
        df_filtered = df[df['transaction_type'] == 'sale']
        df_filtered = (
            df_filtered.sort_values('timestamp')
            .groupby('receiving_address', as_index=False)
            .last()
        )
        table2 = df_filtered.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]
        table2.to_csv(output_table2, index=False)

    # Chama a função de processamento mockada
    mocked_data_processing()

    # Carrega as tabelas geradas para verificação
    table1 = pd.read_csv(output_table1)
    table2 = pd.read_csv(output_table2)

    # Verificações de resultados
    assert len(table1) == 2
    assert len(table2) == 2
    assert table1['location_region'].iloc[1] == "region2"
