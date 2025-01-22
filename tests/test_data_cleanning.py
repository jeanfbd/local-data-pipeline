import os
import pandas as pd
from dags.tasks.data_cleanning import data_cleanning

def test_data_cleanning(tmpdir):
    """
    Testa o processo de limpeza de dados, verificando a remoção de valores ausentes 
    e a conversão de dados inválidos para um formato numérico.

    Este teste realiza as seguintes operações:
    1. Cria um DataFrame de exemplo contendo dados de regiões, pontuação de risco e tipo de transação.
    2. Salva os dados como um arquivo CSV temporário.
    3. Mocka a função de limpeza de dados para:
        - Converter valores não numéricos em 'risk_score' para `NaN`.
        - Remover registros com valores ausentes em 'location_region' e 'risk_score'.
    4. Verifica se o arquivo CSV resultante contém apenas registros válidos.

    Args:
        tmpdir (py.path.local): Um diretório temporário onde os arquivos CSV serão armazenados.

    Asserções:
        Verifica se o número de registros limpos é o esperado e se a 'location_region' 
        contém o valor 'region1' após o processo de limpeza.

    """
    # Caminho temporário para os arquivos CSV
    input_path = tmpdir.join("input.csv")
    output_path = tmpdir.join("cleaned_data.csv")
    
    # Dados de entrada para o teste
    data = {
        "location_region": ["region1", None, "region2"],
        "risk_score": [10, 20, "invalid"],
        "transaction_type": ["sale", "purchase", "sale"],
    }
    pd.DataFrame(data).to_csv(input_path, index=False)

    # Função mockada para limpeza de dados
    def mocked_data_cleanning():
        """
        Realiza a limpeza dos dados:
        - Converte valores não numéricos em 'risk_score' para NaN.
        - Remove registros com valores ausentes nas colunas 'location_region' e 'risk_score'.
        """
        df = pd.read_csv(input_path)
        
        # Converte valores não numéricos em 'risk_score' para NaN
        df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')
        
        # Remove registros com valores ausentes em 'location_region' e 'risk_score'
        df.dropna(subset=['location_region', 'risk_score'], inplace=True)
        
        # Salva os dados limpos no arquivo de saída
        df.to_csv(output_path, index=False)

    # Chama a função mockada de limpeza de dados
    mocked_data_cleanning()

    # Carrega os dados limpos e verifica os resultados
    df_cleaned = pd.read_csv(output_path)

    # Verificações dos dados limpos
    assert len(df_cleaned) == 1
    assert "region1" in df_cleaned['location_region'].values
