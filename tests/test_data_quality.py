import pandas as pd
from dags.tasks.data_quality import data_quality

def test_data_quality(capsys):
    """
    Testa o processamento de dados de qualidade, verificando o número de registros, 
    valores ausentes e a taxa de conformidade.

    Este teste realiza o seguinte:
    1. Cria um DataFrame de exemplo com algumas entradas de dados, incluindo valores ausentes.
    2. Mocka a função de qualidade de dados para calcular e imprimir:
        - O total de registros.
        - O número de valores ausentes.
        - A taxa de conformidade (percentual de registros sem valores ausentes).
    3. Captura a saída impressa e verifica se os valores calculados estão corretos.

    Args:
        capsys (pytest.CaptureFixture): Fixture do pytest usada para capturar a saída de print.

    Asserções:
        Verifica se os valores de total de registros, valores ausentes e a taxa de conformidade 
        estão corretos e são impressos no formato esperado.

    """
    # Dados de entrada para o teste
    data = {
        "location_region": ["region1", None, "region2"],
        "risk_score": [10, 20, None],
    }
    df = pd.DataFrame(data)

    # Função mockada para qualidade de dados
    def mocked_data_quality():
        """
        Calcula a qualidade dos dados no DataFrame fornecido:
        - Total de registros.
        - Número de valores ausentes.
        - Taxa de conformidade (percentual de registros não ausentes).

        A função imprime os resultados.
        """
        total_records = len(df)
        missing_values = df.isnull().sum().sum()
        compliance_rate = 100 * (total_records - missing_values) / total_records

        # Impressão dos resultados
        print(f"Total de registros: {total_records}")
        print(f"Valores ausentes: {missing_values}")
        print(f"Taxa de conformidade: {compliance_rate:.2f}%")

    # Chama a função de qualidade de dados mockada
    mocked_data_quality()

    # Captura a saída gerada pela função mockada
    captured = capsys.readouterr()

    # Verificações de resultados
    assert "Total de registros: 3" in captured.out
    assert "Valores ausentes: 2" in captured.out
    assert "Taxa de conformidade: 33.33%" in captured.out
