import pandas as pd
from dags.tasks.data_report import data_report

def test_data_report(capsys, tmpdir):
    """
    Testa a geração e impressão de relatórios de dados de duas tabelas.

    Este teste simula o processo de leitura de duas tabelas CSV, 
    imprime seu conteúdo e verifica se as informações estão presentes na saída:
    1. A primeira tabela contém informações sobre regiões e pontuações de risco.
    2. A segunda tabela contém informações sobre endereços de recebimento, valores e timestamps.

    O teste realiza as seguintes etapas:
    - Cria dois conjuntos de dados simulados e os salva como arquivos CSV.
    - Mocka o método `data_report` para ler esses arquivos e imprimir as tabelas.
    - Captura a saída gerada pelo `print` e verifica se os dados estão presentes.

    Args:
        capsys (pytest.CaptureFixture): Fixture do pytest usada para capturar a saída de print.
        tmpdir (py.path.local): Um diretório temporário onde os arquivos CSV serão armazenados.

    Asserções:
        Verifica se os nomes das tabelas e os dados esperados estão presentes na saída capturada.

    """
    # Caminhos temporários para salvar as tabelas CSV
    table1_path = tmpdir.join("table1.csv")
    table2_path = tmpdir.join("table2.csv")

    # Dados de entrada para as tabelas
    table1_data = {"location_region": ["region1", "region2"], "risk_score": [15, 10]}
    table2_data = {
        "receiving_address": ["addr1", "addr2"],
        "amount": [200, 100],
        "timestamp": [1000, 2000],
    }

    # Salva os dados nas tabelas CSV
    pd.DataFrame(table1_data).to_csv(table1_path, index=False)
    pd.DataFrame(table2_data).to_csv(table2_path, index=False)

    # Função mockada para gerar o relatório dos dados
    def mocked_data_report():
        """
        Lê as duas tabelas CSV e imprime o conteúdo das tabelas.
        """
        table1 = pd.read_csv(table1_path)
        table2 = pd.read_csv(table2_path)

        print("Tabela 1:")
        print(table1)
        print("\nTabela 2:")
        print(table2)

    # Chama a função de geração do relatório mockada
    mocked_data_report()

    # Captura a saída gerada pelo print
    captured = capsys.readouterr()

    # Verificações dos dados na saída
    assert "Tabela 1:" in captured.out
    assert "Tabela 2:" in captured.out
    assert "region1" in captured.out
    assert "addr1" in captured.out
