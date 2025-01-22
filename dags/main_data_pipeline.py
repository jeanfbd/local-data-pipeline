from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.data_cleanning import data_cleanning
from tasks.data_processing import data_processing
from tasks.data_quality import data_quality
from tasks.data_report import data_report

# Definição do DAG
with DAG(
    "main_data_pipeline",
    description="Pipeline principal para processar e analisar dados",
    schedule_interval=None,
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    # '''
    # Definição do DAG principal do pipeline de dados.

    # Este DAG executa uma série de tarefas para limpar, processar, avaliar a qualidade e gerar relatórios a partir de dados.

    # O fluxo de tarefas é o seguinte:
    # 1. **Data Cleaning**: Limpeza dos dados de entrada, incluindo a remoção de valores ausentes ou inválidos.
    # 2. **Data Processing**: Processamento dos dados limpos, incluindo transformações e agrupamentos.
    # 3. **Data Quality**: Avaliação da qualidade dos dados processados, verificando a conformidade e valores ausentes.
    # 4. **Data Report**: Geração de relatórios sobre os dados processados e limpos.

    # O DAG é configurado para ser executado manualmente (`schedule_interval=None`), com início em 1º de dezembro de 2023. 
    # O parâmetro `catchup=False` garante que o DAG não será executado retroativamente.

    # '''

    task_cleaning = PythonOperator(
        task_id="data_cleanning",
        python_callable=data_cleanning,
        
        # Tarefa para limpar os dados de entrada.

        # A função `data_cleanning` realiza a limpeza dos dados, removendo registros com valores ausentes
        # e corrigindo ou transformando dados inválidos.

        # Dependências:
        # - Nenhuma. Esta tarefa é executada primeiro no pipeline.
        
    )

    task_processing = PythonOperator(
        task_id="data_processing",
        python_callable=data_processing,
        # '''
        # Tarefa para processar os dados limpos.

        # A função `data_processing` transforma e processa os dados limpos para gerar as tabelas ou 
        # resultados desejados, como agregações e transformações.
        
        # Dependências:
        # - `data_cleaning` (task_cleaning)
        # '''
    )

    task_quality = PythonOperator(
        task_id="data_quality",
        python_callable=data_quality,
        # '''
        # Tarefa para avaliar a qualidade dos dados.

        # A função `data_quality` calcula métricas de qualidade dos dados, como a taxa de conformidade 
        # e a quantidade de valores ausentes.
        
        # Dependências:
        # - `data_processing` (task_processing)
        # '''
    )

    task_report = PythonOperator(
        task_id="data_report",
        python_callable=data_report,
    #     '''
    #     Tarefa para gerar relatórios a partir dos dados processados.

    #     A função `data_report` gera relatórios detalhados sobre os dados processados, podendo incluir 
    #     informações agregadas e insights relevantes.
        
    #     Dependências:
    #     - `data_quality` (task_quality)
    #    '''
    )

    # Definindo dependências
    task_cleaning >> task_processing >> task_quality >> task_report
    '''
    Definição da ordem das tarefas no pipeline:
    1. `task_cleaning` é executado primeiro.
    2. `task_processing` depende da conclusão de `task_cleaning`.
    3. `task_quality` depende da conclusão de `task_processing`.
    4. `task_report` depende da conclusão de `task_quality`.
    '''
