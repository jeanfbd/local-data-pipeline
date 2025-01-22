FROM apache/airflow:latest-python3.8
USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags

USER airflow
RUN pip install --upgrade pip
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org boto3

COPY ./dags /opt/airflow/dags
COPY ./src /opt/airflow/src
COPY ./data /opt/airflow/data

USER ${AIRFLOW_UID}