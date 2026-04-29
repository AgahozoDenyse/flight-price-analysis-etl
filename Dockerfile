ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.1
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-fab \
    apache-airflow-providers-mysql \
    apache-airflow-providers-postgres \
    pandas \
    pymysql

