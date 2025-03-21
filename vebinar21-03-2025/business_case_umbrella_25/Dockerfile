# Базовый образ Airflow
FROM apache/airflow:2.0.0-python3.8

USER root

# Установка необходимых библиотек
RUN pip install --no-cache-dir pandas scikit-learn joblib requests
RUN pip install azure-storage-blob==12.8.1

# Создание каталогов и установка прав
RUN mkdir -p /opt/airflow/data /opt/airflow/logs \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs

USER airflow