#!/bin/bash

# Настройки
DAG_ID="download_rocket_local"
CONTAINER_NAME="business_case_rocket_25-webserver-1"  # или "airflow-scheduler"
OUTPUT_FILE="./data/dag_details_${DAG_ID}.json"

# Проверяем, что контейнер работает
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo "Ошибка: Контейнер $CONTAINER_NAME не запущен!"
    exit 1
fi

# Выполняем команду в контейнере для экспорта DAG
docker exec -it $CONTAINER_NAME \
    bash -c "airflow dags list-runs --dag-id $DAG_ID --output json > /opt/airflow/$OUTPUT_FILE"

# Проверяем результат
if [ $? -eq 0 ]; then
    echo "Детали DAG $DAG_ID успешно сохранены в $OUTPUT_FILE"
else
    echo "Ошибка при экспорте DAG $DAG_ID"
    exit 1
fi