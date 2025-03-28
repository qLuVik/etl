# Параметры
CONTAINER_NAME="business_case_rocket_25-webserver-1"
AIRFLOW_DATA_DIR="/opt/airflow/data"
HOST_EXPORT_DIR="$HOME/airflow_data_export"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")


mkdir -p "$HOST_EXPORT_DIR/$TIMESTAMP"


# 1. Копируем JSON файлы
echo "Exporting JSON files..."
docker cp "$CONTAINER_NAME:$AIRFLOW_DATA_DIR/launches.json" "$HOST_EXPORT_DIR/$TIMESTAMP/launches.json"

# 2. Копируем изображения
echo "Exporting images..."
docker cp "$CONTAINER_NAME:$AIRFLOW_DATA_DIR/images/" "$HOST_EXPORT_DIR/$TIMESTAMP/images/"



# Проверяем результат
echo "--------------------------------------------"
echo "Export completed successfully!"
echo "Exported data location: $HOST_EXPORT_DIR/$TIMESTAMP"
echo "Contents:"
ls -lh "$HOST_EXPORT_DIR/$TIMESTAMP"