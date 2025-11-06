FROM python:3.9-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копирование файлов
COPY requirements.txt .
COPY src/ ./src/

COPY service_acc.json .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Запуск скрипта
CMD ["python", "-u", "src/sheets_sync.py"]