# Используем базовый образ python:3.11-slim
FROM python:3.11-slim

# Устанавливаем системные зависимости для pyzbar (libzbar)
RUN apt-get update && apt-get install -y \
    libzbar0 \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем все файлы в рабочую директорию
COPY . /app

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Запускаем приложение
CMD ["python", "favor2025.py"]
