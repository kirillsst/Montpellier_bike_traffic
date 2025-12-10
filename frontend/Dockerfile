FROM python:3.12-slim

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копируем весь frontend
COPY . .

# Указываем PYTHONPATH на текущую папку
ENV PYTHONPATH=/app

# Открываем порт
EXPOSE 8001

# Запуск uvicorn
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
