FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RUN_ON_STARTUP=true

EXPOSE 8080

CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8080", "--timeout", "180"]
