FROM python:3.11-slim

WORKDIR /app

# Sistem paketleri en üstte (layer cache için)
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Önce requirements (kod değişince pip tekrar çalışmasın)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sonra uygulama kodu
COPY . .

# Log klasörü
RUN mkdir -p /app/logs && touch /app/logs/cron.log

# Cron job — her gece 02:00'de ETL çalışır
RUN echo "0 2 * * * python /app/src/etl_pipeline.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/etl
RUN chmod 0644 /etc/cron.d/etl && crontab /etc/cron.d/etl

CMD ["sh", "-c", "cron && tail -f /app/logs/cron.log"]