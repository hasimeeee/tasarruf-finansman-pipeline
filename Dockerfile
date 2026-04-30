FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN apt-get update && apt-get install -y cron

# 🔥 önce klasör
RUN mkdir -p /app/logs

# sonra cron job
RUN echo "0 2 * * * python /app/src/etl_pipeline.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/etl

RUN chmod 0644 /etc/cron.d/etl
RUN crontab /etc/cron.d/etl

# 🔥 sonra log dosyası
RUN touch /app/logs/cron.log

# cron'u foreground gibi tut
CMD cron && tail -f /app/logs/cron.log