FROM python:3.10-slim
RUN pip install --no-cache-dir "psycopg[binary]>=3.2"
COPY traffic /app/traffic
WORKDIR /app
CMD ["python", "-m", "traffic", "run"]
