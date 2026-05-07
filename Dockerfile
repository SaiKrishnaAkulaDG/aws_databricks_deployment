FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python3 -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs; INSTALL parquet;'); c.close()"

COPY . .
