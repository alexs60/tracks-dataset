FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "workers/run_pipeline.py", "--batch-size", "10", "--interval", "60"]
