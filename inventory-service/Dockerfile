FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["gunicorn", "--workers", "4", "--threads", "16", "--bind", "0.0.0.0:5002", "app:app"]
