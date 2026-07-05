FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Run with gunicorn for production-like concurrency (important for bulkhead testing)
CMD ["gunicorn", "--workers", "4", "--threads", "8", "--bind", "0.0.0.0:5000", "app:app"]
