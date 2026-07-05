FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["gunicorn", "--workers", "4", "--threads", "8", "--bind", "0.0.0.0:5001", "app:app"]
