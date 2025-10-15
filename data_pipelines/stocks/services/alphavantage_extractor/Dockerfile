FROM python:3.13-slim

ENV PYTHONUNBUFFERED=True

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .


CMD ["/usr/local/bin/python3", "main.py"]