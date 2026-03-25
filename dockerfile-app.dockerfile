FROM python:3.11-slim

WORKDIR /app

COPY requirements-runtime.txt /app/requirements-runtime.txt
RUN pip install --no-cache-dir -r /app/requirements-runtime.txt

COPY . /app
