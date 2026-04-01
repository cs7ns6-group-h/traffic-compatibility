FROM python:3.11-slim

WORKDIR /app

# System dependencies for osmnx
RUN apt-get update && apt-get install -y \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

CMD ["uvicorn", "src.traffic_compatibility.main:app", "--host", "0.0.0.0", "--port", "8000"]