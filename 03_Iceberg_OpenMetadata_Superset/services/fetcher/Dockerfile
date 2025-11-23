FROM python:3.11-slim

WORKDIR /app

# Install system deps for pandas/pyarrow
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_elering.py /app/

ENV PYTHONUNBUFFERED=1

CMD ["python", "fetch_elering.py"]
