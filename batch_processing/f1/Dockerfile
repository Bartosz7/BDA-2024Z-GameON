FROM python:3.9-slim

WORKDIR /app

COPY vertex_utils.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python3"]