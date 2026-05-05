FROM python:3.11-slim

WORKDIR /app

RUN pip install kafka-python

COPY infra/create_topics.py /app/create_topics.py

CMD ["python", "create_topics.py"]