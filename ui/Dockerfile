FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit pandas plotly clickhouse-connect streamlit-autorefresh

COPY ui/ /app/

EXPOSE 8501

CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]