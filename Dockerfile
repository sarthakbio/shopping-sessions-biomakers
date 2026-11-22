# Shopping Sessions - team Biomakers (DuckDB)
FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
RUN pip install --no-cache-dir duckdb==1.5.6
COPY pipeline.py /app/pipeline/pipeline.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh && chmod -R a+rX /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/app/run.sh"]
