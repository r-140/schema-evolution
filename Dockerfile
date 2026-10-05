FROM python:3.12-slim
WORKDIR /lab
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt
COPY app app
COPY dbt dbt
COPY contracts contracts
ENV PYTHONPATH=/lab DBT_PROFILES_DIR=/lab/dbt PYTHONUNBUFFERED=1
CMD ["python", "-m", "app.service"]
