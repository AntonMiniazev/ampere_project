FROM apache/airflow:3.0.4-python3.12

# install provider and dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-mssql[common.sql] \
    apache-airflow-providers-amazon[apache.hive] \
    pyodbc~=5.2.0

USER root
# Initialize msodbcsql18
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Use custom requirements
COPY requirements.txt /requirements.txt

# Install additional dependencies
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH="/opt/airflow/dags/repo/dags"