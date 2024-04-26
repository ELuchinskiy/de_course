# Use the base Python image
FROM python:3.8.13

# Install dependencies
RUN pip install --user psycopg2-binary==2.9.3 apache-airflow==2.5.1 pip install pandas==2.0.1 pip install redis==4.1.4 pip install pendulum==2.0.0 pip install clickhouse-sqlalchemy==0.2.0

# Set environment variables
RUN mkdir -p /usr/local/airflow/dags
ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

# Copy Airflow configuration file
COPY docker-compose.yml /app/docker-compose.yml
COPY airflow.cfg /usr/local/airflow/airflow.cfg
COPY lab05.py /usr/local/airflow/dags/lab05.py

# Use constraints file
ARG CONSTRAINTS_FILE=https://raw.githubusercontent.com/apache/airflow/constraints-2.5.1/constraints-3.8.txt
RUN pip install --no-cache-dir --user "apache-airflow[postgres]==2.5.1" --constraint "${CONSTRAINTS_FILE}"

# Set the working directory
WORKDIR /usr/local/airflow

CMD ["docker-compose", "up"]
