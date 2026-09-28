FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt

# Constraints resmi Airflow mengunci numpy/pandas agar library model (scikit-learn)
# tidak meng-upgrade dependency yang dipakai Airflow.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-airflow.txt --constraint "${CONSTRAINTS_URL}" \
 && mkdir -p /opt/airflow/models   # dibuat sebagai user airflow -> volume model bisa ditulis
