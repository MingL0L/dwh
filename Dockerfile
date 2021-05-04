FROM apache/airflow:2.0.1
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
RUN pip install s3fs==0.4.0
