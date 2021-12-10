FROM apache/airflow

COPY requirements.txt .

RUN pip3 install -r requirements.txt


