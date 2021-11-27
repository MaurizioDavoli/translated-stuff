FROM apache/airflow

COPY requirements.txt .

RUN pip3 install -r requirements.txt

#for bodo3 
#RUN mkdir -p .aws
#COPY aws/credentials .aws
#COPY aws/config .aws

