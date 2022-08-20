import datetime as dt
from datetime import datetime,timedelta,date
import kafka
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import csv
from kafka import KafkaProducer,KafkaConsumer
import os

import pandas as pd
import time
import json

def my_producer():
    # if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers = ['172.17.80.28:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    topic = 'spotify_tiencm8_1'
    file_r = open('/home/chibm/airflow/dags/count.txt', 'r')
    checkpoin = int(file_r.read().strip()) + 1
    file_r.close()
    file_w = open('/home/chibm/airflow/dags/count.txt', 'w')
    file_w.write(str(checkpoin))
    file_w.close()



    delta = dt.timedelta(days=-2057)
    delta1 = dt.timedelta(days=checkpoin)

    date1 =  date.today() + delta + delta1
    date_current = date1.strftime('%Y-%m-%d')


    # for i in range(-2,365):
    # date2 = date1 +  dt.timedelta(days=i)
    # date_current1 = date2.strftime('%Y-%m-%d')
    df = pd.read_csv(f"/home/tiencm8/data1/{date_current}.csv",encoding="utf-8")
    df1 = df.fillna(0)
    for row in df1.iterrows() :
        producer.send(topic, row[1].to_dict())
        producer.flush()




dag = DAG('Kafka_spotify_test', description='spotiy data to kafka test',
          schedule_interval='*/2 * * * *',
          start_date=datetime(2022, 8, 20), catchup=False)

Kafka_Producer = PythonOperator(
    task_id='kafka_producer',
    python_callable=my_producer,
    dag=dag)


Kafka_Producer