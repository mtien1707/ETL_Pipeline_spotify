
import datetime

import kafka
# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
import csv
from kafka import KafkaProducer,KafkaConsumer
import os
from datetime import date
import pandas as pd
import time
import json

# def my_producer():
if __name__ == '__main__':
    producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    topic = 'spotify_tiencm8_final'
    

    
    delta = datetime.timedelta(days=-2055)
    date1 =  date.today() + delta
    date_current = date1.strftime('%Y-%m-%d')
    
    #vòng lặp 1 năm
    for i in range(1,365):
        date2 = date1 +  datetime.timedelta(days=i)
        date_current1 = date2.strftime('%Y-%m-%d')
        df = pd.read_csv(f"E:\\data_spotify\\data\\{date_current1}.csv",encoding="utf-8")
        df1 = df.fillna(0)
        for row in df1.iterrows() :
            print("sent",row[1].to_dict())
            producer.send(topic, row[1].to_dict())
            producer.flush()

    


# dag = DAG('csvToKafka', description='spotiy data to kafka',
#           schedule_interval='@daily',
#           start_date=datetime(2022, 8, 13), catchup=False)
#
# Kafka_Producer = PythonOperator(
#     task_id='kafka_producer',
#     python_callable=my_producer,
#     dag=dag)

#
# Kafka_Producer