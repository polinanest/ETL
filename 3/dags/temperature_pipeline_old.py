from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np

def simple_temperature_analysis():
    
    df = pd.read_csv('/opt/airflow/dags/IOT-temp.csv', header=0)
    
    df = df[df['out/in'] == 'In']
    
    df['date_dt'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df['date'] = df['date_dt'].dt.strftime('%Y-%m-%d')
    
    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    
    if len(df) >= 2:
        p5, p95 = np.percentile(df['temp'], [5, 95])
        df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]
    
    daily = df.groupby('date')['temp'].mean().round(2).reset_index()
    
    hottest = daily.nlargest(5, 'temp')
    coldest = daily.nsmallest(5, 'temp')
    
    print("5 самых жарких дней:")
    for i, row in hottest.iterrows():
        print(f"{row['date']} {row['temp']}")
    
    print("\n5 самых холодных дней:")
    for i, row in coldest.iterrows():
        print(f"{row['date']} {row['temp']}")
    
    return 'Готово'

with DAG(
    dag_id='temp_analysis',
    description='Анализ температур',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='run_analysis',
        python_callable=simple_temperature_analysis
    )