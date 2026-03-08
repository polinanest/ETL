from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2

def full_load():
    df = pd.read_csv('/opt/airflow/dags/IOT-temp.csv', header=0)
    
    df = df[df['out/in'] == 'In']
    
    df['date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce').dt.strftime('%Y-%m-%d')
    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    df = df.dropna(subset=['date', 'temp'])
    
    if len(df) >= 2:
        p5, p95 = np.percentile(df['temp'], [5, 95])
        df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]
    
    date_stats = df.groupby('date')['temp'].agg(['max', 'min']).reset_index()
    
    hottest_dates = date_stats.nlargest(5, 'max')['date'].tolist()
    coldest_dates = date_stats.nsmallest(5, 'min')['date'].tolist()
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temperature_results (
            date DATE PRIMARY KEY,
            max_temp DECIMAL(5,2),
            min_temp DECIMAL(5,2),
            is_hottest BOOLEAN DEFAULT FALSE,
            is_coldest BOOLEAN DEFAULT FALSE
        )
    """)
    
    cursor.execute("DELETE FROM temperature_results")
    
    for _, row in date_stats.iterrows():
        is_hottest = row['date'] in hottest_dates
        is_coldest = row['date'] in coldest_dates
        cursor.execute("""
            INSERT INTO temperature_results (date, max_temp, min_temp, is_hottest, is_coldest)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['date'], row['max'], row['min'], is_hottest, is_coldest))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    
    return "Полная загрузка завершена"

def incremental_load():
    df = pd.read_csv('/opt/airflow/dags/IOT-temp.csv', header=0)
    
    df = df[df['out/in'] == 'In']
    
    df['date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce').dt.strftime('%Y-%m-%d')
    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    df = df.dropna(subset=['date', 'temp'])
    
    if len(df) >= 2:
        p5, p95 = np.percentile(df['temp'], [5, 95])
        df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]
    
    recent_dates = sorted(df['date'].unique())[-3:]
    df_recent = df[df['date'].isin(recent_dates)]
    
    if len(df_recent) == 0:
        return "Нет новых данных"
    
    recent_stats = df_recent.groupby('date')['temp'].agg(['max', 'min']).reset_index()
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temperature_results (
            date DATE PRIMARY KEY,
            max_temp DECIMAL(5,2),
            min_temp DECIMAL(5,2),
            is_hottest BOOLEAN DEFAULT FALSE,
            is_coldest BOOLEAN DEFAULT FALSE
        )
    """)
    
    for _, row in recent_stats.iterrows():
        cursor.execute("""
            INSERT INTO temperature_results (date, max_temp, min_temp, is_hottest, is_coldest)
            VALUES (%s, %s, %s, FALSE, FALSE)
            ON CONFLICT (date) 
            DO UPDATE SET max_temp = EXCLUDED.max_temp, min_temp = EXCLUDED.min_temp
        """, (row['date'], row['max'], row['min']))
    
    cursor.execute("SELECT date, max_temp FROM temperature_results ORDER BY max_temp DESC LIMIT 5")
    hottest = [row[0] for row in cursor.fetchall()]
    for date in hottest:
        cursor.execute("UPDATE temperature_results SET is_hottest = TRUE WHERE date = %s", (date,))
    
    cursor.execute("SELECT date, min_temp FROM temperature_results ORDER BY min_temp ASC LIMIT 5")
    coldest = [row[0] for row in cursor.fetchall()]
    for date in coldest:
        cursor.execute("UPDATE temperature_results SET is_coldest = TRUE WHERE date = %s", (date,))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    
    return "Инкрементальная загрузка завершена"

with DAG(
    dag_id='temperature_full_load',
    description='Полная загрузка исторических данных',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag_full:
    
    full_task = PythonOperator(
        task_id='full_load_task',
        python_callable=full_load
    )

with DAG(
    dag_id='temperature_incremental_load',
    description='Инкрементальная загрузка последних дней',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 1 * * *',
    catchup=False
) as dag_inc:
    
    inc_task = PythonOperator(
        task_id='incremental_load_task',
        python_callable=incremental_load
    )