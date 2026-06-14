from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_pets_from_json():
    import json
    import psycopg2
    
    with open('/opt/airflow/dags/pets-data.json', 'r') as f:
        data = json.load(f)
    
    pets = data['pets']
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pets (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            species VARCHAR(50),
            fav_foods TEXT,
            birth_year INTEGER,
            photo_url TEXT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("DELETE FROM pets")
    
    for pet in pets:
        fav_foods = ", ".join(pet.get('favFoods', [])) if 'favFoods' in pet else ""
        
        cursor.execute("""
            INSERT INTO pets (name, species, fav_foods, birth_year, photo_url)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            pet['name'],
            pet['species'],
            fav_foods,
            pet['birthYear'],
            pet['photo']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='load_pets_pipeline',
    description='Загружает питомцев из JSON в PostgreSQL',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['simple', 'pets', 'json']
) as dag:
    
    load_task = PythonOperator(
        task_id='load_pets_to_postgres',
        python_callable=load_pets_from_json
    )