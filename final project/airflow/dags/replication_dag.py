from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def copy_data():
    mongo = MongoHook(conn_id='mongo_default').get_conn()['user_analytics']
    
    data = {
        'sessions': list(mongo.user_sessions.find()),
        'events': list(mongo.event_logs.find()),
        'tickets': list(mongo.support_tickets.find()),
        'recommendations': list(mongo.user_recommendations.find()),
        'reviews': list(mongo.moderation_queue.find())
    }
    
    pg = PostgresHook(postgres_conn_id='postgres_default')
    
    stats = {
        'sessions': 0, 'events': 0, 'tickets': 0, 'recommendations': 0, 'reviews': 0
    }
    
    for s in data['sessions']:
        device_obj = s.get('device', {})
        device_type = list(device_obj.keys())[0] if device_obj else None
        
        existing = pg.get_first(
            "SELECT 1 FROM raw_data.user_sessions WHERE session_id = %s",
            parameters=(s['session_id'],)
        )
        
        if existing:
            pg.run("""
                UPDATE raw_data.user_sessions 
                SET user_id = %s, start_time = %s, end_time = %s, 
                    pages_visited = %s, device_type = %s, device_info = %s, 
                    actions = %s
                WHERE session_id = %s
            """, parameters=(
                s['user_id'],
                s.get('start_time'),
                s.get('end_time'),
                s.get('pages_visited', []),
                device_type,
                json.dumps(device_obj, default=json_serial),
                s.get('actions', []),
                s['session_id']
            ))
        else:
            pg.run("""
                INSERT INTO raw_data.user_sessions 
                (session_id, user_id, start_time, end_time, pages_visited, device_type, device_info, actions)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, parameters=(
                s['session_id'],
                s['user_id'],
                s.get('start_time'),
                s.get('end_time'),
                s.get('pages_visited', []),
                device_type,
                json.dumps(device_obj, default=json_serial),
                s.get('actions', [])
            ))
        stats['sessions'] += 1
    
    for e in data['events']:
        existing = pg.get_first(
            "SELECT 1 FROM raw_data.event_logs WHERE event_id = %s",
            parameters=(e['event_id'],)
        )
        
        if existing:
            pg.run("""
                UPDATE raw_data.event_logs 
                SET timestamp = %s, event_type = %s, details = %s
                WHERE event_id = %s
            """, parameters=(
                e.get('timestamp'),
                e.get('event_type'),
                json.dumps(e.get('details', {}), default=json_serial),
                e['event_id']
            ))
        else:
            pg.run("""
                INSERT INTO raw_data.event_logs 
                (event_id, timestamp, event_type, details)
                VALUES (%s, %s, %s, %s)
            """, parameters=(
                e['event_id'],
                e.get('timestamp'),
                e.get('event_type'),
                json.dumps(e.get('details', {}), default=json_serial)
            ))
        stats['events'] += 1
    
    for t in data['tickets']:
        existing = pg.get_first(
            "SELECT 1 FROM raw_data.support_tickets WHERE ticket_id = %s",
            parameters=(t['ticket_id'],)
        )
        
        messages_json = json.dumps(t.get('messages', []), default=json_serial)
        
        if existing:
            pg.run("""
                UPDATE raw_data.support_tickets 
                SET user_id = %s, status = %s, issue_type = %s, 
                    messages = %s, created_at = %s, updated_at = %s
                WHERE ticket_id = %s
            """, parameters=(
                t['user_id'],
                t['status'],
                t.get('issue_type'),
                messages_json,
                t.get('created_at'),
                t.get('updated_at'),
                t['ticket_id']
            ))
        else:
            pg.run("""
                INSERT INTO raw_data.support_tickets 
                (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, parameters=(
                t['ticket_id'],
                t['user_id'],
                t['status'],
                t.get('issue_type'),
                messages_json,
                t.get('created_at'),
                t.get('updated_at')
            ))
        stats['tickets'] += 1
    
    for r in data['recommendations']:
        existing = pg.get_first(
            "SELECT 1 FROM raw_data.user_recommendations WHERE user_id = %s",
            parameters=(r['user_id'],)
        )
        
        if existing:
            pg.run("""
                UPDATE raw_data.user_recommendations 
                SET recommended_products = %s, last_updated = %s
                WHERE user_id = %s
            """, parameters=(
                r.get('recommended_products', []),
                r.get('last_updated'),
                r['user_id']
            ))
        else:
            pg.run("""
                INSERT INTO raw_data.user_recommendations 
                (user_id, recommended_products, last_updated)
                VALUES (%s, %s, %s)
            """, parameters=(
                r['user_id'],
                r.get('recommended_products', []),
                r.get('last_updated')
            ))
        stats['recommendations'] += 1
    
    for rev in data['reviews']:
        existing = pg.get_first(
            "SELECT 1 FROM raw_data.moderation_queue WHERE review_id = %s",
            parameters=(rev['review_id'],)
        )
        
        if existing:
            pg.run("""
                UPDATE raw_data.moderation_queue 
                SET user_id = %s, product_id = %s, review_text = %s, 
                    rating = %s, moderation_status = %s, flags = %s, submitted_at = %s
                WHERE review_id = %s
            """, parameters=(
                rev['user_id'],
                rev['product_id'],
                rev.get('review_text'),
                rev.get('rating'),
                rev['moderation_status'],
                rev.get('flags', []),
                rev.get('submitted_at'),
                rev['review_id']
            ))
        else:
            pg.run("""
                INSERT INTO raw_data.moderation_queue 
                (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, parameters=(
                rev['review_id'],
                rev['user_id'],
                rev['product_id'],
                rev.get('review_text'),
                rev.get('rating'),
                rev['moderation_status'],
                rev.get('flags', []),
                rev.get('submitted_at')
            ))
        stats['reviews'] += 1
    
  

dag = DAG(
    'mongo_replication',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1
    },
    schedule_interval='*/10 * * * *',
    catchup=False,
)

replication_task = PythonOperator(
    task_id='copy_all_collections',
    python_callable=copy_data,
    dag=dag,
)