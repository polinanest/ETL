"""
Отправка JSON сообщений в топик Kafka.
Установка: pip install kafka-python
Запуск: python3 kafka_producer.py
"""

import os
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "rc1a-3k1ta5efkqovuo83.mdb.yandexcloud.net:9091"
TOPIC = "loan-applications"
KAFKA_USER = "kafka-user"
KAFKA_PASSWORD = "admin123"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    ssl_cafile=os.path.expanduser("~/yandex-certs/YandexInternalRootCA.crt"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

regions = ["DE-HE", "DE-BY", "DE-NW", "DE-BW", "DE-NI"]
risk_levels = ["low", "medium", "high"]
decisions = ["approved", "rejected", "manual_review"]
doc_types = ["passport", "income_statement", "tax_return"]
doc_statuses = ["verified", "pending", "rejected"]

total = 0
for i in range(1, 50001):
    msg = {
        "application_id": f"loan_{700000 + i}",
        "customer": {
            "customer_id": f"cust_{random.randint(100, 999)}",
            "region": random.choice(regions)
        },
        "loan": {
            "amount": random.randint(5000, 50000),
            "term_months": random.choice([12, 24, 36, 48, 60])
        },
        "scoring": {
            "score": random.randint(300, 850),
            "risk_level": random.choice(risk_levels)
        },
        "documents": [
            {
                "type": random.choice(doc_types),
                "status": random.choice(doc_statuses)
            }
        ],
        "decision_status": random.choice(decisions),
        "submitted_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    producer.send(TOPIC, value=msg)
    total += 1
    if total % 1000 == 0:
        producer.flush()
        print(f"Отправлено: {total} сообщений")

producer.flush()
producer.close()
print(f"Готово! Всего отправлено: {total} сообщений")
