# ETL Exam

Отчет со скриншотами: https://docs.google.com/document/d/154F5yZTRu18_ffVWyYqQWNv4W-30AZCv1LVexSTxeW0/edit?usp=sharing

## Задание 1. Работа с Yandex DataTransfer

### Цель

Перенос данных из Managed Service for YDB в Object Storage с помощью Data Transfer.

### Шаги выполнения

1. Создана база данных YDB (Serverless) — `ydb857`

2. Создана таблица `transactions_v2` и загружено 400 000 строк (38.7 МБ)

3. Создан бакет Object Storage — `etl002`

4. Настроены эндпоинты DataTransfer: источник YDB → приёмник Object Storage

5. Трансфер успешно выполнен, данные выгружены в файл `part-1781461123-c21f969b.00000.csv`

### Структура таблицы transactions_v2

| Поле | Тип | Описание |

|------|-----|----------|

| call_id | Text | Уникальный ID звонка (PK) |

| call_time | Text | Время звонка |

| client_id | Text | ID клиента |

| region_code | Text | Код региона |

| campaign_type | Text | Тип кампании |

| call_status | Text | Статус звонка |

| client_response | Text | Ответ клиента |

| duration_sec | Int32 | Длительность в секундах |

| follow_up_required | Text | Требуется ли повторный звонок |

### SQL скрипты

- `task1/01_create_table.yql` — создание таблицы

- `task1/02_insert_sample.yql` — вставка тестовых данных

=======

## Задание 2. Автоматизация Yandex Data Processing с Apache Airflow

### Цель
Автоматическая обработка файлов из Object Storage с помощью PySpark через Airflow DAG.

### Шаги выполнения

1. Сгенерирован файл `applications.csv` (54.8 МБ, 500 000 строк) и загружен в `etl002/input/`
2. Написан PySpark скрипт `pyspark_job.py` — читает CSV, считает агрегаты по регионам/продуктам/решениям, сохраняет результат в `etl002/output/applications_agg/`
3. Скрипт загружен в `etl002/scripts/`
4. Создан Managed Airflow кластер `airflow211` (версия 2.10)
5. DAG `DATA_INGEST` выполняет 3 шага: создать кластер DataProc → запустить PySpark → удалить кластер
6. DAG успешно выполнен, результат в бакете `etl002/output/applications_agg/`

### Структура DAG

| Таск | Оператор | Описание |
|------|----------|----------|
| create_cluster | DataprocCreateClusterOperator | Создаёт кластер Spark |
| run_pyspark | DataprocCreatePysparkJobOperator | Запускает PySpark задание |
| delete_cluster | DataprocDeleteClusterOperator | Удаляет кластер |

### Файлы
- `task2/dag_data_ingest.py` — DAG файл
- `task2/pyspark_job.py` — PySpark задание
EOF

