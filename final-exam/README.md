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

======

## Задание 3. Работа с топиками Apache Kafka с помощью PySpark

### Цель
Настроить чтение топиков Kafka для реализации потоковой аналитики и разложить JSON в плоский вид.

### Шаги выполнения

1. Создан кластер Managed Service for Apache Kafka (`kafka788`, версия 3.9)
2. Создан топик `loan-applications`
3. Создан пользователь `kafka-user` с правами producer и consumer на топик
4. Написан producer (`kafka_producer.py`) — отправляет 50 000 JSON сообщений с данными о кредитных заявках в топик через SASL_SSL
5. Написан PySpark скрипт (`kafka_pyspark.py`) — читает сообщения из топика, разворачивает вложенный JSON в плоскую таблицу, сохраняет результат в Object Storage
6. Создан кластер Yandex Data Processing (`dataproc728`) и запущено PySpark задание
7. Результат сохранён в `etl002/output/kafka_loans/` — 50 000 строк

### Структура JSON топика

Входящие сообщения содержат вложенные объекты: `customer`, `loan`, `scoring`, `documents`. PySpark разворачивает их в плоскую таблицу со столбцами: `application_id`, `customer_id`, `region`, `loan_amount`, `term_months`, `credit_score`, `risk_level`, `decision_status`, `submitted_at`, `doc_type`, `doc_status`.

### Файлы
- `task3/kafka_producer.py` — отправка JSON в топик Kafka
- `task3/kafka_pyspark.py` — чтение и обработка данных из Kafka через PySpark
======

## Задание 4. Визуализация в DataLens

### Цель
Построить дашборд для визуализации загруженных данных с помощью Yandex DataLens.

### Шаги выполнения

1. Создано подключение типа **Файлы** в DataLens
2. Загружен CSV файл с агрегированными данными из `etl002/output/applications_agg/`
3. Создан датасет `applications_agg` на основе загруженного файла
4. Созданы три чарта:
   - **Заявки по типу продукта** — столбчатая диаграмма с разбивкой по статусу решения
   - **Средний кредитный скор по регионам** — столбчатая диаграмма с разбивкой по типу продукта
   - **Одобренные кредиты по регионам** — столбчатая диаграмма суммарных одобренных сумм
5. Все чарты собраны в дашборд `ETL Dashboard`

### Выводы из дашборда

- Распределение заявок по типам продуктов равномерное
- Кредитный скор примерно одинаков во всех регионах 
- Суммы одобренных кредитов распределены равномерно по регионам
