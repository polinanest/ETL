
# ETL Exam

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

