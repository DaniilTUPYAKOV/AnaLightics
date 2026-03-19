# 🚀 Analightics

![Python](https://img.shields.io/badge/python-3.11-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100-green)
![Kafka](https://img.shields.io/badge/Kafka-3.5-red)
![ClickHouse](https://img.shields.io/badge/ClickHouse-23.8-yellow)

**High-performance Web Analytics System** designed to handle high throughput event streams.

## 💡 О проекте
Analightics — это легковесная система сбора и анализа событий (clickstream).
Цель проекта: создать отказоустойчивый пайплайн обработки данных, способный пережить падение базы данных или пиковые нагрузки, не потеряв события.

**Ключевые особенности:**
- **Асинхронный API** на FastAPI для приема событий с минимальной задержкой.
- **Буферизация через Kafka** для сглаживания пиков нагрузки (Backpressure).
- **Batch Processing:** Consumer накапливает данные и пишет в ClickHouse пачками (bulk insert) для максимальной производительности.
- **Reliability:** Реализован механизм **Dead Letter Queue (DLQ)**. Если ClickHouse недоступен, данные не теряются, а уходят в отдельный топик Kafka для последующей обработки.
- **Infrastructure as Code:** Полное развертывание через `docker-compose`.
- **Auto-Migrations:** Система автоматически обновляет схему таблицы в ClickHouse при изменении Pydantic-моделей.
