# Event-Driven-Change-Data-Capture-for-a-Relational-DatabasePerfect.

A production-style **event-driven Change Data Capture system** built using **MySQL, Kafka, Docker, and Python**, implementing deterministic watermark tracking, reliable event publishing, and unit-tested components.

---

## 🚀 Architecture Overview

```
            +-------------+
            |   MySQL     |
            |  (Source DB)|
            +-------------+
                    |
                    | Polling (last_updated watermark)
                    v
        +----------------------+
        |   CDC Service        |
        |  (Python Producer)   |
        +----------------------+
                    |
                    | Reliable Kafka Producer
                    v
              +-----------+
              |   Kafka   |
              +-----------+
                    |
                    v
          +------------------+
          | Kafka Consumer   |
          +------------------+
```

---

## 🧩 Tech Stack

* **MySQL 8.0** – Source database
* **Apache Kafka** – Event streaming platform
* **Zookeeper** – Kafka coordination
* **Python 3.11** – CDC service & consumer
* **Docker & Docker Compose** – Container orchestration
* **Pytest** – Unit testing

---

## 🎯 Key Features

### ✅ Deterministic Watermark Tracking

* Uses `last_updated` column
* Persists state in `state.json`
* Ensures idempotent processing
* Safe restarts without data loss

### ✅ Reliable Kafka Producer

* Retry logic with exponential wait
* `acks="all"` for durability
* Controlled batching with `linger_ms`
* Graceful shutdown support

### ✅ Structured Event Schema

Each event follows a versioned, production-ready format:

```json
{
  "event_id": "uuid",
  "event_version": "1.0",
  "timestamp": "ISO-8601",
  "source": "mysql-cdc-service",
  "table_name": "products",
  "operation_type": "INSERT | UPDATE | DELETE",
  "primary_keys": {
    "id": 1
  },
  "payload": {
    "old_data": {},
    "new_data": {}
  }
}
```

---

## 📂 Project Structure

```
.
├── src/
│   ├── main.py
│   ├── db_client.py
│   ├── kafka_producer.py
│   ├── cdc_processor.py
│   ├── state_manager.py
│   └── config.py
│
├── tests/
│   ├── test_cdc_processor.py
│   └── test_kafka_producer.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone <repo-url>
cd Event-Driven-Change-Data-Capture-for-a-Relational-Database
```

---

### 2️⃣ Build and Start Services

```bash
docker-compose up --build
```

Services started:

* MySQL
* Zookeeper
* Kafka
* CDC Service
* Consumer

---

### 3️⃣ Trigger Change Events

Connect to MySQL:

```bash
docker exec -it <mysql-container> mysql -uroot -p
USE cdc_db;
```

Insert / Update / Delete:

```sql
INSERT INTO products (name, description, price, stock)
VALUES ('Phone', 'Smartphone', 800, 30);

UPDATE products SET price = 900 WHERE id = 1;

UPDATE products SET is_deleted = TRUE WHERE id = 1;
```

You should see:

```
Event published: <uuid>
```

---

## 🧪 Running Tests

Inside CDC container:

```bash
docker exec -it <cdc-container> bash
pytest
```

Expected output:

```
2 passed in 0.08s
```

---

## 🔐 Design Decisions & Tradeoffs

### Why Polling Instead of Binlog?

* Simpler to implement
* Suitable for controlled environments
* Easier to test in Docker setup
* Tradeoff: Not real-time like Debezium

---

### Why Deterministic Watermark?

* Prevents duplicate processing
* Enables restart safety
* Simplifies failure recovery
* Avoids complex offset management

---

### Why `acks="all"`?

* Guarantees strongest delivery semantics
* Ensures event durability
* Slight latency tradeoff

---

## 🛡 Reliability Considerations

* Kafka retry logic
* Producer flush before shutdown
* State file atomic writes (temp file + replace)
* Graceful signal handling
* Topic auto-creation handling

---

## 📈 Scalability Notes

This implementation can be extended to:

* Multiple partitions
* Multiple consumers
* Schema registry integration
* Exactly-once semantics
* Debezium-based CDC
* Cloud deployment (AWS MSK / Confluent Cloud)

---

## 🏁 Completion Checklist

* [✅] CDC polling implemented
* [✅] Reliable Kafka producer
* [✅] Deterministic watermark state tracking
* [✅] Dockerized microservices
* [✅] Unit tests implemented
* [✅] Graceful shutdown handling
* [✅] Structured event schema
* [✅] End-to-end verified

## 👨‍💻 Author
  KATHULA DHAMINI SRI RAJA JAHNAVI

Built as part of an event-driven systems learning module focusing on:

* Data Engineering fundamentals
* Distributed systems reliability
* Streaming architectures
* Fault tolerance patterns

---

# ⭐ Summary

This project demonstrates:

* Practical event-driven architecture
* Real-world reliability handling
* Stateful processing
* Production-style coding practices
* Containerized distributed systems

