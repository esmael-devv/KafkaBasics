# KafkaBasics

Small Java/Gradle playground for learning Apache Kafka producers and consumers.

## Tech Stack

- Java (JDK 8+)
- Gradle
- Apache Kafka (local broker on `localhost:9092`)

## What’s Inside

All examples live under:

- `src/main/java/io/kafka/demo`

You’ll find simple demos showing:

- Basic Kafka producer
- Producer with callbacks
- Producer with keys
- Basic consumer examples (reading messages from a topic)

This is purely a learning repo – configs are intentionally simple and **not** production-grade.

## Getting Started

1. Start a local Kafka broker (e.g., via Docker or Kafka binaries) listening on `localhost:9092`.
2. Clone the repo:
   ```bash
   git clone https://github.com/esmael-devv/KafkaBasics.git
   cd KafkaBasics
