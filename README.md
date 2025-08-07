# EmoStream: Real-Time Emoji Broadcast via Scalable Event-Driven Architecture

## 📌 Project Overview

In live sporting events, fans express their emotions in real-time—cheering for boundaries, celebrating wickets, or reacting to every twist in the game. Emojis serve as a vibrant, collective signal of this fan sentiment.

**EmoStream** is built to capture and visualize these reactions at massive scale—processing billions of emoji events to deliver a live "emoji swarm" that reflects the crowd's mood in real time. This project tackles the challenge of high-throughput, low-latency streaming and display of these emotions during events like cricket tournaments with millions of concurrent viewers.

---

## 🎯 Objective

The aim is to build a **horizontally scalable system** capable of handling billions of concurrent emoji interactions during live broadcasts on platforms like Hotstar. The system:
- Captures user-generated emojis in real time
- Processes them with minimal delay
- Delivers live feedback to clients
- Is designed using **Kafka** for event streaming and **Apache Spark** for real-time processing

The architecture ensures both **high concurrency** and **low latency**, essential for a seamless and engaging user experience.

---

## 🏗️ System Architecture

![Application_Architecture](./Architecture.png)

The system architecture is divided into three key stages:

---

## 1️⃣ Client Event Ingestion & Queueing

### ➤ Step 1: API Endpoint for Incoming Emoji Events
- Built using lightweight web frameworks like **Flask** (Python) or **Express.js** (Node.js)
- Clients send emoji data via `POST` requests containing:
  - `User ID`
  - `Emoji Type`
  - `Timestamp`
- Each request is handled asynchronously to ensure non-blocking performance
- The emoji payload is forwarded to a **Kafka Producer**

### ➤ Step 2: Message Queueing via Kafka
- Kafka serves as the message queue for buffering high-volume emoji traffic
- A **flush interval** of 500ms is used to push buffered messages from the producer to the Kafka broker
- This ensures timely and efficient batch delivery without overwhelming the broker

---

## 2️⃣ Stream Processing & Aggregation

![Micro-Batch-Processing](./Mirco-Batch.png)

- A **Kafka Consumer** ingests emoji events from the broker
- The system performs **real-time aggregation** of emoji counts over short, fixed intervals (e.g., every 2 seconds)
- **Apache Spark Streaming** is used in **micro-batch mode** to efficiently process these time-windowed emoji events
- This maintains near real-time responsiveness without the overhead of per-message processing

---

## 3️⃣ Real-Time Delivery to Clients at Scale

![Pub-Sub-Model](./Pub-Sub.png)

The final stage involves **distributing the processed emoji aggregates back to end-users**, using a publish-subscribe (pub-sub) model:

### ➤ Main Publisher
- Receives processed aggregates from Spark
- Forwards updates to the appropriate cluster based on region, audience, or other logical partitions

### ➤ Message Broker
- A **decoupled messaging layer** (Kafka, RabbitMQ, etc.) connects the main publisher to multiple downstream clusters
- Ensures reliable, asynchronous communication

### ➤ Cluster Publishers & Subscribers
- Each cluster includes:
  - A **Cluster Publisher** that receives messages from the broker
  - Multiple **Subscribers** that deliver updates to the actual clients
- Supports **horizontal scaling**—clusters can be spun up or down based on user load

---

## ✅ Summary

EmoStream offers a resilient, scalable pipeline to manage real-time fan sentiment using emojis:
- **Low latency** interactions
- **Scalable architecture** for high traffic
- **Stream-first design** using Kafka and Spark
- **Client-driven feedback loop** that enhances viewer engagement

This architecture is ideal for platforms looking to blend real-time sentiment with live sports or entertainment streaming.

