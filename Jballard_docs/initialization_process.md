### **📌 Updated Initialization Process Using `py -m`**
Since you'll be using **`py -m`** instead of `python`, here’s how to initialize your **Kafka → JSON → CSV** pipeline while maintaining best practices.

---

## **🔹 Step 1: Start Kafka & Zookeeper**
Before running any producers or consumers, ensure **Apache Kafka and Zookeeper** are running.

### **Start Zookeeper**
```bash
zookeeper-server-start.sh config/zookeeper.properties
```

### **Start Kafka Broker**
```bash
kafka-server-start.sh config/server.properties
```

🔹 **Verify Kafka is running:**
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```
You should see topics listed if Kafka is active.

---

## **🔹 Step 2: Generate Synthetic Data**
Before streaming data, generate **customer feedback, weather conditions, and river flow data**.

```bash
py -m utils_generate_rafting_data
py -m utils_generate_weather_data
py -m utils_generate_river_flow
```
🔹 This ensures your **JSON producer** has data to stream.

---

## **🔹 Step 3: Start the JSON Pipeline**
### **1️⃣ Run JSON Producer (`rafting_producer.py`)**
```bash
py -m rafting_producer
```
✅ **Streams customer feedback to `rafting_feedback` Kafka topic.**

---

### **2️⃣ Run JSON Consumer (`rafting_consumer.py`)**
```bash
py -m rafting_consumer
```
✅ **Reads `rafting_feedback` topic, enriches data with weather & river flow conditions, and publishes to `rafting_csv_feedback` Kafka topic.**

📡 **At this stage, JSON is transformed into a structured CSV-friendly format.**

---

## **🔹 Step 4: Start the CSV Pipeline**
### **3️⃣ Run CSV Producer (`csv_rafting_producer.py`)**
```bash
py -m csv_rafting_producer
```
✅ **Reads enriched messages from `rafting_csv_feedback` Kafka topic.**
✅ **Flags negative feedback & identifies trip disruptions.**
✅ **Publishes processed messages to `processed_csv_feedback` Kafka topic.**

📡 **This ensures trip data is refined before long-term analysis.**

---

### **4️⃣ Run CSV Consumer (`csv_rafting_consumer.py`)**
```bash
py -m csv_rafting_consumer
```
✅ **Reads structured trip data from `processed_csv_feedback` Kafka topic.**  
✅ **Tracks long-term guide performance & analyzes trends.**  
✅ **Saves final insights for reporting & visualization.**  

📊 **This consumer generates final insights into guide performance & customer satisfaction.**

---

## **🔹 Step 5: Verify Data Flow**
### **Check Messages in Kafka Topics**
🔹 **List Kafka topics**
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

🔹 **Check messages in `rafting_feedback` topic (JSON)**
```bash
kafka-console-consumer.sh --topic rafting_feedback --from-beginning --bootstrap-server localhost:9092
```

🔹 **Check messages in `processed_csv_feedback` topic (CSV)**
```bash
kafka-console-consumer.sh --topic processed_csv_feedback --from-beginning --bootstrap-server localhost:9092
```

✅ If data is flowing through topics, your **Kafka pipeline is fully operational!**

---

## **🔹 Step 6: (Optional) Convert Log to CSV for Backup**
If needed, generate a **CSV file from log data** for offline analysis.

```bash
py -m utils_convert_log_to_csv
```
📂 **Saves structured messages into `rafting_feedback.csv` for review.**

---

### **🚀 Your Kafka-Powered Rafting Feedback System is Now Live!**
📡 **Kafka Streams Feedback → JSON Enriched with Weather & River Flow → CSV Analyzed for Performance Trends** 📊

