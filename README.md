![Banner](images/banner.png)


## 🧑‍💼 Jason A. Ballard  
**Instructional Systems Special | Data Scientist | Data and AI Officer | Data Literacy Advocate | Educator in Professional Military Education**

Welcome! I'm Jason A. Ballard, an experienced data and AI integration leader currently serving as a Data and AI Officer for the **US Army Combined Arms Center** at Fort Leavenworth, Kansas. My work bridges data science, AI strategy, and higher education, focusing on transforming decision-making through data literacy and innovation.

I invite you to explore my GitHub repository [jbtallgrass](https://github.com/JBtallgrass?tab=repositories), where I share insights, tools, and resources geared toward data literacy and advanced analytics in educational contexts. My projects emphasize practical solutions, open collaboration, and a commitment to enhancing data accessibility across teams.

### 🔑 Key Areas of Focus:
- **Data Strategy & Governance**: Developing frameworks that promote data-driven decision-making and cross-departmental data sharing.  
- **AI & Analytics**: Leveraging data analytics and GenAI to unlock insights and drive transformational initiatives within Army University.  
- **Data Literacy & Education**: Equipping leaders and students with data literacy skills critical for today's complex, data-rich environments.  

Please don't hesitate to connect, collaborate, or contact me if our interests align. **Let's make data-driven transformation a reality together.**  

📍 **LinkedIn**: [Jason A. Ballard](https://www.linkedin.com/in/jasonaballard)

**GitHub** : [jbtallgrass](https://github.com/JBtallgrass)
---
# 🌊 Rafting Feedback Streaming Project support Module 4
---
## 📚 Table of Contents
- [Project Overview](#Project-Overview)
- [Technologies Used](#Technologies-used)
- [Setup](#setup)
- [Project Components](#project-components)

---

## 📌 Project Overview

### 🎯 Goals
- **Real-time processing** of structured (CSV) and semi-structured (JSON) data.
- **Automated enrichment** of feedback with weather and river conditions.
- **Performance tracking** for rafting guides based on customer reviews.
- **Predictive insights** into trip satisfaction and environmental impact.

### 🚣 Data Sources
- **Customer Feedback**: Reviews from rafting trip participants.
- **Weather Conditions**: Temperature, wind speed, and precipitation.
- **River Flow Levels**: Water level, current speed, and temperature.

### ⚡ Technologies Used
- **Apache Kafka**: Real-time message streaming and processing.
- **Python**: Data generation, transformation, and analytics.
- **dotenv**: Environment variable management.
- **Loguru**: Logging feedback and performance.
- **matplotlib**: Data visualization for performance trends.
- **Pandas**: Data manipulation and analysis.
- **VS Code**: Development environment.

---
Here's an accurate and detailed `README.md` file for the **Rafting Feedback Streaming Project**:

---

# 🌊 Rafting Feedback Streaming Project

This project is designed to **stream, process, and analyze real-time customer feedback** from rafting trips on the **French Broad River, NC**, using **Apache Kafka**. It integrates customer reviews with **weather and river flow conditions**, providing valuable insights into trip experiences and environmental impacts.

Here’s how you can add the disclaimer to your `README.md`:

---

### ⚠️ Note: ⚠️
The data in this project is **fictitious** with the use of  **Generative AI (GenAI)** assistants to **generate, problem-solve, and debug** the process.

---

## 📌 Project Overview

### 🎯 Goals
- **Real-time processing** of structured (CSV) and semi-structured (JSON) data.
- **Automated enrichment** of feedback with weather and river conditions.
- **Performance tracking** for rafting guides based on customer reviews.
- **Predictive insights** into trip satisfaction and environmental impact.

### 🚣 Data Sources
- **Customer Feedback**: Reviews from rafting trip participants.
- **Weather Conditions**: Temperature, wind speed, and precipitation.
- **River Flow Levels**: Water level, current speed, and temperature.

### ⚡ Technologies Used
- **Apache Kafka**: Real-time message streaming and processing.
- **Python**: Data generation, transformation, and analytics.
- **dotenv**: Environment variable management.
- **Loguru**: Logging feedback and performance.
- **matplotlib**: Data visualization for performance trends.
- **Pandas**: Data manipulation and analysis.
- **VS Code**: Development environment.

---

## 🛠️ Setup & Requirements

### ✅ Prerequisites
- **Python 3.11+**
- **Kafka & Zookeeper** installed and running.
    - bin/zookeeper-server-start.sh config/zookeeper.properties
    - bin/kafka-server-start.sh config/server.properties
  - **Virtual Environment** set up for dependency management.

### 📥 Installation and Setup

1. Clone the project:
   
2. Create and activate a virtual environment:
   
3. Install dependencies:
  
4. Set up Kafka and Zookeeper:
   Follow the instructions in [Kafka Install Guide](Jballard_docs/kafka-install-guide.md).

5. Configure environment variables in `.env`:
   ```
   KAFKA_BROKER_ADDRESS=localhost:9092
   RAFTING_TOPIC=rafting_feedback
   RAFTING_INTERVAL_SECONDS=2
   ```

---

## 🔹 Project Components

### 1. Data Generation
- **Weather Data** (`utils_generate_weather_data.py`): Generates synthetic weather data for the rafting region.
- **River Flow Data** (`utils_generate_river_flow.py`): Creates realistic river flow conditions.
- **Rafting Feedback** (`utils_generate_rafting_data.py`): Produces customer reviews with a mix of positive and negative feedback.

### 2. Kafka Producers and Consumers
- **Rafting Producer (`rafting_producer.py`)**: Streams generated feedback data to the `rafting_feedback` topic.
- **JSON Consumer (`rafting_consumer.py`)**: Logs all feedback, flags negative comments, and enriches messages with weather and river data.
- **CSV Consumer (`csv_rafting_consumer.py`)**: Processes JSON feedback and republishes it as CSV-friendly structured data.
- **CSV Feedback Consumer (`csv_feedback_consumer.py`)**: Writes structured feedback to a CSV file for analysis.
- **Processed CSV Producer (`csv_rafting_producer.py`)**: Enhances and republishes CSV feedback with status flags and trip disruption alerts.

### 3. Visualization
- **Real-Time Feedback Charts (`jb_project_consumer.py`)**:
  - Positive vs. Negative feedback.
  - Weekly performance trends.
  - Weather impact on feedback.
  - River flow and feedback correlation.

---

## 🔄 Workflow

1. **Data Generation**: Run `rafting_producer.py` to generate and stream rafting feedback.
2. **Real-Time Feedback Processing**: Consumers enrich, log, and publish processed feedback.
3. **Visualization**: `jb_project_consumer.py` updates charts every 10 messages.

---

## 📊 Visualizations

- **Positive vs. Negative Feedback (Bar Chart)** 
- **Weekly Feedback Trends (Line Chart)**
- **Weather vs. Negative Feedback (Bar Chart)**
- **River Flow vs. Feedback Type (Box Plot)**

---

## 📂 Project Structure

```
├── data/                  # Generated data files
├── images/                # Visualization charts
├── utils/                 # Utility scripts for data generation and logging
├── producers/             # Kafka producers
├── consumers/             # Kafka consumers
├── .env                   # Environment variables
├── requirements.txt       # Project dependencies
└── README.md              # Project documentation
```

## ⚠️ Important Notes

1. Ensure Kafka and Zookeeper are running before starting producers or consumers.
2. Always verify environment variables in the `.env` file.
3. Regularly check logs in `logs/rafting_project_log.log`.

---

## 📝 License

This project is licensed under the **MIT License**. You are encouraged to fork, modify, and explore the code.

[![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/) 

[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Jason%20A.%20Ballard-blue?style=flat&logo=linkedin)](https://www.linkedin.com/in/jasonaballard/)  

---
_Project completed Februray 9th 2025_
---

