### **🚀 Step-by-Step Execution Plan for Running & Analyzing Rafting Feedback**

**analysis step should be conducted after a sufficient number of messages have been processed by the consumer**.

### **📌 Full Execution Plan**
| **Step** | **Task** | **Command** |
|----------|---------|-------------|
| **Step 1** | **Start Terminal 1 (Zookeeper Service)** | ✅ Activate virtual environment → Start WSL → Run Zookeeper |
| **Step 2** | **Start Terminal 2 (Kafka Server)** | ✅ Activate virtual environment → Start WSL → Start Kafka |
| **Step 3** | **Start Terminal 3 (Rafting Producer)** | ✅ Activate virtual environment → Run rafting producer |
| **Step 4** | **Start Terminal 4 (Rafting Consumer)** | ✅ Activate virtual environment → Run rafting consumer |
| **Step 5** | **Monitor Data Flow** | ⏳ Let the producer/consumer run for some time |
| **Step 6** | **Run Analysis (After Enough Data is Collected)** | ✅ Activate virtual environment → Run analysis script |
| **Step 7** | **Review Plots** | 🖼️ Open `plots/` folder to view results |

## **🚀 Step-by-Step Commands**

### **🔹 Step 1: Start Terminal 1 (Zookeeper Service)**
1. **Activate your virtual environment** (inside your project root directory):
   ```bash
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```
2. **Start WSL (if using Windows)**
   ```bash
   wsl
   ```
3. **Run Zookeeper**
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
   _(Leave this terminal running)_

### **🔹 Step 2: Start Terminal 2 (Kafka Server)**
1. **Open a new terminal**
2. **Activate your virtual environment**
   ```bash
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```
3. **Start WSL (if using Windows)**
   ```bash
   wsl
   ```
4. **Start Kafka**
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```
   _(Leave this terminal running)_

### **🔹 Step 3: Start Terminal 3 (Rafting Producer)**
1. **Open a new terminal**
2. **Activate your virtual environment**
   ```bash
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```
3. **Run the rafting producer**
   ```bash
   python rafting_producer.py
   ```
   _(Leave this running to generate feedback data)_

### **🔹 Step 4: Start Terminal 4 (Rafting Consumer)**
1. **Open a new terminal**
2. **Activate your virtual environment**
   ```bash
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```
3. **Run the rafting consumer**
   ```bash
   python rafting_consumer.py
   ```
   _(This will continuously process incoming rafting feedback)_

### **🔹 Step 5: Monitor Data Flow**
- **Let the producer generate rafting feedback** and **allow the consumer to process data**.
- You can check if data is being saved in:
  - `negative_feedback.json` (Negative feedback collected)
  - `all_rafting_remarks.json` (All feedback)

### **🔹 Step 6: Run the Analysis (AFTER Enough Data is Collected)**
1. **Open a new terminal**
2. **Activate your virtual environment**
   ```bash
   source .venv/bin/activate   # Mac/Linux
   .venv\Scripts\activate      # Windows
   ```
3. **Run the analysis script**
   ```bash
   python analyze_rafting_feedback.py
   ```
   _(This will generate visualizations and save them as PNG files)_

### **🔹 Step 7: Review Results**
1. **Open the `plots/` folder** in your project directory.
2. **View PNG files**:
   - 📊 `guide_feedback_comparison.png`
   - 📈 `weekly_feedback_trend.png`
   - 🌦️ `weather_feedback_comparison.png`
   - 🌊 `river_flow_feedback_comparison.png`

### **❓ When Should I Conduct Analysis?**
- **You should conduct the analysis AFTER enough messages have been processed by the consumer**.
- **How much data is enough?** At least **100+ messages** for meaningful trends.

### **🚀 Summary of Steps**
| **Step** | **Task** | **When to Run?** |
|----------|---------|----------------|
| ✅ **Start Zookeeper** | `bin/zookeeper-server-start.sh` | **First** |
| ✅ **Start Kafka** | `bin/kafka-server-start.sh` | **After Zookeeper is running** |
| ✅ **Start Producer** | `python rafting_producer.py` | **Once Kafka is ready** |
| ✅ **Start Consumer** | `python rafting_consumer.py` | **Right after producer starts** |
| ⏳ **Wait for Data Collection** | Let data flow for ~5-10 minutes | **Monitor output** |
| ✅ **Run Analysis** | `python analyze_rafting_feedback.py` | **After enough data is collected** |
| 🖼️ **Review Plots** | Open `plots/` folder | **After analysis completes** |

