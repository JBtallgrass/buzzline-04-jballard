### **📊 Estimating When You'll Have 500 Messages in Your Kafka Topic**

To estimate when you'll have **500 rafting feedback messages**, we need to consider:
1. **How frequently the producer sends messages** (`BUZZ_INTERVAL_SECONDS` in `.env`).
2. **How many messages does the producer generate per cycle** (each time it reads `all_rafting_remarks.json`)?
3. **The number of records in `all_rafting_remarks.json`** (170 total: **150 positive + 20 negative**).

### **🔢 Key Variables**
- **Producer sends messages every:** `1 second` (from `.env`: `BUZZ_INTERVAL_SECONDS=1`)
- **Total messages in `all_rafting_remarks.json`:** `170`
- **Producer loops continuously, reusing the same data set.**
- **Consumer processes messages immediately, keeping up with Kafka.**

### **🕒 Estimated Time to Reach 500 Messages**
| **Loop #** | **Total Messages Sent** | **Elapsed Time** |
|------------|----------------------|------------------|
| **1** | 170 messages | **170 sec (~3 min)** |
| **2** | 340 messages | **340 sec (~5.5 min)** |
| **3** | 510 messages | **510 sec (~8.5 min)** |

**✅ Conclusion: You will have at least 500 messages after ~8.5 minutes of running the producer.**

### **🚀 Recommendations for Running Analysis at the Right Time**
| **Scenario** | **Best Time to Run Analysis** |
|-------------|----------------------------|
| **Small dataset (~100 messages)** | After **2 minutes** |
| **Medium dataset (~500 messages)** | After **8-10 minutes** |
| **Large dataset (~1000 messages)** | After **17-20 minutes** |

### **📌 Strategy for Running Analysis Automatically**
Instead of manually tracking time, you can:
- **Set up an automatic analysis trigger** (e.g., every 10 minutes).
- **Modify the consumer to run the analysis script when 500+ messages are reached**.

### **⏳ Option 1: Manually Check & Run Analysis**
If you're manually checking, **start the analysis after 8-10 minutes**:
```bash
python analyze_rafting_feedback.py
```

### **🤖 Option 2: Automate Analysis Trigger**
Modify `rafting_consumer.py` to **automatically trigger analysis** once 500 messages are processed:

#### **🔹 Modify `rafting_consumer.py` to Track Message Count**
```python
MESSAGE_COUNT_THRESHOLD = 500  # Run analysis after 500 messages
message_count = 0  # Track processed messages

def process_message(message: str) -> None:
    global message_count
    # Existing processing logic...
    message_count += 1

    if message_count >= MESSAGE_COUNT_THRESHOLD:
        logger.info("🚀 Threshold reached! Running analysis...")
        os.system("python analyze_rafting_feedback.py")
        message_count = 0  # Reset count
```

**✅ Now, the analysis will automatically run every 500 messages!**

### **🚀 Summary**
| **Goal** | **When to Run Analysis?** |
|----------|--------------------------|
| **Fast check (~100 messages)** | **After 2 minutes** |
| **Good dataset (~500 messages)** | **After 8-10 minutes** |
| **Deep analysis (~1000+ messages)** | **After 17-20 minutes** |
| **Fully automated trigger** | **Every 500 messages** |

