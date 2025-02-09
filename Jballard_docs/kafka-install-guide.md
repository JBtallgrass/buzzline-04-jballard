# Kafka Installation Guide - WSL Ubuntu

## System Requirements
- Windows 10 version 2004 or higher
- Administrator access
- 4GB RAM minimum
- 4GB available storage

---

## Install WSL & Ubuntu
### Step 1: Enable WSL and Install Ubuntu
In **PowerShell (Administrator)**:
```powershell
wsl --install -d Ubuntu
```

### Step 2: Verify WSL Installation ✅
```powershell
wsl -l -v  # Should show VERSION 2
wsl        # Enter Ubuntu environment
```

---

## Install Java Development Kit (JDK)
Apache Kafka requires **Java 8 or newer**. We will install **OpenJDK 17** explicitly.

```bash
# Update package lists
sudo apt update

# Install OpenJDK 17
sudo apt install openjdk-17-jdk -y

# Verify installation ✅
java --version

# If multiple Java versions are installed, switch to OpenJDK 17:
sudo update-alternatives --config java
```

---

## Install Apache Kafka
### Step 1: Download Kafka
```bash
cd ~

# Get latest version from Apache Kafka website: https://kafka.apache.org/downloads
wget https://downloads.apache.org/kafka/<VERSION>/kafka_2.13-<VERSION>.tgz

# Example for Kafka 3.9.0:
# wget https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
```

### Step 2: Extract and Rename ✅
```bash
tar -xvzf kafka_*.tgz
mv kafka_* kafka
```

---

## Start Kafka Services
### Step 1: Start Zookeeper (Terminal 1) ⚠️
Kafka requires Zookeeper to run first.
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2: Start Kafka Broker (Terminal 2) ⚠️
```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### Step 3: Create and Verify a Kafka Topic (Terminal 3) ✅
```bash
cd ~/kafka

# Create a test topic
bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Verify topic creation
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## System Checks ✅
- **Check WSL version:** `wsl -l -v`  
- **Verify Java installation:** `java --version`  
- **Confirm Kafka is running:** `netstat -tulnp | grep 9092`  
- **Check Zookeeper status:** `ps aux | grep zookeeper`  
- **Ensure Kafka topic exists:** `bin/kafka-topics.sh --list --bootstrap-server localhost:9092`  

---

## Troubleshooting
### 1. WSL Installation Issues
**Issue:** WSL is not installing properly or showing incorrect version.
**Fix:**
- Enable virtualization in BIOS
- Run Windows Update
- Verify system requirements with `wsl -l -v`

### 2. Java Issues
**Issue:** Kafka fails to start due to missing Java or incorrect version.
**Fix:**
- Ensure Java is installed: `java --version`
- If multiple versions exist, switch:
  ```bash
  sudo update-alternatives --config java
  ```
- Reinstall Java if necessary: `sudo apt install --reinstall openjdk-17-jdk`

### 3. Kafka Services Not Starting ⚠️
**Issue:** Kafka does not start or fails with connection errors.
**Fix:**
- Ensure **Zookeeper** starts before Kafka.
- Check if Kafka is running:
  ```bash
  netstat -tulnp | grep 9092
  ```
- Verify Java is installed: `which java`
- Look at error logs in `logs/` directory inside Kafka.
- Ensure correct **server.properties** settings, including `listeners=PLAINTEXT://:9092`

### 4. Kafka Topics Not Created ⚠️
**Issue:** Kafka topics do not appear when listing topics.
**Fix:**
- Ensure Kafka and Zookeeper are running.
- Check broker connection:
  ```bash
  bin/kafka-topics.sh --list --bootstrap-server localhost:9092
  ```
- Restart Kafka and retry.

---

## Additional Resources
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [WSL Documentation](https://learn.microsoft.com/en-us/windows/wsl/install)
- [OpenJDK Documentation](https://openjdk.org/)

---
This guide ensures **clear installation steps**, **explicit Java setup**, and **troubleshooting tips** for smoother Kafka deployment in WSL. 🚀

