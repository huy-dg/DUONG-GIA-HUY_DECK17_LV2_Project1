# Kafka → MongoDB Streaming Pipeline

This project implements a **data streaming pipeline** that consumes real-time data from a Kafka topic and writes processed results into MongoDB.

It is built using:
- 🐍 **Python 3.13+**
- 🧰 **Poetry** for dependency management
- ☁️ **Confluent Kafka** client
- 🍃 **MongoDB** for storage
- ⚙️ **Conda** for environment isolation



---

# PREREQUISITES

Miniconda or Anaconda MUST be install

Kafka containers MUST be started

MongoDB Community Edition MUST be install


---



# 📁 Project structure
├── kafka_project/ 

    ├── scripts/ 
    │ ├── setup.py 
    │ ├── streaming_data_from_source.py 
    │ └── consumer_to_mongodb.py 
    ├── src/ 
    │ └── kafka_project/ 
    │ ├── __init__.py 
    │ ├── core/ 
    │ │ ├── config.py 
    │ │ ├── logger.py 
    │ │ └── json_processing.py 
    │ └── kafka/ 
    │   ├── consumer.py 
    │   ├── producer.py 
    │   └── topic.py 
    ├── tests/ 
    │ └── __init__.py 
    ├── .env 
    ├── environment.yml 
    ├── .gitignore 
    ├── poetry.lock 
    ├── pyproject.toml 
    └── README.md

## ⚙️ Environment setup

### 1️⃣ Clone the repository
```bash
git clone https://github.com/huy-dg/DUONG-GIA-HUY_DECK17_LV2_Project1.git
mv DUONG-GIA-HUY_DECK17_LV2_Project1 "any_name"
cd "any_name"
```

### 2️⃣ Create and activate Conda environment
```bash
conda env create -f environment.yml
conda activate test
```

### 3️⃣ Install dependencies with Poetry
```bash
poetry install
```

### 🔑 Environment variables
Use the .env provided personally
or email me at duongghuy96@gmail.com

---



# 🚀 Running the pipeline
```bash
# Start the data source → Kafka
python scripts/streaming_data_from_source.py

# Start the consumer → MongoDB writer (in another terminal)
python scripts/consumer_to_mongodb.py
```

---

# 🧱 Development notes

All configuration is handled in src/kafka_project/core/config.py

JSON parsing and validation are handled in json_processing.py

Kafka topic, consumer, and producer utilities are under src/kafka_project/kafka/

Minor bugs: 

- consumer - mongoDB pipeline stopped after a while when waiting for message in topic.

- offset_metadata some how only got data of partition 0 and 2.

Improvement in next update:

- Will fix minor bugs.

- Will combine 2 script into 1 main.py script.

Future improvements:

- Rebuild source-to-producer pipeline with queuing model to improve throughput.

---

# 🧑‍💻 Author

huy-dg
📧 duongghuy96@gmail.com
🌐 https://github.com/huy-dg
