WikiGuard — Real-Time Edit Fraud Detection
A real-time vandalism and fraud detection system for Wikipedia edits, built with Apache Kafka, Spark Structured Streaming, and an ML scoring pipeline.
Every edit happening on Wikipedia right now gets ingested, scored, and flagged — live.

What it does
Wikipedia receives thousands of edits every hour. Most are legitimate. Some are vandalism — page blanking, suspicious deletions, bot-like behavior. WikiGuard detects these in real time by streaming every edit through a fraud scoring model and surfacing flagged edits on a live dashboard.

Architecture
Wikimedia SSE API → Kafka (wiki-edits) → Spark Structured Streaming → ML Scoring → Dashboard
ComponentRoleWikimedia Live APIReal SSE stream of every Wikipedia edit globallyApache KafkaDistributed ingestion, decouples producer from consumerSpark Structured StreamingConsumes Kafka topic, processes in 5s micro-batchesML Fraud ScorerScores each edit 0–100 based on behavioral featuresFlask + JSON sinkServes flagged events to the dashboardDashboardLive HTML dashboard, refreshes every 4 seconds

ML Scoring Model
Each edit is scored from 0 to 100 using a weighted feature model. The same signals are used by Wikipedia's production anti-vandalism bot, ClueBot NG.
FeatureSignalScoreDeletion ratio > 90%Page blanking+40Deletion ratio > 50%Large removal+20Edit size > 5000 bytes addedSpam injection+15Empty commentNo edit summary+10Suspicious keywords in comment"vandal", "lol", "test" etc.+20Minor flag with large deltaContradictory metadata+15Non-article namespaceTalk/user page abuse+5Known botReduce false positives-30
Risk labels:

HIGH_RISK — score ≥ 60
SUSPICIOUS — score 30–59
CLEAN — score < 30


Tech Stack

Python 3.12
Apache Kafka 3.7
Apache Spark 4.1.1 + PySpark
Flask (dashboard API)
kafka-python
HTML / CSS / JS (dashboard frontend)


Getting Started
Prerequisites

Java 11+
Apache Kafka installed at C:\kafka_2.12-3.7.2
Apache Spark installed at C:\spark-4.1.1
Python 3.12

Install dependencies
bashpip install kafka-python requests flask flask-cors pyspark
Start everything (in order)
1. Zookeeper
bash.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
2. Kafka
bash.\bin\windows\kafka-server-start.bat .\config\server.properties
3. Create topic (first time only)
bash.\bin\windows\kafka-topics.bat --create --topic wiki-edits --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
4. Producer
bashpython producer.py
5. Spark Streaming
bashspark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3 spark_streaming.py
6. Dashboard server
bashpython dashboard_server.py
7. Open dashboard
Go to http://localhost:5000 in your browser.

Project Structure
wikiguard/
├── producer.py           # Streams live Wikipedia edits into Kafka
├── spark_streaming.py    # Consumes Kafka, scores edits, writes flagged output
├── dashboard_server.py   # Flask API serving live fraud data
├── dashboard.html        # Live frontend dashboard
└── requirements.txt      # Python dependencies

Notes

On Windows, always stop Kafka cleanly with Ctrl+C. Force-closing terminals leaves locked log files. If Kafka fails to start, delete C:\tmp\kafka-logs and C:\tmp\zookeeper and restart.
The Spark job writes flagged events to C:\tmp\wikiguard_output. The dashboard reads from this directory every 4 seconds.
Built and tested on Windows 11, Spark 4.1.1, Kafka 3.7.2.


Acknowledgements
Built as part of the Big Data Analytics course. Fraud detection heuristics inspired by ClueBot NG, Wikipedia's production anti-vandalism bot.
