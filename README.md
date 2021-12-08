# tw-kafka-mongo-demo

Use Twitter feed to create a datapipeline which is streamed to Kafka , consumed by Mongo and persisted. Build a simple
dashboard with basic metrics from tweets

## Usage
1. Run zookeeper , kafka broker, start mongo
2. Create & activate a virtualenv and install dependencies by typing `pip install -r requirements.txt`
3. Insert **your** twitter API key and token in _kafkaproducer.py_
4. Run `python KafkaProducer.py #<sometrendyhashtag>` in one terminal
5. Run `python Mongoconsumer.py` in the other terminal
6. Run `python DashboardFinal.py` in the other terminal


A link to the Dashboard is generated with basic metrics plotted

## Installation
Download kafka_2.13-3.0.0 and mongo 
unzip them
Browse to 
cd kafka_2.13-3.0.0 
<cmd to run Zk>  bin/zookeeper-server-start.sh config/zookeeper.properties
<cmd to run kafka broker> bin/kafka-server-start.sh ../config/server.properties
Browse to mongo folder (/usr/local/Cellar/mongodb-community@4.0/4.0.27/) - installed via brew
<cmd to run mongo> ./bin/mongo

