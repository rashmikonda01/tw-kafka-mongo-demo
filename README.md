# tw-kafka-mongo-demo

Use Twitter feed to create a datapipeline which is streamed to Kafka , consumed by Mongo and persisted. Build a simple
dashboard with basic metrics from tweets

## Usage
1. Run zookeeper and kafka broker
2. Create & activate a virtualenv and install dependencies by typing `pip install -r requirements.txt`
3. Insert **your** twitter API key and token in _kafkaproducer.py_
4. Run `python KafkaProducer.py #<sometrendyhashtag>` in one terminal
5. Run `python Mongoconsumer.py` in the other terminal
6. Run `python DashboardFinal.py` in the other terminal


A link to the Dashboard is generated with basic metrics plotted
