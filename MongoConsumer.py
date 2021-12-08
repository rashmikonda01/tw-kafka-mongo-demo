from kafka import KafkaConsumer
import pymongo
import json


# connect to the mongoclient
client = pymongo.MongoClient('mongodb://localhost:27017')

# get the database
db = client['test']
db = client.test

consumer = KafkaConsumer('tweets-1', bootstrap_servers=['localhost:9092'])
for msg in consumer:
    record = json.loads(msg.value)
    text = record['text']
    senti_val = record['senti_val'][:4]
    subjectivity = record['subjectivity'][:4]
    creation_datetime = record['creation_datetime']
    username = record['username']
    location = record['location']
    user_description = record['userDescr']
    followers = record['followers']
    retweets = record['retweets']
    favorites = record['favorites']

    # create dictionary and ingest data into mongo
    try:
       twitter_rec = {'text':text,'senti_val':senti_val,'subjectivity':subjectivity,'creation_datetime':creation_datetime,'username' :username,'location':location,
                      'user_description':user_description,'followers':followers,'retweets':retweets,'favorites':favorites}
       rec_id = db.tweet_info.insert_one(twitter_rec)
       print("Data inserted with record ids",rec_id)
    except:
       print("Could not insertInMongo")