import tweepy
from kafka import KafkaProducer
from kafka.errors import KafkaError
from threading import Timer
import json
from wordcloud import WordCloud
import sys
from tweepy import Stream
from tweepy.streaming import StreamListener
import pykafka
from textblob import TextBlob

TWITTER_CONSUMER_KEY = '#Paste your keys ' #5DJ74EvnLhAV6ZFRc6XzSJQBf'
TWITTER_CONSUMER_SECRET = '#Paste your keys ' #SDGqSLecCHOLPTfTZE7ZaggjQe07USEVDgSsIsBA1K4c68CITf'
TWITTER_ACCESS_TOKEN = '#Paste your keys ' #112672182-YwQ4I5HQIoMVQPBaIPTqzwJ6PLH8xKWpf9D68X1d'
TWITTER_TOKEN_SECRET = '#Paste your keys ' #7CdKvGYsYmXRlBnYel4Wm1vqyd2k6ea35uwwlsMn35jub'
words_to_track = ['olympics', 'ipl',
                  'sports']  # ['bitcoin', 'ethereum', 'litecoin', 'denarius', '-filter:links', '-filter:retweets']
topic = 'tweets-1'


class TWStreamListener(StreamListener):
    def __init__(self):

        self.client = pykafka.KafkaClient("localhost:9092")
        self.kafka_producer = self.client.topics[bytes('tweets-1', 'ascii')].get_producer()

    def on_status(self, status):
        self.kafka_producer.send('tweets-1', status.text.encode()).add_callback(self.on_send_success).add_errback(
            self.on_send_error)

    def on_data(self, data):
        try:
            json_data = json.loads(data)
            send_data = '{}'
            json_send_data = json.loads(send_data)
            # make checks for retweet and extended tweet-->done for truncated text
            if "retweeted_status" in json_data:
                try:
                    json_send_data['text'] = json_data['retweeted_status']['extended_tweet']['full_text']
                except:
                    json_send_data['text'] = json_data['retweeted_status']['text']
            else:
                try:
                    json_send_data['text'] = json_data['extended_tweet']['full_text']
                except:
                    json_send_data['text'] = json_data['text']

            json_send_data['creation_datetime'] = json_data['created_at']
            json_send_data['username'] = json_data['user']['name']
            json_send_data['location'] = json_data['user']['location']
            json_send_data['userDescr'] = json_data['user']['description']
            json_send_data['followers'] = json_data['user']['followers_count']
            json_send_data['retweets'] = json_data['retweet_count']
            json_send_data['favorites'] = json_data['favorite_count']

            blob = TextBlob(json_send_data['text'])
            (json_send_data['senti_val'], json_send_data['subjectivity']) = blob.sentiment
            #
            # # check for this really small value and make it 0
            if json_send_data['senti_val'] == 1.6653345369377347e-17:
                json_send_data['senti_val'] = 0
            # # keep only the first two decimal points of sentiment value and subjectivity
            json_send_data['senti_val'] = str(json_send_data['senti_val'])[:4]
            json_send_data['subjectivity'] = str(json_send_data['subjectivity'])[:4]
            # # printing for testing
            print("Send---", json_send_data)

            self.kafka_producer.produce(bytes(json.dumps(json_send_data), 'ascii'))

            return True

        except KeyError:
            return True

    def on_exception(self, status_code):
        if status_code == 420:
            print('Disconnecting Twitter stream due to an Error!')
            return False

    def on_send_success(self, record_metadata):
        topic, partition, offset = record_metadata
        print('TOPIC: {}\t\tPARTITION: {}\t\tOFFSET: {}'.format(topic, partition, offset))

    def on_send_error(self, excp):
        print('Exception Occured', excp)

    def flush_buffer(self):
        flush_timer = Timer(60, self.flush_buffer)  # Flush buffer async every 60 seconds
        flush_timer.daemon = True
        flush_timer.start()
        print('============================================================')
        print('!!!!!!!!!!!!!!!!!!!!!!!FLUSHING BUFFER!!!!!!!!!!!!!!!!!!!!!!!')
        print('============================================================')
        self.kafka_producer.flush()


class Producer:
    def __init__(self):
        print('Starting Producer...\n\n')
        auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_TOKEN_SECRET)

        print('Twitter API initialized')
        twStreamListener = TWStreamListener()
        self.twStream = Stream(auth, twStreamListener)
        print('Realtime Feed started!')
        # search topic has to be specified thorough cli args
        self.twStream.filter(track=words_to_track)


if __name__ == '__main__':
    Producer()
