import pandas as pd
from kafka import KafkaProducer
import schedule
import time
from json import dumps

# Global variables
sample_interval = 10  # in seconds

def send_sample_data(producer, df_full):
    dict_full = df_full.sample(476).to_dict(orient="records")[0]
    producer.send('streamTopic', value=dict_full)

# Create object producer for send stream data to Kafka
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

df_full = pd.read_csv("/home/thanhphat/Downloads/StreamData/part-00000-6f30406d-8e25-496b-afa0-9a6e1d9778e5-c000.csv")

# Schedule the task to run every 10 seconds
schedule.every(sample_interval).seconds.do(send_sample_data, producer=producer, df_full=df_full)

while True:
    print("Sending data")
    schedule.run_pending()
    time.sleep(1)  # sleep for 1 second to avoid high CPU usage

