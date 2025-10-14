from kafka import KafkaConsumer
import json

# Create a consumer to read messages from the 'traffic_data' topic
consumer = KafkaConsumer(
    'traffic_data',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest', # Start reading from the newest message in the topic
    group_id='my-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Decode the JSON message
)

print("Consumer is listening for messages on the 'traffic_data' topic...")

# Continuously listen for and print messages
try:
    for message in consumer:
        print(f"Received data: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
