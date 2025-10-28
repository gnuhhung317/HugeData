from kafka import KafkaProducer
import json
import time

# Kết nối Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize JSON
)

topic = 'test-topic'  # topic đã tạo trên Kafka

# Gửi dữ liệu
message = {"message": "hello"}
producer.send(topic, message)
producer.flush()  # đảm bảo gửi hết

print(f"Đã gửi message: {message}")
producer.close()
