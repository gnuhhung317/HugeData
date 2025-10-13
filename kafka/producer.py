import requests
import cv2
import numpy as np
from ultralytics import YOLO
import time
import logging
import datetime
from kafka import KafkaProducer
import json
from collections import Counter

# Configure logging
logging.basicConfig(filename='producer.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Load a pretrained YOLOv8n model
model = YOLO('yolov8n.pt')

# Kafka configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# URL of the webcam image
base_url = 'https://webcams.transport.nsw.gov.au/livetraffic-webcams/cameras/wentworth_avenue_sydney.jpeg'

while True:
    try:
        # Add a timestamp to the URL
        now = datetime.datetime.now()
        url = f'{base_url}?{now.strftime("%I:%M:%S%p")}&refresh={now.strftime("%I:%M:%S%p")}'

        # Fetch the image from the URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        # Check if the content is an image
        if 'image/jpeg' not in response.headers.get('Content-Type', '-'):
            logging.warning('Content is not a JPEG image.')
            print('Content is not a JPEG image.')
            time.sleep(10)
            continue

        # Read the image data into a numpy array
        image_array = np.asarray(bytearray(response.raw.read()), dtype=np.uint8)

        # Decode the image
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        # Run inference on the image
        results = model.predict(source=img, conf=0.25)

        # Log detected vehicles
        vehicles = []
        for r in results:
            for c in r.boxes.cls:
                class_name = model.names[int(c)]
                if class_name in ['car', 'motorcycle', 'bus', 'truck']:
                    vehicles.append(class_name)

        vehicle_counts = Counter(vehicles)
        log_message = {vehicle_type: count for vehicle_type, count in vehicle_counts.items()}

        # Send to Kafka
        producer.send('traffic_data', value=log_message)
        producer.flush()

        logging.info(log_message)
        print(log_message)

    except Exception as e:
        logging.error(f'An error occurred: {e}')
        print(f'An error occurred: {e}')

    # Wait for 10 seconds before the next refresh
    time.sleep(15)