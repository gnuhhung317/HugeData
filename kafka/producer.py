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

# Logging setup
logging.basicConfig(filename='producer.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Load YOLOv8 model
model = YOLO('yolov8n.pt')

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks='all'
)

# TP.HCM camera base URL (chỉ cần giữ id cố định, t thay đổi)
base_url = 'https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515ad10'

while True:
    try:
        # Thêm timestamp để tránh ảnh bị cache
        timestamp = int(time.time() * 1000)
        url = f'{base_url}&t={timestamp}'

        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()

        if 'image/jpeg' not in response.headers.get('Content-Type', ''):
            logging.warning('Not a JPEG image.')
            time.sleep(10)
            continue

        image_array = np.asarray(bytearray(response.raw.read()), dtype=np.uint8)
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        # Run YOLO inference
        results = model.predict(source=img, conf=0.05, verbose=False)[0]

        # Lọc các loại xe
        vehicles = [
            model.names[int(c)] for c in results.boxes.cls
            if model.names[int(c)] in ['car', 'motorcycle', 'bus', 'truck']
        ]
        vehicle_counts = dict(Counter(vehicles))

        # Gói dữ liệu gửi Kafka
        data = {
            "camera": "HCMC_5deb576d1dc17d7c5515acfa",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "counts": vehicle_counts
        }

        producer.send('traffic_data', value=data)
        producer.flush()

        logging.info(data)
        print(data)

        del img, results

    except Exception as e:
        logging.error(f'Error: {e}')
        print(f'Error: {e}')

    # Đợi 15 giây trước lần lấy tiếp theo
    time.sleep(5)
